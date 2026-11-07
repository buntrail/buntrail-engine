#![cfg(unix)]
use buntrail_engine::Engine;
use serde_json::{Value, json};
use std::{fs, os::unix::ffi::OsStrExt, path::Path};
fn value(engine: &Engine, request: Value) -> Value {
    let result = engine.call(request);
    assert_eq!(result["ok"], true, "{result}");
    result["value"].clone()
}
fn entries(engine: &Engine, path: &Path) -> Vec<Value> {
    let expected = serde_json::to_value(path.as_os_str().as_bytes()).unwrap();
    value(engine, json!({"command":"recovery.list"}))["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["file"]["path"]["PosixBytes"] == expected)
        .cloned()
        .collect()
}
fn open(engine: &Engine, path: &Path) -> Value {
    let read = value(engine, json!({"command":"fs.read","path":path}));
    value(
        engine,
        json!({"command":"document.open","path":path,"text":read["text"],"version":read["version"]}),
    )
}
#[test]
fn bound_checkpoint_preserves_text_encoding_and_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("utf16");
    fs::write(&path, b"\xff\xfea\0\r\0\n\0").unwrap();
    let engine = Engine::new();
    let document = open(&engine, &path);
    let id = &document["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":1,"text":"draft🐇"}),
    );
    value(&engine, json!({"command":"document.recovery","id":id}));
    let found = entries(&engine, &path);
    assert_eq!(found.len(), 1);
    let draft = value(
        &engine,
        json!({"command":"recovery.read","document":found[0]["document"]}),
    );
    assert_eq!(draft["text"], "draft🐇\r\n");
    assert_eq!(draft["version"], document["version"]);
    value(&engine, json!({"command":"document.save","id":id}));
    assert!(entries(&engine, &path).is_empty());
    let bytes = fs::read(&path).unwrap();
    assert!(bytes.starts_with(b"\xff\xfe"));
    assert_eq!(
        String::from_utf16(
            &bytes[2..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|v| u16::from_le_bytes([v[0], v[1]]))
                .collect::<Vec<_>>()
        )
        .unwrap(),
        "draft🐇\r\n"
    );
}
#[test]
fn failed_save_preserves_recovery_and_external_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    fs::write(&path, "original").unwrap();
    let engine = Engine::new();
    let document = open(&engine, &path);
    let id = &document["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"retained draft"}),
    );
    fs::write(&path, "external").unwrap();
    assert_eq!(
        engine.call(json!({"command":"document.save","id":id}))["ok"],
        false
    );
    let found = entries(&engine, &path);
    assert_eq!(found.len(), 1);
    let draft = value(
        &engine,
        json!({"command":"recovery.read","document":found[0]["document"]}),
    );
    assert_eq!(draft["text"], "retained draft");
    assert_eq!(fs::read(&path).unwrap(), b"external");
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["dirty"],
        true
    );
}
#[test]
fn explicit_discard_retires_bound_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    fs::write(&path, "original").unwrap();
    let engine = Engine::new();
    let document = open(&engine, &path);
    let id = &document["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"draft"}),
    );
    value(&engine, json!({"command":"document.recovery","id":id}));
    assert_eq!(entries(&engine, &path).len(), 1);
    assert_eq!(
        engine.call(json!({"command":"document.close","id":id}))["ok"],
        false
    );
    value(
        &engine,
        json!({"command":"document.close","id":id,"discard":true}),
    );
    assert!(entries(&engine, &path).is_empty());
    assert_eq!(
        engine.call(json!({"command":"document.recovery","id":id}))["ok"],
        false
    );
    assert!(entries(&engine, &path).is_empty());
    assert_eq!(fs::read(&path).unwrap(), b"original");
}

#[test]
fn retired_owner_cannot_recreate_discarded_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    fs::write(&path, "original").unwrap();
    let engine = Engine::new();
    let doc = open(&engine, &path);
    let id = &doc["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"draft"}),
    );
    let owner =
        value(&engine, json!({"command":"document.status","id":id}))["recovery_owner"].clone();
    value(
        &engine,
        json!({"command":"document.recovery","id":id,"expected_owner":owner}),
    );
    value(
        &engine,
        json!({"command":"document.discard_recovery","id":id}),
    );
    assert_eq!(
        engine.call(json!({"command":"document.recovery","id":id,"expected_owner":owner}))["ok"],
        false
    );
    assert!(entries(&engine, &path).is_empty());
    let owner =
        value(&engine, json!({"command":"document.status","id":id}))["recovery_owner"].clone();
    value(
        &engine,
        json!({"command":"document.recovery","id":id,"expected_owner":owner}),
    );
    assert_eq!(entries(&engine, &path).len(), 1);
}
#[test]
fn inventory_discard_refuses_live_journal_and_retires_inactive_draft() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    fs::write(&path, "original").unwrap();
    let draft_id = {
        let engine = Engine::new();
        let doc = open(&engine, &path);
        let id = &doc["id"];
        value(
            &engine,
            json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"draft"}),
        );
        value(&engine, json!({"command":"document.recovery","id":id}));
        let draft_id = entries(&engine, &path)[0]["document"].clone();
        assert_eq!(
            engine.call(json!({"command":"recovery.discard","document":draft_id}))["ok"],
            false
        );
        assert_eq!(entries(&engine, &path).len(), 1);
        draft_id
    };
    let engine = Engine::new();
    value(
        &engine,
        json!({"command":"recovery.discard","document":draft_id}),
    );
    assert!(entries(&engine, &path).is_empty());
    assert_eq!(fs::read(&path).unwrap(), b"original");
}
