use buntrail_engine::Engine;
use serde_json::{Value, json};

fn value(engine: &Engine, request: Value) -> Value {
    let response = engine.call(request);
    assert_eq!(response["ok"], true, "{response}");
    response["value"].clone()
}

#[test]
fn unicode_edit_undo_and_stale_revision_preserve_text() {
    let engine = Engine::new();
    let doc = value(
        &engine,
        json!({"command":"document.open","text":"a🐇\r\nz"}),
    );
    let id = doc["id"].as_u64().unwrap();
    let invalid = engine
        .call(json!({"command":"document.edit","id":id,"revision":0,"start":2,"end":3,"text":"x"}));
    assert_eq!(invalid["ok"], false);
    let edited = value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":1,"end":3,"text":"兔"}),
    );
    assert_eq!(edited["revision"], 1);
    assert!(
        edited.get("text").is_none(),
        "edit reply must be a delta, not a full mirror"
    );
    let stale = engine
        .call(json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":1,"text":"!"}));
    assert_eq!(stale["error"]["code"], "stale_revision");
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["text"],
        "a兔\r\nz"
    );
    assert_eq!(
        value(&engine, json!({"command":"document.undo","id":id}))["text"],
        "a🐇\r\nz"
    );
    assert_eq!(
        value(&engine, json!({"command":"document.redo","id":id}))["text"],
        "a兔\r\nz"
    );
}

#[test]
fn dirty_close_requires_explicit_discard_and_unknown_commands_fail() {
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":""}));
    let id = doc["id"].as_u64().unwrap();
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":0,"text":"draft"}),
    );
    assert_eq!(
        engine.call(json!({"command":"document.close","id":id}))["error"]["code"],
        "dirty_document"
    );
    value(
        &engine,
        json!({"command":"document.close","id":id,"discard":true}),
    );
    assert_eq!(engine.call(json!({"command":"invented"}))["ok"], false);
}

#[test]
fn conflicted_save_preserves_external_contents_and_dirty_buffer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("space ' 🐇.txt");
    std::fs::write(&path, "original").unwrap();
    let engine = Engine::new();
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let doc = value(
        &engine,
        json!({"command":"document.open","path":path,"text":read["text"],"version":read["version"]}),
    );
    let id = doc["id"].as_u64().unwrap();
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"my draft"}),
    );
    std::fs::write(&path, "external").unwrap();
    assert_eq!(
        engine.call(json!({"command":"document.save","id":id}))["ok"],
        false
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "external");
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["text"],
        "my draft"
    );
}

#[test]
fn remote_document_never_saves_to_a_same_named_local_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("same-name.txt");
    std::fs::write(&path, "local").unwrap();
    let engine = Engine::new();
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let doc = value(
        &engine,
        json!({"command":"document.open","path":path,"remote":"disconnected-target","text":"remote","version":read["version"]}),
    );
    let id = doc["id"].as_u64().unwrap();
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":6,"text":"remote edit"}),
    );
    assert_eq!(
        engine.call(json!({"command":"document.save","id":id}))["ok"],
        false
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "local");
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["dirty"],
        true
    );
}

#[test]
fn save_and_history_track_saved_state_independently_of_revision() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("saved.txt");
    std::fs::write(&path, "before").unwrap();
    let engine = Engine::new();
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let doc = value(
        &engine,
        json!({"command":"document.open","text":read["text"],"path":path,"version":read["version"]}),
    );
    let id = &doc["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":6,"text":"after"}),
    );
    assert_eq!(
        value(&engine, json!({"command":"document.save","id":id}))["dirty"],
        false
    );
    let undone = value(&engine, json!({"command":"document.undo","id":id}));
    assert_eq!(undone["dirty"], true);
    assert_eq!(undone["revision"], 2);
    assert_eq!(
        value(&engine, json!({"command":"document.redo","id":id}))["dirty"],
        false
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "after");
}

#[test]
fn bind_created_file_preserves_history_and_rejects_stale_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("new.txt");
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":""}));
    let id = &doc["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":0,"text":"new"}),
    );
    let created = value(
        &engine,
        json!({"command":"fs.create","path":path,"text":"new"}),
    );
    assert_eq!(engine.call(json!({"command":"document.bind","id":id,"revision":0,"path":path,"version":created["version"]}))["ok"],false);
    let bound = value(
        &engine,
        json!({"command":"document.bind","id":id,"revision":1,"path":path,"version":created["version"]}),
    );
    assert_eq!(bound["dirty"], false);
    assert_eq!(
        value(&engine, json!({"command":"document.undo","id":id}))["text"],
        ""
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "new");
}

#[test]
fn external_reload_requires_clean_current_snapshot_and_retains_undo() {
    let engine = Engine::new();
    let doc = value(
        &engine,
        json!({"command":"document.open","text":"before🐇\r\n","path":"/workspace/file","version":"v1"}),
    );
    let id = &doc["id"];
    assert_eq!(doc["version"], "v1");
    let request = json!({"command":"document.reload","id":id,"revision":0,"path":"/workspace/file","expected_version":"v1","text":"external🐇\r\n","version":"v2"});
    let refreshed = value(&engine, request.clone());
    assert_eq!(refreshed["text"], "external🐇\r\n");
    assert_eq!(refreshed["dirty"], false);
    assert_eq!(refreshed["revision"], 1);
    assert_eq!(refreshed["version"], "v2");
    assert_eq!(engine.call(request)["error"]["code"], "stale_revision");
    let undone = value(&engine, json!({"command":"document.undo","id":id}));
    assert_eq!(undone["text"], "before🐇\r\n");
    assert_eq!(undone["dirty"], true);
    let dirty_reload = engine.call(json!({"command":"document.reload","id":id,"revision":2,"path":"/workspace/file","expected_version":"v2","text":"discard draft","version":"v3"}));
    assert_eq!(dirty_reload["error"]["code"], "dirty_document");
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["text"],
        "before🐇\r\n"
    );
    value(&engine, json!({"command":"document.redo","id":id}));
    let wrong_version = engine.call(json!({"command":"document.reload","id":id,"revision":3,"path":"/workspace/file","expected_version":"v1","text":"stale read","version":"v3"}));
    assert_eq!(wrong_version["error"]["code"], "stale_version");
    let wrong_path = engine.call(json!({"command":"document.reload","id":id,"revision":3,"path":"/different/file","expected_version":"v2","text":"wrong file","version":"v3"}));
    assert_eq!(wrong_path["error"]["code"], "wrong_file");
    let wrong_remote = engine.call(json!({"command":"document.reload","id":id,"revision":3,"path":"/workspace/file","remote":"other-environment","expected_version":"v2","text":"wrong environment","version":"v3"}));
    assert_eq!(wrong_remote["error"]["code"], "wrong_file");
}

// These catch missing editor composition/atomicity/selection/save-checkpoint coordination.
#[test]
fn native_composition_commit_cancel_and_unicode_selection_restore() {
    use buntrail_core::contracts::*;
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":"a🐇z"}));
    let id = doc["id"].as_u64().unwrap();
    let transact = |intent, revision, changes, selection| {
        engine
            .transaction(
                id,
                EditTransaction {
                    expected_revision: BufferRevision(revision),
                    source_view: ViewID(7),
                    replacements: changes,
                    intent,
                },
                selection,
                0,
            )
            .unwrap()
    };
    let selection = BufferRange {
        revision: BufferRevision(0),
        start: 1,
        end: 3,
    };
    transact(EditIntent::BeginComposition, 0, vec![], Some(selection));
    transact(
        EditIntent::UpdateComposition,
        0,
        vec![Replacement {
            range: BufferRange {
                revision: BufferRevision(0),
                start: 1,
                end: 3,
            },
            text: "兔".into(),
        }],
        None,
    );
    transact(EditIntent::CommitComposition, 1, vec![], None);
    let undo = transact(EditIntent::Undo, 1, vec![], None);
    assert_eq!(
        undo.selection_anchors,
        vec![BufferRange {
            revision: BufferRevision(2),
            start: 1,
            end: 3
        }]
    );
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["text"],
        "a🐇z"
    );
    assert_eq!(
        value(&engine, json!({"command":"document.get","id":id}))["dirty"],
        false
    );
    transact(EditIntent::BeginComposition, 2, vec![], None);
    transact(
        EditIntent::UpdateComposition,
        2,
        vec![Replacement {
            range: BufferRange {
                revision: BufferRevision(2),
                start: 1,
                end: 3,
            },
            text: "中文".into(),
        }],
        None,
    );
    transact(EditIntent::CancelComposition, 3, vec![], None);
    let state = value(&engine, json!({"command":"document.get","id":id}));
    assert_eq!(state["text"], "a🐇z");
    assert_eq!(state["dirty"], false);
    assert!(state["path"].is_null());
}

#[test]
fn native_multirange_rejection_is_atomic_and_valid_changes_undo_together() {
    use buntrail_core::contracts::*;
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":"a🐇z"}));
    let id = doc["id"].as_u64().unwrap();
    let tx = |ranges: Vec<(u64, u64, &str)>| EditTransaction {
        expected_revision: BufferRevision(0),
        source_view: ViewID(1),
        intent: EditIntent::Replace,
        replacements: ranges
            .into_iter()
            .map(|(start, end, text)| Replacement {
                range: BufferRange {
                    revision: BufferRevision(0),
                    start,
                    end,
                },
                text: text.into(),
            })
            .collect(),
    };
    assert!(
        engine
            .transaction(id, tx(vec![(0, 1, "A"), (2, 3, "bad")]), None, 0)
            .is_err()
    );
    let original = value(&engine, json!({"command":"document.get","id":id}));
    assert_eq!(original["text"], "a🐇z");
    assert_eq!(original["revision"], 0);
    let result = engine
        .transaction(id, tx(vec![(0, 1, "A"), (3, 4, "Z")]), None, 0)
        .unwrap();
    assert_eq!(result.deltas.len(), 2);
    assert_eq!(
        value(&engine, json!({"command":"document.undo","id":id}))["text"],
        "a🐇z"
    );
}

#[test]
fn typing_after_save_cannot_merge_over_saved_checkpoint() {
    use buntrail_core::contracts::*;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint.txt");
    std::fs::write(&path, "").unwrap();
    let engine = Engine::new();
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let doc = value(
        &engine,
        json!({"command":"document.open","text":"","path":path,"version":read["version"]}),
    );
    let id = doc["id"].as_u64().unwrap();
    let tx = |revision, start, text: &str| EditTransaction {
        expected_revision: BufferRevision(revision),
        source_view: ViewID(1),
        intent: EditIntent::Replace,
        replacements: vec![Replacement {
            range: BufferRange {
                revision: BufferRevision(revision),
                start,
                end: start,
            },
            text: text.into(),
        }],
    };
    engine.transaction(id, tx(0, 0, "a"), None, 1).unwrap();
    value(&engine, json!({"command":"document.save","id":id}));
    engine.transaction(id, tx(1, 1, "b"), None, 0).unwrap();
    let undone = value(&engine, json!({"command":"document.undo","id":id}));
    assert_eq!(undone["text"], "a");
    assert_eq!(undone["dirty"], false);
}

#[test]
fn empty_history_and_noop_edits_preserve_legacy_revision_and_binding() {
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":"🐇"}));
    let id = &doc["id"];
    assert_eq!(
        value(&engine, json!({"command":"document.undo","id":id})),
        doc
    );
    assert_eq!(
        value(&engine, json!({"command":"document.redo","id":id})),
        doc
    );
    let edit = value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":2,"text":"🐇"}),
    );
    assert_eq!(edit["revision"], 0);
    assert_eq!(edit["dirty"], false);
    assert!(
        value(&engine, json!({"command":"document.status","id":id}))
            .get("text")
            .is_none()
    );
}

#[test]
fn documents_larger_than_previous_session_limit_remain_editable() {
    let engine = Engine::new();
    let text = "x".repeat(16 * 1024 * 1024 + 1);
    let doc = value(&engine, json!({"command":"document.open","text":text}));
    let id = &doc["id"];
    let changed = value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":1,"text":"🐇"}),
    );
    assert_eq!(changed["revision"], 1);
    assert_eq!(
        engine.snapshot(id.as_u64().unwrap()).unwrap().text.len(),
        16 * 1024 * 1024 + 4
    );
}

#[test]
fn first_save_binding_ends_typing_at_saved_checkpoint() {
    use buntrail_core::contracts::*;
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":""}));
    let id = doc["id"].as_u64().unwrap();
    let tx = |revision, start, text: &str| EditTransaction {
        expected_revision: BufferRevision(revision),
        source_view: ViewID(1),
        intent: EditIntent::Replace,
        replacements: vec![Replacement {
            range: BufferRange {
                revision: BufferRevision(revision),
                start,
                end: start,
            },
            text: text.into(),
        }],
    };
    engine.transaction(id, tx(0, 0, "a"), None, 1).unwrap();
    value(
        &engine,
        json!({"command":"document.bind","id":id,"revision":1,"path":"/created.txt","version":"observed"}),
    );
    engine.transaction(id, tx(1, 1, "b"), None, 0).unwrap();
    let undone = value(&engine, json!({"command":"document.undo","id":id}));
    assert_eq!(undone["text"], "a");
    assert_eq!(undone["dirty"], false);
    assert_eq!(undone["path"], "/created.txt");
}

#[test]
fn existing_sixty_four_mib_admission_and_edit_limits_are_retained() {
    let engine = Engine::new();
    let text = "x".repeat(64 * 1024 * 1024);
    let opened = engine.call(json!({"command":"document.open","text":text}));
    assert_eq!(opened["ok"], true);
    let id = opened["value"]["id"].as_u64().unwrap();
    drop(opened);
    let oversized = "x".repeat(64 * 1024 * 1024 + 1);
    assert_eq!(
        engine.call(json!({"command":"document.open","text":oversized}))["error"]["code"],
        "too_large"
    );
    assert_eq!(engine.call(json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":1,"text":oversized}))["error"]["code"],"too_large");
    assert_eq!(engine.snapshot(id).unwrap().revision, 0);
}

#[test]
fn composition_cancel_restores_the_captured_selection() {
    use buntrail_core::contracts::*;
    let engine = Engine::new();
    let doc = value(&engine, json!({"command":"document.open","text":"a🐇z"}));
    let id = doc["id"].as_u64().unwrap();
    let selected = BufferRange {
        revision: BufferRevision(0),
        start: 1,
        end: 3,
    };
    let tx = |revision, intent, replacements| EditTransaction {
        expected_revision: BufferRevision(revision),
        source_view: ViewID(7),
        intent,
        replacements,
    };
    engine
        .transaction(
            id,
            tx(0, EditIntent::BeginComposition, vec![]),
            Some(selected),
            0,
        )
        .unwrap();
    engine
        .transaction(
            id,
            tx(
                0,
                EditIntent::UpdateComposition,
                vec![Replacement {
                    range: BufferRange {
                        revision: BufferRevision(0),
                        start: 1,
                        end: 3,
                    },
                    text: "日本".into(),
                }],
            ),
            None,
            0,
        )
        .unwrap();
    let canceled = engine
        .transaction(id, tx(1, EditIntent::CancelComposition, vec![]), None, 0)
        .unwrap();
    assert_eq!(
        canceled.selection_anchors,
        vec![BufferRange {
            revision: BufferRevision(2),
            start: 1,
            end: 3
        }]
    );
}

#[test]
fn moving_binding_preserves_dirty_checkpoint_history_and_remote_route() {
    let engine = Engine::new();
    let opened = value(
        &engine,
        json!({"command":"document.open","text":"old","path":"before.txt","version":"base","remote":"ssh-target"}),
    );
    let id = &opened["id"];
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":3,"text":"draft"}),
    );
    let wrong=engine.call(json!({"command":"document.rebind","id":id,"expected_path":"before.txt","path":"after.txt","remote":"wrong-target"}));
    assert_eq!(wrong["error"]["code"], "wrong_file");
    let moved = value(
        &engine,
        json!({"command":"document.rebind","id":id,"expected_path":"before.txt","path":"after.txt","remote":"ssh-target"}),
    );
    assert_eq!(moved["dirty"], true);
    assert_eq!(moved["revision"], 1);
    assert_eq!(moved["version"], "base");
    assert_eq!(moved["remote"], "ssh-target");
    let undone = value(&engine, json!({"command":"document.undo","id":id}));
    assert_eq!(undone["text"], "old");
    assert_eq!(undone["dirty"], false);
    assert_eq!(undone["path"], "after.txt");
}

#[test]
fn empty_native_history_preserves_the_supplied_caret() {
    use buntrail_core::contracts::*;
    let engine = Engine::new();
    let opened = value(&engine, json!({"command":"document.open","text":"abcdef"}));
    let id = opened["id"].as_u64().unwrap();
    for intent in [EditIntent::Undo, EditIntent::Redo] {
        let result = engine
            .transaction(
                id,
                EditTransaction {
                    expected_revision: BufferRevision(0),
                    source_view: ViewID(1),
                    replacements: vec![],
                    intent,
                },
                Some(BufferRange {
                    revision: BufferRevision(0),
                    start: 4,
                    end: 4,
                }),
                0,
            )
            .unwrap();
        assert!(result.deltas.is_empty());
        assert_eq!(
            result.selection_anchors,
            vec![BufferRange {
                revision: BufferRevision(0),
                start: 4,
                end: 4
            }]
        );
    }
}

#[cfg(unix)]
#[test]
fn moving_strong_binding_adopts_fresh_version_and_remains_saveable() {
    let root = tempfile::tempdir().unwrap();
    let before = root.path().join("before");
    let after = root.path().join("after");
    std::fs::write(&before, "original").unwrap();
    let engine = Engine::new();
    let read = value(&engine, json!({"command":"fs.read","path":before}));
    let opened = value(
        &engine,
        json!({"command":"document.open","path":before,"text":read["text"],"version":read["version"]}),
    );
    let id = &opened["id"];
    value(
        &engine,
        json!({"command":"fs.rename","path":before,"to":after}),
    );
    let fresh = value(&engine, json!({"command":"fs.read","path":after}));
    let rebound = value(
        &engine,
        json!({"command":"document.rebind","id":id,"expected_path":before,"expected_version":read["version"],"path":after,"version":fresh["version"]}),
    );
    assert_eq!(rebound["version"], fresh["version"]);
    assert_eq!(rebound["dirty"], false);
    assert_eq!(rebound["revision"], opened["revision"]);
    value(
        &engine,
        json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":8,"text":"changed"}),
    );
    value(&engine, json!({"command":"document.save","id":id}));
    assert_eq!(std::fs::read(&after).unwrap(), b"changed");
}
