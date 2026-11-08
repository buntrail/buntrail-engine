#![cfg(unix)]
use buntrail_engine::Engine;
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};
fn git(root: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn call(engine: &Engine, request: Value) -> Value {
    let reply = engine.call(request);
    assert_eq!(reply["ok"], true, "{reply}");
    reply["value"].clone()
}
#[test]
fn repository_identity_receipts_and_retry_are_authoritative() {
    let dir = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(dir.path()).unwrap();
    git(&root, &["init", "-b", "main"]);
    git(&root, &["config", "user.name", "Fixture"]);
    git(&root, &["config", "user.email", "fixture@example.invalid"]);
    git(&root, &["config", "commit.gpgsign", "false"]);
    git(&root, &["config", "core.hooksPath", ".git/hooks"]);
    fs::write(root.join("file"), b"original\r\n").unwrap();
    git(&root, &["add", "--all"]);
    git(&root, &["commit", "-m", "fixture"]);
    let engine = Engine::new();
    let snapshot = call(&engine, json!({"command":"git.repository","root":root}));
    assert_eq!(snapshot["pending_operations"], json!([]));
    let expected = json!({"Git":{"worktree":snapshot["worktree"],"state":snapshot["state"]}});
    let oid = git(&root, &["rev-parse", "HEAD"]);
    let action = |name: &str| json!({"CreateBranch":{"name":name.as_bytes(),"source_ref":b"refs/heads/main","start_oid":oid}});
    let mut foreign = expected.clone();
    foreign["Git"]["worktree"]["common_git_directory_identity"] = json!([1, 2, 3]);
    let result=engine.call(json!({"command":"git.mutate","root":root,"expected":foreign,"action":action("wrong-identity")}));
    assert_eq!(result["ok"], false, "{result}");
    assert!(!git(&root, &["for-each-ref", "--format=%(refname)"]).contains("wrong-identity"));
    let refused = call(
        &engine,
        json!({"command":"git.mutate","root":root,"expected":expected,"action":action("bad name")}),
    );
    assert_eq!(refused["status"], 1, "{refused}");
    assert_eq!(refused["pending"], false);
    assert_eq!(
        call(&engine, json!({"command":"git.repository","root":root}))["pending_operations"],
        json!([]),
        "proven no effect must permit corrected input"
    );
    let created = call(
        &engine,
        json!({"command":"git.mutate","root":root,"expected":expected,"action":action("created")}),
    );
    assert_eq!(created["status"], 0, "{created}");
    assert_eq!(
        created["receipt"]["state"]["Confirmed"]["GitChanged"]["outcome"],
        json!("RefsUpdated")
    );
    let blocked=engine.call(json!({"command":"git.mutate","root":root,"expected":expected,"action":action("must-inspect-first")}));
    assert_eq!(blocked["ok"], false, "{blocked}");
    assert_eq!(
        call(
            &engine,
            json!({"command":"git.inspect","operation":created["operation"]})
        )["resolved"],
        true
    );
    let read = call(
        &engine,
        json!({"command":"git","root":root,"args":["rev-parse","--abbrev-ref","HEAD"]}),
    );
    assert_eq!(read["stdout"], "main\n");
    let history = call(
        &engine,
        json!({"command":"git.history","root":root,"filter":{"authored_since":null,"text":"FIXTURE"}}),
    );
    assert_eq!(history["commits"].as_array().unwrap().len(), 1);
    let commit = call(
        &engine,
        json!({"command":"git.commit","root":root,"oid":oid}),
    );
    assert_eq!(commit["files"].as_array().unwrap().len(), 1);
    assert_eq!(git(&root, &["rev-parse", "HEAD"]), oid);
    assert_eq!(fs::read(root.join("file")).unwrap(), b"original\r\n");
}
