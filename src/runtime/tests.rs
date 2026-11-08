use super::*;
use crate::{Engine, remote_test_support::Server};
fn fixture() -> (Server, Engine, std::path::PathBuf) {
    let server = Server::start();
    let services = Arc::new(Services::with_state_directory(
        server.dir.path().join("client"),
    ));
    let mut engine = Engine::new();
    engine.services = services.clone();
    engine.runtime = Arc::new(Runtime::new(services, engine.documents.clone()));
    let local = engine.runtime.services.local_connection().unwrap();
    let remote = Arc::new(
        RemoteServices::connect(
            server.connect(),
            local.identity.clone(),
            WorkspaceLease {
                client: local.client,
                workspace: WorkspaceID([11; 16]),
                lease: WorkspaceLeaseID([12; 16]),
                runtime: RuntimeGeneration {
                    supervisor_boot: [0; 16],
                    container_start: None,
                },
            },
            server.dir.path().join("client-spools"),
        )
        .unwrap(),
    );
    remote.set_authentication(None).unwrap();
    engine.runtime.registry.lock().unwrap().remotes.insert(
        "fixture".into(),
        Route {
            services: remote,
            _owner: None,
            locator: EnvironmentLocator {
                route: RouteKind::Ssh,
                ssh_profile: Some("buntrail-fixture.invalid".into()),
                engine: None,
                engine_context: None,
                container_selector: None,
                effective_user: None,
            },
        },
    );
    let file = server.dir.path().join("file");
    (server, engine, file)
}
fn value(engine: &Engine, request: Value) -> Value {
    let response = engine.call(request);
    assert_eq!(response["ok"], true, "{response}");
    response["value"].clone()
}
#[test]
fn framed_engine_round_trip_exceeds_legacy_frame_limit_and_preserves_encoding() {
    let (_server, engine, path) = fixture();
    let original = "x".repeat(9 * 1024 * 1024);
    std::fs::write(&path, format!("\u{feff}{original}\r\n")).unwrap();
    let read = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"fs.read","path":path}}),
    );
    assert_eq!(read["text"].as_str().unwrap().len(), original.len() + 2);
    let doc = value(
        &engine,
        json!({"command":"document.open","text":read["text"],"version":read["version"],"path":path,"remote":"fixture"}),
    );
    value(
        &engine,
        json!({"command":"document.edit","id":doc["id"],"revision":0,"start":0,"end":1,"text":"🐇"}),
    );
    let saved = value(&engine, json!({"command":"document.save","id":doc["id"]}));
    assert_eq!(saved["dirty"], false);
    let actual = std::fs::read(&path).unwrap();
    assert!(actual.starts_with(b"\xef\xbb\xbf"));
    assert!(actual.ends_with(b"\r\n"));
    assert!(String::from_utf8(actual).unwrap().contains('🐇'));
}
#[test]
fn retired_route_and_changed_target_never_fall_back_to_local_writes() {
    let (_server, engine, path) = fixture();
    std::fs::write(&path, "original").unwrap();
    let read = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"fs.read","path":path}}),
    );
    let mut binding = Services::file_binding(read["version"].as_str().unwrap()).unwrap();
    binding.file.target.effective_user.push(1);
    binding.version.target = binding.file.target.clone();
    let doc = value(
        &engine,
        json!({"command":"document.open","text":"original","path":path,"version":binding.token().unwrap(),"remote":"fixture"}),
    );
    value(
        &engine,
        json!({"command":"document.edit","id":doc["id"],"revision":0,"start":0,"end":8,"text":"unsafe"}),
    );
    assert_eq!(
        engine.call(json!({"command":"document.save","id":doc["id"]}))["ok"],
        false
    );
    engine
        .runtime
        .registry
        .lock()
        .unwrap()
        .remotes
        .remove("fixture");
    assert_eq!(
        engine.call(json!({"command":"document.save","id":doc["id"]}))["ok"],
        false
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"original");
}

#[test]
fn final_remote_close_waits_for_physical_retirement() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_server, engine, _path) = fixture();
    let engine = Arc::new(engine);
    let started = Arc::new(AtomicBool::new(false));
    let allowed = Arc::new(AtomicBool::new(false));
    let mut registry = engine.runtime.registry.lock().unwrap();
    let route = registry.remotes.get_mut("fixture").unwrap();
    let mut service = (*route.services).clone();
    let begin = started.clone();
    let permit = allowed.clone();
    service
        .bind_retirement(Box::new(move || {
            Box::pin(async move {
                begin.store(true, Ordering::SeqCst);
                while !permit.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        }))
        .unwrap();
    service.physical().unwrap().finish_fixture_startup();
    route.services = Arc::new(service);
    drop(registry);
    let worker = engine.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let response = worker.call(json!({"command":"remote.close","id":"fixture"}));
        send.send(response).unwrap();
    });
    let until = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !started.load(Ordering::SeqCst) && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let early = receive.recv_timeout(std::time::Duration::from_millis(40));
    allowed.store(true, Ordering::SeqCst);
    let response = early.clone().unwrap_or_else(|_| {
        receive
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap()
    });
    thread.join().unwrap();
    assert_eq!(response["ok"], true, "{response}");
    assert!(
        early.is_err(),
        "Close published success before retiring its physical owner"
    );
}

#[test]
fn handles_from_an_earlier_runtime_cannot_alias_a_new_environment() {
    let mut previous = Registry::default();
    let mut current = Registry::default();
    assert_ne!(
        previous.id("remote").unwrap(),
        current.id("remote").unwrap(),
        "Persisted recovery route handles must not authorize a later runtime"
    );
}

#[test]
fn owned_terminal_layout_splits_reattaches_and_drains_without_relaunch() {
    let (_server, engine, path) = fixture();
    let root = path.parent().unwrap();
    let first = value(
        &engine,
        json!({"command":"terminal.start","root":root,"shell":"/bin/sh"}),
    );
    let id = first["id"].clone();
    let second = value(
        &engine,
        json!({"command":"terminal.start","root":root,"shell":"/bin/sh","split":id,"orientation":"Right"}),
    );
    assert_ne!(id, second["id"]);
    let layout = value(&engine, json!({"command":"terminal.layout","root":root}));
    assert_eq!(layout["tabs"].as_array().unwrap().len(), 1);
    assert!(layout["tabs"][0]["root"].get("Split").is_some());
    value(
        &engine,
        json!({"command":"terminal.write","id":id,"text":"printf 'owned-marker'; exit\n"}),
    );
    let mut bytes = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut sequence = 0;
    loop {
        let read = value(&engine, json!({"command":"terminal.read","id":id}));
        bytes.extend(
            read["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b.as_u64().unwrap() as u8),
        );
        sequence = sequence.max(read["sequence"].as_u64().unwrap());
        if read["drained"] == true {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(String::from_utf8_lossy(&bytes).contains("owned-marker"));
    value(
        &engine,
        json!({"command":"terminal.attach","id":id,"sequence":0}),
    );
    let mut replayed = Vec::new();
    loop {
        let replay = value(&engine, json!({"command":"terminal.read","id":id}));
        replayed.extend(
            replay["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b.as_u64().unwrap() as u8),
        );
        assert!(replay["sequence"].as_u64().unwrap() <= sequence);
        if replay["drained"] == true {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
    }
    assert_eq!(replayed, bytes);
    value(&engine, json!({"command":"terminal.close","id":id}));
    value(
        &engine,
        json!({"command":"terminal.close","id":second["id"]}),
    );
}

#[test]
fn owned_remote_terminal_uses_verified_route_and_refuses_local_controls() {
    let (_server, engine, path) = fixture();
    let root = path.parent().unwrap();
    let created = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.start","root":root,"shell":"/bin/sh"}}),
    );
    assert_eq!(
        engine.call(json!({"command":"terminal.write","id":created["id"],"text":"exit\n"}))["ok"],
        false
    );
    value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.write","id":created["id"],"text":"printf REMOTE_OWNED; exit\n"}}),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        let read = value(
            &engine,
            json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.read","id":created["id"]}}),
        );
        bytes.extend(
            read["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_u64().unwrap() as u8),
        );
        if read["drained"] == true {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(String::from_utf8_lossy(&bytes).contains("REMOTE_OWNED"));
    value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.close","id":created["id"]}}),
    );
}

#[test]
fn streaming_search_filters_and_cancel_use_bounded_jobs() {
    let (_server, engine, path) = fixture();
    std::fs::write(&path, "needle one\nneedle two\n").unwrap();
    std::fs::write(path.with_extension("skip"), "needle hidden\n").unwrap();
    let root = path.parent().unwrap();
    let job = value(
        &engine,
        json!({"command":"service.start","request":{"command":"search.stream","root":root,"query":"needle","excludes":["*.skip"]}}),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut matches = 0;
    let mut complete = false;
    loop {
        let poll = value(&engine, json!({"command":"service.poll","id":job["id"]}));
        for e in poll["events"].as_array().unwrap() {
            assert_ne!(e["kind"], "failed", "{e}");
            if e["value"]["kind"] == "match" {
                matches += 1;
                assert_eq!(e["value"]["path"], json!(path));
            }
            if e["value"]["kind"] == "complete" {
                complete = true;
            }
        }
        if poll["done"] == true {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(matches, 2);
    assert!(complete);
    value(&engine, json!({"command":"service.release","id":job["id"]}));
}

#[test]
fn git_adapter_refuses_arbitrary_mutations_and_protects_registered_buffers() {
    let (_server, engine, path) = fixture();
    let root = path.parent().unwrap();
    assert!(
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(root)
            .status()
            .unwrap()
            .success()
    );
    let id = value(
        &engine,
        json!({"command":"document.open","text":"unsaved","path":path,"version":"legacy"}),
    )["id"]
        .clone();
    assert_eq!(
        engine.call(json!({"command":"git","root":root,"args":["checkout","--no-guess","other"]}))
            ["error"]["code"],
        "busy"
    );
    assert_eq!(
        engine.call(json!({"command":"git","root":root,"args":["clean","-fd"]}))["ok"],
        false
    );
    value(
        &engine,
        json!({"command":"document.close","id":id,"discard":true}),
    );
    let status = value(
        &engine,
        json!({"command":"git","root":root,"args":["status","--short","--branch"]}),
    );
    assert_eq!(status["status"], 0);
}

#[test]
fn typed_git_round_trip_and_read_only_history_work_on_local_and_framed_routes() {
    let (_server, engine, base) = fixture();
    let root = base.parent().unwrap().join("repository");
    std::fs::create_dir(&root).unwrap();
    let path = root.join("file");
    let git = |args: &[&str]| {
        let result = std::process::Command::new("git")
            .args(args)
            .current_dir(&root)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    std::fs::write(&path, "one\n").unwrap();
    for remote in [false, true] {
        let call = |args: Value| {
            let r = json!({"command":"git","root":root,"args":args});
            let response = value(
                &engine,
                if remote {
                    json!({"command":"remote.call","id":"fixture","request":r})
                } else {
                    r
                },
            );
            if !response["operation"].is_null() {
                let inspect = json!({"command":"git.inspect","operation":response["operation"]});
                value(
                    &engine,
                    if remote {
                        json!({"command":"remote.call","id":"fixture","request":inspect})
                    } else {
                        inspect
                    },
                );
            }
            response
        };
        let staged = call(json!(["add", "--", "file"]));
        assert_eq!(staged["status"], 0, "{staged}");
        assert_eq!(
            call(json!([
                "commit",
                "-m",
                if remote { "second" } else { "first" }
            ]))["status"],
            0
        );
        let log = call(json!([
            "log",
            "--graph",
            "--oneline",
            "--decorate",
            "--all",
            "-100"
        ]));
        assert_eq!(log["status"], 0);
        assert!(log["stdout"].as_str().unwrap().contains("first"));
        if !remote {
            git(&["branch", "branch-fixture"]);
            assert_eq!(
                call(json!(["checkout", "--no-guess", "branch-fixture"]))["status"],
                0
            );
            std::fs::write(&path, "two\n").unwrap();
        }
    }
    assert!(
        value(&engine, json!({"command":"git.pending"}))["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn git_proof_refusal_before_execution_does_not_leave_an_uninspectable_operation() {
    let (_server, engine, path) = fixture();
    let root = path.parent().unwrap();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
    ] {
        assert!(
            std::process::Command::new("git")
                .args(args)
                .current_dir(root)
                .status()
                .unwrap()
                .success()
        );
    }
    std::fs::write(&path, "one").unwrap();
    for args in [
        vec!["add", "file"],
        vec!["commit", "-qm", "initial"],
        vec!["branch", "other"],
    ] {
        assert!(
            std::process::Command::new("git")
                .args(args)
                .current_dir(root)
                .status()
                .unwrap()
                .success()
        );
    }
    // Deliberately include this fixture's changing backend state in the worktree observation.
    let result =
        engine.call(json!({"command":"git","root":root,"args":["checkout","--no-guess","other"]}));
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        value(&engine, json!({"command":"git.pending"}))["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn confirmed_git_outcome_survives_a_dropped_reply_until_consumer_inspects() {
    let (_server, engine, base) = fixture();
    let root = base.parent().unwrap().join("repo");
    std::fs::create_dir(&root).unwrap();
    assert!(
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&root)
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(root.join("file"), "safe").unwrap();
    let result = value(
        &engine,
        json!({"command":"git","root":root,"args":["add","--","file"]}),
    );
    assert_eq!(result["status"], 0);
    let pending = value(&engine, json!({"command":"git.pending"}));
    assert_eq!(
        pending["entries"].as_array().unwrap().len(),
        1,
        "A cancelled/stale consumer must still find the original outcome"
    );
    let inspected = value(
        &engine,
        json!({"command":"git.inspect","operation":result["operation"]}),
    );
    assert_eq!(inspected["resolved"], true);
    assert!(
        value(&engine, json!({"command":"git.pending"}))["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn recovered_codec_is_preserved_without_granting_a_live_file_binding() {
    let (_server, engine, path) = fixture();
    std::fs::write(&path, [255, 254, b'x', 0, 13, 0, 10, 0]).unwrap();
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let recovered = value(
        &engine,
        json!({"command":"document.open","text":"","encoding_source":read["version"]}),
    );
    assert!(recovered["path"].is_null() && recovered["version"].is_null());
    assert_eq!(recovered["encoding_source"], read["version"]);
    value(
        &engine,
        json!({"command":"document.edit","id":recovered["id"],"revision":0,"start":0,"end":0,"text":"kept\r\n"}),
    );
    let destination = path.with_extension("restored");
    value(
        &engine,
        json!({"command":"fs.create","path":destination,"text":"kept\r\n","source_version":recovered["encoding_source"]}),
    );
    assert_eq!(
        std::fs::read(destination).unwrap(),
        [255, 254, b'k', 0, b'e', 0, b'p', 0, b't', 0, 13, 0, 10, 0]
    );
}

#[test]
fn unreachable_remote_detachment_retires_local_authority_without_replay() {
    let (mut server, engine, _) = fixture();
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    let start = std::time::Instant::now();
    let detached = value(&engine, json!({"command":"remote.detach","id":"fixture"}));
    assert_eq!(detached["confirmed"], false);
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
    assert!(engine.runtime.route("fixture").is_err());
    assert_eq!(
        engine.call(json!({"command":"remote.detach","id":"fixture"}))["ok"],
        false
    );
}

#[test]
fn workspace_state_uses_persistent_ids_and_fences_missing_roots_and_stale_viewports() {
    let (_server, engine, base) = fixture();
    let root = base.parent().unwrap().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let path = root.join("file");
    std::fs::write(&path, "one\n").unwrap();
    let opened = value(&engine, json!({"command":"workspace.open","root":root}));
    assert_eq!(opened["state"], "Ready");
    assert!(opened["worktree"].is_null());
    let read = value(&engine, json!({"command":"fs.read","path":path}));
    let doc = value(
        &engine,
        json!({"command":"document.open","text":read["text"],"path":path,"version":read["version"]}),
    );
    let viewport = json!({"selection":{"revision":999,"start":0,"end":1},"horizontal_offset":0.0,"vertical_offset":10.0,"folded_ranges":[],"revision_anchor":999});
    let saved = value(
        &engine,
        json!({"command":"workspace.snapshot","root":root,"documents":[{"id":doc["id"],"viewport":viewport}],"selected":doc["id"]}),
    );
    let record = &saved["record"];
    assert_eq!(record["documents"].as_array().unwrap().len(), 1);
    assert!(record["documents"][0]["viewport"].is_null());
    let persistent = record["documents"][0]["id"].clone();
    assert_eq!(persistent.as_array().unwrap().len(), 16);
    value(
        &engine,
        json!({"command":"document.close","id":doc["id"],"discard":false}),
    );
    let fresh = value(
        &engine,
        json!({"command":"document.open","text":read["text"],"path":path,"version":read["version"],"persistent_id":persistent}),
    );
    assert_ne!(fresh["id"], doc["id"]);
    assert_eq!(fresh["persistent_id"], persistent);
    value(
        &engine,
        json!({"command":"document.close","id":fresh["id"],"discard":false}),
    );
    std::fs::remove_dir_all(&root).unwrap();
    let missing = value(
        &engine,
        json!({"command":"workspace.open","workspace":opened["record"]["id"]}),
    );
    assert_eq!(missing["state"], "Missing");
    assert_eq!(missing["record"]["id"], opened["record"]["id"]);
    let mut changed = opened["record"]["target"].clone();
    changed["effective_user"] = json!([99]);
    assert_eq!(
        engine.call(json!({"command":"workspace.open","root":root,"expected":changed}))["ok"],
        false
    );
}

#[test]
fn verified_document_reconnect_retains_the_original_pending_save() {
    let (_server, engine, path) = fixture();
    std::fs::write(&path, "original").unwrap();
    let read = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"fs.read","path":path}}),
    );
    let doc = value(
        &engine,
        json!({"command":"document.open","text":read["text"],"path":path,"version":read["version"],"remote":"fixture"}),
    );
    value(
        &engine,
        json!({"command":"document.edit","id":doc["id"],"revision":0,"start":0,"end":8,"text":"pending"}),
    );
    assert!(
        engine
            .save_with(doc["id"].as_u64().unwrap(), |_, _| Err(fail(
                "save_uncertain",
                "lost"
            )))
            .is_err()
    );
    let before = value(&engine, json!({"command":"document.status","id":doc["id"]}));
    let remote = engine.runtime.route("fixture").unwrap();
    let locator = engine.runtime.registry.lock().unwrap().remotes["fixture"]
        .locator
        .clone();
    engine.runtime.registry.lock().unwrap().remotes.insert(
        "resumed".into(),
        Route {
            services: remote,
            _owner: None,
            locator,
        },
    );
    let resumed = value(
        &engine,
        json!({"command":"document.reconnect","id":doc["id"],"expected_remote":"fixture","remote":"resumed"}),
    );
    assert_eq!(resumed["remote"], "resumed");
    assert_eq!(resumed["pending_operation"], before["pending_operation"]);
    assert_eq!(resumed["text"], "pending");
    assert_eq!(
        engine.call(json!({"command":"document.save","id":doc["id"]}))["ok"],
        false
    );
}

#[test]
fn silent_remote_detachment_has_a_bounded_unconfirmed_result() {
    let (server, engine, _) = fixture();
    assert_eq!(
        unsafe { libc::kill(server.child.id() as i32, libc::SIGSTOP) },
        0
    );
    let start = std::time::Instant::now();
    let detached = value(&engine, json!({"command":"remote.detach","id":"fixture"}));
    assert_eq!(
        unsafe { libc::kill(server.child.id() as i32, libc::SIGCONT) },
        0
    );
    assert_eq!(detached["confirmed"], false);
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
    assert!(engine.runtime.route("fixture").is_err());
}

#[test]
fn terminal_layout_is_memory_only_and_restart_is_empty() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let make = || {
        let services = Arc::new(Services::with_state_directory(state.path().to_owned()));
        let mut engine = Engine::new();
        engine.services = services.clone();
        engine.runtime = Arc::new(Runtime::new(services, engine.documents.clone()));
        engine
    };
    let engine = make();
    let started = value(
        &engine,
        json!({"command":"terminal.start","root":root.path(),"shell":"/bin/sh"}),
    );
    let before = value(
        &engine,
        json!({"command":"terminal.layout","root":root.path()}),
    );
    value(
        &engine,
        json!({"command":"workspace.snapshot","root":root.path(),"documents":[]}),
    );
    let after = value(
        &engine,
        json!({"command":"terminal.layout","root":root.path()}),
    );
    assert_eq!(before["tabs"], after["tabs"]);
    assert!(
        engine
            .runtime
            .services
            .local_connection()
            .unwrap()
            .store
            .load()
            .unwrap()
            .records[0]
            .terminal_layout
            .tabs
            .is_empty()
    );
    value(&engine, json!({"command":"workspace.shutdown"}));
    drop(engine);
    let engine = make();
    let layout = value(
        &engine,
        json!({"command":"terminal.layout","root":root.path()}),
    );
    assert!(layout["tabs"].as_array().unwrap().is_empty());
    assert_eq!(
        engine.call(json!({"command":"terminal.attach","id":started["id"],"sequence":0}))["ok"],
        false
    );
}

#[test]
fn workspace_preferences_are_validated_before_persistence() {
    let (_server, engine, base) = fixture();
    let root = base.parent().unwrap().join("prefs");
    std::fs::create_dir(&root).unwrap();
    value(&engine, json!({"command":"workspace.open","root":root}));
    let initial = value(&engine, json!({"command":"preferences.get","root":root}));
    let mut style = initial["terminal"].clone();
    style["font_size"] = json!(0);
    assert_eq!(
        engine.call(json!({"command":"preferences.set","root":root,"override":{"terminal":style}}))
            ["ok"],
        false
    );
    assert_eq!(
        value(&engine, json!({"command":"preferences.get","root":root})),
        initial
    );
    style["font_size"] = json!(15);
    assert_eq!(
        value(
            &engine,
            json!({"command":"preferences.set","root":root,"override":{"terminal":style}})
        )["terminal"]["font_size"],
        15.0
    );
}

#[test]
fn reopening_a_ready_saved_remote_reuses_its_registration_and_cancel_is_harmless() {
    let (_server, engine, path) = fixture();
    let opened = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"workspace.open","root":path.parent().unwrap()}}),
    );
    let mut wrong = opened["record"]["target"].clone();
    wrong["effective_user"] = json!([99]);
    assert_eq!(
        engine.call(
            json!({"command":"remote.begin","workspace":opened["record"]["id"],"expected":wrong})
        )["ok"],
        false
    );
    let begin = value(
        &engine,
        json!({"command":"remote.begin","workspace":opened["record"]["id"]}),
    );
    assert_eq!(begin["id"], "fixture");
    let ready = value(&engine, json!({"command":"remote.poll","id":begin["id"]}));
    assert_eq!(ready["reused"], true);
    value(&engine, json!({"command":"remote.cancel","id":begin["id"]}));
    assert!(
        engine
            .runtime
            .route("fixture")
            .unwrap()
            .validate_environment_ready()
            .is_ok()
    );
}

#[test]
fn shutdown_does_not_initialize_or_require_an_unused_session_store() {
    let state = tempfile::tempdir().unwrap();
    std::fs::write(state.path().join("context"), "unavailable context").unwrap();
    let services = Arc::new(Services::with_state_directory(state.path().to_owned()));
    let mut engine = Engine::new();
    engine.services = services.clone();
    engine.runtime = Arc::new(Runtime::new(services, engine.documents.clone()));
    assert_eq!(
        value(&engine, json!({"command":"workspace.shutdown"}))["closed"],
        true
    );
    assert_eq!(
        std::fs::read(state.path().join("context")).unwrap(),
        b"unavailable context"
    );
}

#[test]
fn explorer_remove_is_nonrecursive_on_local_and_framed_routes() {
    let (_server, engine, path) = fixture();
    for remote in [false, true] {
        let root = path.with_extension(if remote { "remote-dir" } else { "local-dir" });
        std::fs::create_dir(&root).unwrap();
        let child = root.join("keep");
        std::fs::write(&child, "unsaved work").unwrap();
        let remove = json!({"command":"fs.remove", "path":root});
        let response = engine.call(if remote {
            json!({"command":"remote.call", "id":"fixture", "request":remove})
        } else {
            remove
        });
        assert_eq!(response["ok"], false, "{response}");
        assert_eq!(std::fs::read_to_string(&child).unwrap(), "unsaved work");
        std::fs::remove_file(&child).unwrap();
        let remove = json!({"command":"fs.remove", "path":root});
        let response = engine.call(if remote {
            json!({"command":"remote.call", "id":"fixture", "request":remove})
        } else {
            remove
        });
        assert_eq!(response["ok"], true, "{response}");
        assert!(!root.exists());
    }
}

#[test]
fn disconnected_terminal_context_survives_verified_route_replacement() {
    let (server, engine, path) = fixture();
    let old = engine.runtime.route("fixture").unwrap();
    let owner = old.lease().clone();
    let target = old.target().clone();
    let locator = engine.runtime.registry.lock().unwrap().remotes["fixture"]
        .locator
        .clone();
    let root = path.parent().unwrap();
    let started = value(
        &engine,
        json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.start","root":root,"shell":"/bin/sh"}}),
    );
    value(&engine, json!({"command":"remote.detach","id":"fixture"}));
    assert_eq!(engine.call(json!({"command":"remote.call","id":"fixture","request":{"command":"terminal.write","id":started["id"],"text":"unsafe"}}))["ok"], false);
    drop(old);
    let remote = Arc::new(
        RemoteServices::connect(
            server.connect(),
            target,
            owner,
            server.dir.path().join("new-spools"),
        )
        .unwrap(),
    );
    remote.set_authentication(None).unwrap();
    engine.runtime.registry.lock().unwrap().remotes.insert(
        "replacement".into(),
        Route {
            services: remote,
            _owner: None,
            locator,
        },
    );
    let layout = value(
        &engine,
        json!({"command":"remote.call","id":"replacement","request":{"command":"terminal.layout","root":root}}),
    );
    assert_eq!(layout["tabs"], started["layout"]["tabs"]);
    value(
        &engine,
        json!({"command":"remote.call","id":"replacement","request":{"command":"terminal.attach","id":started["id"],"sequence":0}}),
    );
    value(
        &engine,
        json!({"command":"remote.call","id":"replacement","request":{"command":"terminal.close","id":started["id"]}}),
    );
}
