use super::*;
use crate::remote_test_support as support;
use buntrail_services::connections::local::LocalConnection;
use std::time::Instant;
#[test]
fn container_welcome_refuses_missing_or_changed_actual_start_before_registration() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let target = TargetIdentity {
        security_binding: b"host".to_vec(),
        effective_user: b"1000".to_vec(),
        filesystem_namespace: b"container".to_vec(),
        container: Some(ContainerIdentity {
            engine_endpoint: b"endpoint".to_vec(),
            immutable_id: vec![b'a'; 64],
        }),
    };
    for (expected_start, actual) in [
        (Some(b"current-start".to_vec()), None),
        (
            Some(b"current-start".to_vec()),
            Some(STANDARD.encode(b"other-start")),
        ),
        (None, None),
    ] {
        let (client, mut server) = UnixStream::pair().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let binding = w::hex(&sha2::Sha256::digest(serde_json::to_vec(&target).unwrap()));
        let peer = std::thread::spawn(move || {
            let hello = read_frame(&mut server, None).unwrap();
            let mut header = hello.header;
            header.message = Message::Welcome {
                capabilities: vec!["services".into(), "container.binding.v1".into()],
                max_header: 65536,
                max_payload: 1048576,
                initial_credit: 524288,
                target_binding: binding,
                runtime_generation: w::uuid([9; 16]),
                container_start_base64: actual,
            };
            write_frame(
                &mut server,
                &Frame {
                    header,
                    payload: vec![],
                },
            )
            .unwrap();
            if let Ok(register) = read_frame(&mut server, None) {
                let mut header = register.header;
                header.message = Message::Ack {};
                let _ = write_frame(
                    &mut server,
                    &Frame {
                        header,
                        payload: vec![],
                    },
                );
                return true;
            }
            false
        });
        let lease = WorkspaceLease {
            client: ClientID([1; 16]),
            workspace: WorkspaceID([2; 16]),
            lease: WorkspaceLeaseID([3; 16]),
            runtime: RuntimeGeneration {
                supervisor_boot: [0; 16],
                container_start: expected_start,
            },
        };
        let result = Transport::connect(client, &target, &lease);
        assert!(
            !peer.join().unwrap(),
            "wrong/missing actual start must never register an owner; expected start {:?}",
            lease.runtime.container_start
        );
        assert_eq!(result.err().unwrap().code, ErrorCode::TargetChanged);
    }
}

#[test]
fn welcome_accepts_bounded_container_start_and_ordinary_ssh_without_start() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    for container in [true, false] {
        let target = TargetIdentity {
            security_binding: b"host".to_vec(),
            effective_user: b"1000".to_vec(),
            filesystem_namespace: b"namespace".to_vec(),
            container: container.then(|| ContainerIdentity {
                engine_endpoint: b"endpoint".to_vec(),
                immutable_id: vec![b'a'; 64],
            }),
        };
        let start = container.then(|| vec![b's'; 256]);
        let actual = start.as_ref().map(|value| STANDARD.encode(value));
        let binding = w::hex(&sha2::Sha256::digest(serde_json::to_vec(&target).unwrap()));
        let (client, mut server) = UnixStream::pair().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let peer = std::thread::spawn(move || {
            let mut header = read_frame(&mut server, None).unwrap().header;
            header.message = Message::Welcome {
                capabilities: vec!["services".into(), "container.binding.v1".into()],
                max_header: 65536,
                max_payload: 1048576,
                initial_credit: 524288,
                target_binding: binding,
                runtime_generation: w::uuid([9; 16]),
                container_start_base64: actual,
            };
            write_frame(
                &mut server,
                &Frame {
                    header,
                    payload: vec![],
                },
            )
            .unwrap();
            let mut header = read_frame(&mut server, None).unwrap().header;
            let Message::Register {
                client_id,
                workspace_id,
                target_binding,
                runtime_generation,
                ..
            } = &header.message
            else {
                panic!("expected owner registration")
            };
            header.context = Some(Context {
                client_id: client_id.clone(),
                workspace_id: workspace_id.clone(),
                target_binding: target_binding.clone(),
                runtime_generation: runtime_generation.clone(),
            });
            header.message = Message::Ack {};
            write_frame(
                &mut server,
                &Frame {
                    header,
                    payload: vec![],
                },
            )
            .unwrap();
        });
        let lease = WorkspaceLease {
            client: ClientID([1; 16]),
            workspace: WorkspaceID([2; 16]),
            lease: WorkspaceLeaseID([3; 16]),
            runtime: RuntimeGeneration {
                supervisor_boot: [0; 16],
                container_start: start.clone(),
            },
        };
        let transport = Transport::connect(client, &target, &lease).unwrap();
        assert_eq!(transport.scope.lease.runtime.supervisor_boot, [9; 16]);
        assert_eq!(transport.scope.lease.runtime.container_start, start);
        peer.join().unwrap();
    }
}

fn fixture() -> (support::Server, RemoteServices, FileRef) {
    fixture_server(support::Server::start())
}
fn fixture_server(server: support::Server) -> (support::Server, RemoteServices, FileRef) {
    let local = LocalConnection::open(server.dir.path().join("local")).unwrap();
    let root = server.dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file"), b"needle\n").unwrap();
    let file = FileRef {
        target: local.identity.clone(),
        path: TargetPath::posix(root.as_os_str().as_encoded_bytes().to_vec()).unwrap(),
        resolved_identity: None,
    };
    let remote = RemoteServices::connect(
        server.connect(),
        local.identity,
        WorkspaceLease {
            client: ClientID([10; 16]),
            workspace: WorkspaceID([11; 16]),
            lease: WorkspaceLeaseID([12; 16]),
            runtime: RuntimeGeneration {
                supervisor_boot: [0; 16],
                container_start: None,
            },
        },
        server.dir.path().join("spools"),
    )
    .unwrap();
    (server, remote, file)
}

#[test]
fn shared_transport_registers_workspace_without_replacing_first_owner() {
    let (_server, first, root) = fixture();
    let second = first
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([21; 16]),
            lease: WorkspaceLeaseID([22; 16]),
            ..first.lease.clone()
        })
        .unwrap();
    for view in [&first, &second] {
        assert!(
            matches!(view.call(w::Call::List { file: w::File::from_core(&root).unwrap() }, None).unwrap(), w::Reply::List { entries } if entries.len() == 1)
        );
    }
}

#[test]
fn releasing_one_workspace_keeps_other_owner_and_late_obligations() {
    let (_server, first, root) = fixture();
    let second = first
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([21; 16]),
            lease: WorkspaceLeaseID([22; 16]),
            ..first.lease.clone()
        })
        .unwrap();
    assert_eq!(
        first
            .register_workspace(WorkspaceLease {
                client: ClientID([44; 16]),
                ..second.lease.clone()
            })
            .err()
            .unwrap()
            .code,
        ErrorCode::TargetChanged
    );
    let mut pending = first
        .stream(
            w::Call::List {
                file: w::File::from_core(&root).unwrap(),
            },
            None,
        )
        .unwrap();
    wait_unread_terminal(&pending.request);
    first
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    first
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    assert!(
        first
            .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
            .is_err(),
        "detachment was reported as final closure"
    );
    assert!(
        first
            .call(
                w::Call::List {
                    file: w::File::from_core(&root).unwrap()
                },
                None
            )
            .is_err()
    );
    loop {
        let frame = pending.request.next().unwrap();
        if let Some(value) = pending.accept(frame).unwrap() {
            assert!(matches!(value.unwrap(), w::Reply::List { entries } if entries.len() == 1));
            break;
        }
    }
    drop(pending);
    for _ in 0..12 {
        assert!(
            matches!(second.call(w::Call::List { file: w::File::from_core(&root).unwrap() }, None).unwrap(), w::Reply::List { entries } if entries.len() == 1)
        );
    }
    let resumed = second.register_workspace(first.lease.clone()).unwrap();
    assert!(
        first
            .call(w::Call::Inspect, Some(OperationID([77; 16])))
            .is_err()
    );
    assert!(
        resumed
            .call(w::Call::Inspect, Some(OperationID([77; 16])))
            .is_ok()
    );
    resumed
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    assert!(
        second
            .call(w::Call::Inspect, Some(OperationID([77; 16])))
            .is_ok()
    );
    let mut shutdown = Box::pin(second.close_connection());
    let deadline = Instant::now() + Duration::from_secs(3);
    while std::future::Future::poll(
        shutdown.as_mut(),
        &mut std::task::Context::from_waker(Waker::noop()),
    )
    .is_pending()
    {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(second.connection_state(), ConnectionState::Disconnected);
}

// Observe the actual terminal frame in the worker mailbox without consuming it or returning credit.
fn wait_unread_terminal(request: &Request) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut inbox = request.mailbox.state.lock().unwrap();
    loop {
        assert!(inbox.error.is_none(), "{:?}", inbox.error);
        if inbox.frames.iter().any(|f| {
            matches!(
                f.header.message,
                Message::Ack {}
                    | Message::ReadEnd { .. }
                    | Message::Error { .. }
                    | Message::ServiceEvent { terminal: true, .. }
            )
        }) {
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "terminal frame never arrived");
        inbox = request
            .mailbox
            .ready
            .wait_timeout(inbox, remaining)
            .unwrap()
            .0;
    }
}

#[test]
fn completed_unread_search_service_and_read_owners_retire_on_drop() {
    let (_server, remote, file) = fixture();
    let search = w::Call::Search {
        request: w::Search {
            root: w::File::from_core(&file).unwrap(),
            expression: "needle".into(),
            regex: false,
            case_sensitive: true,
            whole_word: false,
            includes: vec![],
            excludes: vec![],
            hidden: false,
            ignored: false,
        },
    };
    // Exhaust each helper quota if completed unread owners are leaked.
    for (message, count) in [
        (
            Message::ServiceCall {
                call: search.clone(),
            },
            4,
        ),
        (
            Message::ServiceCall {
                call: w::Call::List {
                    file: w::File::from_core(&file).unwrap(),
                },
            },
            10,
        ),
        (
            Message::Read {
                path_base64: B64.encode(format!(
                    "{}/file",
                    String::from_utf8_lossy(match &file.path {
                        TargetPath::PosixBytes(b) => b,
                        _ => unreachable!(),
                    })
                )),
            },
            6,
        ),
    ] {
        for _ in 0..count {
            let request = remote.transport.start(message.clone(), None, None).unwrap();
            wait_unread_terminal(&request);
            assert!(
                !request
                    .mailbox
                    .state
                    .lock()
                    .unwrap()
                    .frames
                    .iter()
                    .any(|f| matches!(f.header.message, Message::Error { .. })),
                "capacity leaked after terminal/drop"
            );
            drop(request);
        }
    }
    let mut stream = remote.stream(search, None).unwrap();
    loop {
        let f = stream.request.next().unwrap();
        if let Some(value) = stream.accept(f).unwrap()
            && matches!(value.unwrap(), w::Reply::SearchComplete)
        {
            break;
        }
    }
    assert!(matches!(
        remote
            .call(
                w::Call::List {
                    file: w::File::from_core(&file).unwrap()
                },
                None
            )
            .unwrap(),
        w::Reply::List { .. }
    ));
    assert!(matches!(
        remote
            .call(w::Call::Inspect, Some(OperationID([7; 16])))
            .unwrap(),
        w::Reply::Receipt { .. }
    ));
}

#[test]
fn completed_unread_routes_remain_bounded_until_finish_or_drop() {
    let (_server, remote, _) = fixture();
    let mut held = vec![];
    for _ in 0..32 {
        let r = remote
            .transport
            .start(
                Message::Authentication {
                    ssh_auth_sock: None,
                },
                None,
                None,
            )
            .unwrap();
        wait_unread_terminal(&r);
        held.push(r);
    }
    let mut refused = remote
        .transport
        .start(
            Message::Authentication {
                ssh_auth_sock: None,
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(refused.next().unwrap_err().code, ErrorCode::Busy);
    drop(refused);
    proven(mkdir(&remote), ErrorCode::Busy);
    for mut r in held {
        assert!(matches!(r.next().unwrap().header.message, Message::Ack {}));
        r.finish();
    }
    remote.set_authentication(None).unwrap();
}

fn run<T>(mut future: ServiceFuture<'_, T>) -> T {
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("synchronous proxy future unexpectedly pending"),
    }
}
fn proven(receipt: OperationReceipt, code: ErrorCode) {
    let OperationState::Failed(failure) = receipt.state else {
        panic!("{receipt:?}");
    };
    assert_eq!(failure.effect, FailureEffect::ProvenNoEffect);
    assert_eq!(failure.error.code, code);
}
fn mkdir(remote: &RemoteServices) -> OperationReceipt {
    run(FileService::mutate(
        remote,
        FileMutationRequest {
            operation: OperationID([71; 16]),
            file: FileRef {
                target: remote.target.clone(),
                path: TargetPath::posix(b"/never-created".to_vec()).unwrap(),
                resolved_identity: None,
            },
            expected: ExpectedPathState::Absent,
            command: FileMutation::CreateDirectory,
        },
    ))
}

#[test]
fn bounded_queue_refusal_before_send_is_proven_no_effect() {
    let (_server, mut remote, _) = fixture();
    let (tx, _rx) = mpsc::sync_channel(1);
    let _original_sender = remote.transport.sender.clone();
    remote.transport.sender = tx;
    let _held = remote
        .transport
        .start(
            Message::Authentication {
                ssh_auth_sock: None,
            },
            None,
            None,
        )
        .unwrap();
    proven(mkdir(&remote), ErrorCode::Busy);
}

// A scripted peer distinguishes generic Error from authoritative operation receipts.
// It exercises the real worker/decoder, while real-helper tests cover admission policy.
fn scripted_peer(
    stage: &'static str,
    code: &'static str,
) -> (
    tempfile::TempDir,
    RemoteServices,
    std::thread::JoinHandle<Vec<Message>>,
) {
    scripted_peer_with_gate(stage, code, None)
}
fn scripted_peer_with_gate(
    stage: &'static str,
    code: &'static str,
    gate: Option<Arc<std::sync::Barrier>>,
) -> (
    tempfile::TempDir,
    RemoteServices,
    std::thread::JoinHandle<Vec<Message>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let target = buntrail_services::connections::local::identity().unwrap();
    let context = wire_context(&target);
    let (client, mut peer) = UnixStream::pair().unwrap();
    client.set_nonblocking(true).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let (sender, rx) = mpsc::sync_channel(32);
    let (control, controls) = mpsc::sync_channel(32);
    let credits: Credits = Arc::new(Mutex::new(HashMap::new()));
    let connected = Arc::new(AtomicBool::new(true));
    let ids = Arc::new(std::sync::atomic::AtomicU64::new(3));
    let scope = Arc::new(Scope {
        context: context.clone(),
        lease: WorkspaceLease {
            client: ClientID([1; 16]),
            workspace: WorkspaceID([2; 16]),
            lease: WorkspaceLeaseID([3; 16]),
            runtime: RuntimeGeneration {
                supervisor_boot: [1; 16],
                container_start: None,
            },
        },
        active: AtomicBool::new(true),
        release_confirmed: Mutex::new(None),
    });
    let scopes = Arc::new(Mutex::new(vec![Arc::downgrade(&scope)]));
    let retired = Arc::new(Retirement::default());
    let transport = Transport {
        authentication_ready: Arc::new(AtomicBool::new(false)),
        physical: None,
        connection_permit: None,
        requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        sender,
        control,
        scope,
        scopes: scopes.clone(),
        registrations: Arc::new(Mutex::new(())),
        capabilities: Arc::new(vec![
            "services".into(),
            "leases.retirement".into(),
            "file.save_proof".into(),
        ]),
        retired: retired.clone(),
        credits: credits.clone(),
        context: context.clone(),
        runtime: RuntimeGeneration {
            supervisor_boot: [1; 16],
            container_start: None,
        },
        connected: connected.clone(),
        ids: ids.clone(),
    };
    let peer_context = context.clone();
    std::thread::spawn(move || {
        if let Some(gate) = gate {
            gate.wait();
        }
        let _retire = Retire(retired, connected.clone());
        worker(client, rx, controls, credits, ids, connected)
    });
    let thread = std::thread::spawn(move || {
        let mut seen = vec![];
        loop {
            let frame = read_frame(&mut peer, Some(&peer_context)).unwrap();
            let stop = matches!(
                (&frame.header.message, stage),
                (Message::UploadBegin { .. }, "begin")
                    | (Message::ProofLookupBegin { .. }, "proof-lookup")
                    | (Message::UploadEnd { .. }, "end")
                    | (Message::SaveCommitOwned { .. }, "commit")
                    | (Message::ServiceCall { .. }, "service")
                    | (Message::Release { .. }, "release")
            );
            let response = if stop && code == "wrong_stream" {
                Message::ProofLookupReady { credit: 65536 }
            } else if stop && matches!(code, "wrong_context" | "wrong_operation" | "late_ack") {
                Message::Ack {}
            } else if stop {
                Message::Error {
                    code: code.into(),
                    safe_message: "scripted phase refusal".into(),
                }
            } else {
                match &frame.header.message {
                    Message::UploadBegin { .. } | Message::UploadEnd { .. } => Message::Ack {},
                    Message::UploadChunk { .. } => Message::Credit {
                        bytes: frame.payload.len() as u32,
                    },
                    other => panic!("unexpected frame: {other:?}"),
                }
            };
            seen.push(frame.header.message.clone());
            let mut header = frame.header;
            header.message = response;
            if stop && code == "late_ack" {
                let mut late = header.clone();
                late.request_id = 999;
                late.context.as_mut().unwrap().workspace_id = w::uuid([88; 16]);
                write_frame(
                    &mut peer,
                    &Frame {
                        header: late,
                        payload: vec![],
                    },
                )
                .unwrap();
            }
            if stop && code == "wrong_context" {
                header.context.as_mut().unwrap().workspace_id = w::uuid([88; 16]);
            }
            if stop && code == "wrong_stream" {
                header.stream_id = Some(w::uuid([88; 16]));
            }
            if stop && code == "wrong_operation" {
                header.operation_id = Some(w::uuid([89; 16]));
            }
            if stop && code == "closed" {
                return seen;
            }
            write_frame(
                &mut peer,
                &Frame {
                    header,
                    payload: vec![],
                },
            )
            .unwrap();
            if stop {
                return seen;
            }
        }
    });
    let remote = RemoteServices {
        anchor: None,
        cancellation: None,
        lease: WorkspaceLease {
            client: ClientID([1; 16]),
            workspace: WorkspaceID([2; 16]),
            lease: WorkspaceLeaseID([3; 16]),
            runtime: transport.runtime.clone(),
        },
        transport,
        target,
        spools: buntrail_services::leases::LeasePool::open(dir.path().join("spools")).unwrap(),
    };
    (dir, remote, thread)
}
fn wire_context(target: &TargetIdentity) -> buntrail_protocol::types::Context {
    buntrail_protocol::types::Context {
        client_id: w::uuid([1; 16]),
        workspace_id: w::uuid([2; 16]),
        target_binding: w::hex(&sha2::Sha256::digest(serde_json::to_vec(target).unwrap())),
        runtime_generation: w::uuid([1; 16]),
    }
}

#[test]
fn replies_cannot_cross_registered_workspace_or_operation_owners() {
    for mismatch in ["wrong_context", "wrong_operation"] {
        let (_dir, remote, peer) = scripted_peer("service", mismatch);
        let mut context = remote.transport.context.clone();
        context.workspace_id = w::uuid([88; 16]);
        let scope = Arc::new(Scope {
            context,
            lease: WorkspaceLease {
                workspace: WorkspaceID([88; 16]),
                ..remote.lease.clone()
            },
            active: AtomicBool::new(true),
            release_confirmed: Mutex::new(None),
        });
        remote
            .transport
            .scopes
            .lock()
            .unwrap()
            .push(Arc::downgrade(&scope));
        let mut request = remote
            .transport
            .start(
                Message::ServiceCall {
                    call: w::Call::Inspect,
                },
                Some(OperationID([90; 16])),
                None,
            )
            .unwrap();
        assert!(request.next().is_err(), "reply accepted for {mismatch}");
        peer.join().unwrap();
    }
}

#[test]
fn late_reply_from_retired_workspace_does_not_disconnect_current_owner() {
    let (_dir, remote, peer) = scripted_peer("service", "late_ack");
    let mut request = remote
        .transport
        .start(
            Message::ServiceCall {
                call: w::Call::Inspect,
            },
            Some(OperationID([90; 16])),
            None,
        )
        .unwrap();
    let reply = request.next().unwrap();
    assert_eq!(reply.header.request_id, request.header.request_id);
    assert_eq!(
        reply.header.context.as_ref(),
        Some(&remote.transport.context)
    );
    assert!(matches!(reply.header.message, Message::Ack {}));
    request.finish();
    peer.join().unwrap();
}

#[test]
fn failed_release_is_not_later_reported_as_confirmed_or_replayed() {
    let (_dir, remote, peer) = scripted_peer("release", "closed");
    assert!(
        remote
            .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
            .is_err()
    );
    assert!(
        remote
            .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
            .is_err(),
        "unconfirmed release was reported as success"
    );
    assert_eq!(peer.join().unwrap().len(), 1);
}

#[test]
fn workspace_view_capacity_is_live_ownership_not_a_lifetime_quota() {
    let (_server, first, root) = fixture();
    let lease = |n: u16| {
        let mut id = [0; 16];
        id[..2].copy_from_slice(&n.to_be_bytes());
        WorkspaceLease {
            workspace: WorkspaceID(id),
            lease: WorkspaceLeaseID(id),
            ..first.lease.clone()
        }
    };
    let mut views = Vec::new();
    for n in 1..256 {
        views.push(first.register_workspace(lease(n)).unwrap());
    }
    assert_eq!(
        first.register_workspace(lease(256)).err().unwrap().code,
        ErrorCode::Busy
    );
    let last = views.pop().unwrap();
    last.release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    drop(last);
    let deadline = Instant::now() + Duration::from_secs(3);
    let replacement = loop {
        match first.register_workspace(lease(256)) {
            Ok(view) => break view,
            Err(error) if error.code == ErrorCode::Busy && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(error) => panic!("retired view capacity did not return: {error:?}"),
        }
    };
    assert!(
        matches!(replacement.call(w::Call::List { file: w::File::from_core(&root).unwrap() }, None).unwrap(), w::Reply::List { entries } if entries.len() == 1)
    );
    assert_eq!(
        first
            .register_workspace(WorkspaceLease {
                workspace: WorkspaceID([77; 16]),
                ..replacement.lease.clone()
            })
            .err()
            .unwrap()
            .code,
        ErrorCode::InvalidArgument
    );
    assert!(
        first
            .call(
                w::Call::List {
                    file: w::File::from_core(&root).unwrap()
                },
                None
            )
            .is_ok()
    );
}
#[test]
fn upload_refusals_before_commit_preserve_error_and_prove_no_effect() {
    for (stage, code, expected) in [
        ("begin", "unavailable", ErrorCode::Unavailable),
        ("end", "Io", ErrorCode::Io),
    ] {
        let (_dir, remote, peer) = scripted_peer(stage, code);
        let receipt = run(FileService::save(
            &remote,
            SaveRequest {
                operation: OperationID([72; 16]),
                file: FileRef {
                    target: remote.target.clone(),
                    path: TargetPath::posix(b"/never-written".to_vec()).unwrap(),
                    resolved_identity: None,
                },
                expected_version: None,
                submitted_revision: BufferRevision(1),
                source: ByteSource::Memory(b"private upload".to_vec().into()),
            },
        ));
        proven(receipt, expected);
        let seen = peer.join().unwrap();
        assert!(!seen.iter().any(|m| matches!(
            m,
            Message::SaveCommitOwned { .. } | Message::CreateCommit { .. }
        )));
        assert_eq!(
            seen.iter()
                .filter(|m| matches!(m, Message::UploadBegin { .. }))
                .count(),
            1
        );
    }
}
#[test]
fn generic_errors_after_submission_do_not_prove_no_effect() {
    for code in ["busy", "unavailable", "Io", "closed"] {
        let (_dir, remote, peer) = scripted_peer("service", code);
        assert!(matches!(mkdir(&remote).state, OperationState::Unknown(_)));
        assert_eq!(peer.join().unwrap().len(), 1);
    }
    let (_dir, remote, peer) = scripted_peer("commit", "busy");
    let receipt = run(FileService::save(
        &remote,
        SaveRequest {
            operation: OperationID([73; 16]),
            file: FileRef {
                target: remote.target.clone(),
                path: TargetPath::posix(b"/may-have-written".to_vec()).unwrap(),
                resolved_identity: None,
            },
            expected_version: None,
            submitted_revision: BufferRevision(1),
            source: ByteSource::Memory(b"upload".to_vec().into()),
        },
    ));
    assert!(matches!(receipt.state, OperationState::Unknown(_)));
    assert_eq!(
        peer.join()
            .unwrap()
            .iter()
            .filter(|m| matches!(m, Message::SaveCommitOwned { .. }))
            .count(),
        1
    );
}

#[test]
fn deadline_before_worker_send_is_not_an_unsent_refusal() {
    let gate = Arc::new(std::sync::Barrier::new(2));
    let (_dir, remote, peer) = scripted_peer_with_gate("service", "busy", Some(gate.clone()));
    let id = OperationID([74; 16]);
    let mut request = remote
        .transport
        .start(
            Message::ServiceCall {
                call: w::Call::FileMutate {
                    file: w::File {
                        path_base64: B64.encode(b"/queued"),
                        resolved_identity: None,
                    },
                    expected: None,
                    command: w::FileCommand::CreateDirectory,
                },
            },
            Some(id),
            None,
        )
        .unwrap();
    // The real worker has not started. Exercise the actual deadline branch with
    // a zero wait, then classify it through the same proxy mutation error path.
    let error = request.next_with_timeout(Duration::ZERO).unwrap_err();
    assert_eq!(error.code, ErrorCode::Unknown);
    let result = super::super::operations::request_failure(&request, id, &remote.target, error);
    gate.wait();
    // Timing out did not remove the queued command: the worker can still send it.
    assert!(matches!(
        request.next().unwrap().header.message,
        Message::Error { .. }
    ));
    request.finish();
    assert_eq!(peer.join().unwrap().len(), 1);
    assert!(
        matches!(result.state, OperationState::Unknown(_)),
        "deadline falsely proved no effects: {result:?}"
    );
}

#[test]
fn cancelled_worker_view_refuses_before_admission_without_cancelling_its_origin() {
    let (_server, remote, root) = fixture();
    let cancelled = remote.with_cancellation(Arc::new(AtomicBool::new(true)));
    proven(mkdir(&cancelled), ErrorCode::Cancelled);
    assert!(matches!(
        remote
            .call(
                w::Call::List {
                    file: w::File::from_core(&root).unwrap()
                },
                None
            )
            .unwrap(),
        w::Reply::List { .. }
    ));
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
}

#[test]
fn old_lease_capability_cannot_confirm_process_retirement() {
    let (_server, mut remote, file) = fixture();
    remote.transport.capabilities = Arc::new(
        remote
            .negotiated_capabilities()
            .iter()
            .filter(|c| c.as_str() != "leases.retirement")
            .cloned()
            .collect(),
    );
    let result = remote.release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure);
    assert!(
        result.is_err(),
        "generic leases ACK from an older helper cannot prove process retirement"
    );
    assert!(
        remote.transport.scope.active.load(Ordering::SeqCst),
        "unsupported close must not revoke admission"
    );
    assert!(matches!(
        remote
            .call(
                w::Call::List {
                    file: w::File::from_core(&file).unwrap()
                },
                None
            )
            .unwrap(),
        w::Reply::List { .. }
    ));
}

#[test]
fn old_terminal_capability_cannot_replace_ordered_renderer_or_size() {
    let (_server, mut remote, file) = fixture();
    remote.transport.capabilities = Arc::new(
        remote
            .negotiated_capabilities()
            .iter()
            .filter(|c| c.as_str() != "terminal.ordered_controls")
            .cloned()
            .collect(),
    );
    let attached = crate::worker::run(remote.attach(TerminalAttachRequest {
        session: TerminalSessionID([90; 16]),
        owner: remote.lease().clone(),
        runtime: remote.runtime(),
        last_output_sequence: 0,
    }));
    assert!(
        attached.is_err(),
        "legacy terminal capability does not guarantee ordered renderer replacement"
    );
    assert!(
        remote
            .admit_terminal_resize(
                TerminalSessionID([90; 16]),
                TerminalSize {
                    columns: 80.try_into().unwrap(),
                    rows: 24.try_into().unwrap()
                }
            )
            .is_err()
    );
    assert!(
        remote
            .admit_terminal_input(TerminalSessionID([90; 16]), b"must not send".to_vec())
            .is_err()
    );
    assert!(remote.transport.scope.active.load(Ordering::SeqCst));
    assert!(matches!(
        remote
            .call(
                w::Call::List {
                    file: w::File::from_core(&file).unwrap()
                },
                None
            )
            .unwrap(),
        w::Reply::List { .. }
    ));
}

#[test]
fn owner_observation_does_not_resume_or_confirm_a_release() {
    let (_server, first, root) = fixture();
    let second = first
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([61; 16]),
            lease: WorkspaceLeaseID([62; 16]),
            ..first.lease.clone()
        })
        .unwrap();
    first.set_authentication(None).unwrap();
    let opened = crate::worker::run(first.open(TerminalOpenRequest {
        operation: OperationID([90; 16]),
        session: TerminalSessionID([91; 16]),
        owner: first.lease.clone(),
        target: first.target.clone(),
        runtime: first.runtime(),
        directory: root.path.clone(),
        shell: TargetPath::posix(b"/bin/sh".to_vec()).unwrap(),
        size: TerminalSize {
            columns: std::num::NonZeroU16::new(80).unwrap(),
            rows: std::num::NonZeroU16::new(24).unwrap(),
        },
    }));
    assert!(matches!(opened.state, OperationState::Confirmed(_)));
    let observed = second.observe_owner(first.lease.clone()).unwrap();
    assert_eq!(observed.presence, LeasePresence::Active);
    assert!(
        !observed.workers_retired,
        "live target worker was reported retired"
    );
    first
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    let observed = second.observe_owner(first.lease.clone()).unwrap();
    let LeasePresence::Detached {
        remaining_millis: before,
    } = observed.presence
    else {
        panic!("detachment lost")
    };
    std::thread::sleep(Duration::from_millis(25));
    let observed = second.observe_owner(first.lease.clone()).unwrap();
    let LeasePresence::Detached {
        remaining_millis: after,
    } = observed.presence
    else {
        panic!("observation resumed owner")
    };
    assert!(after < before, "read-only observation renewed retention");
    assert!(after <= 1_800_000);
    let wrong = WorkspaceLease {
        workspace: WorkspaceID([63; 16]),
        ..first.lease.clone()
    };
    assert!(second.observe_owner(wrong).is_err());
    let wrong = WorkspaceLease {
        client: ClientID([63; 16]),
        ..first.lease.clone()
    };
    assert!(second.observe_owner(wrong).is_err());
    let old = WorkspaceLease {
        runtime: RuntimeGeneration {
            supervisor_boot: [64; 16],
            container_start: None,
        },
        ..first.lease.clone()
    };
    let mixed_wrong = WorkspaceLease {
        workspace: WorkspaceID([63; 16]),
        ..old.clone()
    };
    assert!(
        second.observe_owner(mixed_wrong).is_err(),
        "runtime mismatch hid an existing owner identity mismatch"
    );
    assert_eq!(
        second.observe_owner(old).unwrap().presence,
        LeasePresence::RuntimeLost
    );
    // Explicit resume is separate from observation and remains owner-specific.
    let resumed = second.register_workspace(first.lease.clone()).unwrap();
    resumed
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    let observed = second.observe_owner(first.lease.clone()).unwrap();
    assert_eq!(observed.presence, LeasePresence::Absent);
    assert!(observed.workers_retired);
    assert!(
        second
            .call(
                w::Call::List {
                    file: w::File::from_core(&root).unwrap()
                },
                None
            )
            .is_ok()
    );
    second
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    crate::worker::run(Box::pin(second.close_connection()));
}

#[test]
fn environment_readiness_requires_current_auth_ack_and_all_live_capabilities() {
    let (_server, mut remote, _) = fixture();
    assert!(
        remote.validate_environment_ready().is_err(),
        "handshake alone is not authenticated environment readiness"
    );
    remote.set_authentication(None).unwrap();
    remote.validate_environment_ready().unwrap();
    for missing in [
        "leases.retirement",
        "terminal.ordered_controls",
        "leases.observe",
        "git.existing",
        "file.save_proof",
    ] {
        let full = remote.transport.capabilities.clone();
        remote.transport.capabilities =
            Arc::new(full.iter().filter(|c| *c != missing).cloned().collect());
        assert_eq!(
            remote.validate_environment_ready().unwrap_err().code,
            ErrorCode::Unavailable
        );
        assert_eq!(
            remote.connection_state(),
            ConnectionState::Ready,
            "compatibility refusal killed active helper connection"
        );
        remote.transport.capabilities = full;
    }
    crate::worker::run(Box::pin(remote.close_connection()));
    assert!(remote.validate_environment_ready().is_err());
}
#[test]
fn container_capability_never_replaces_current_authentication_readiness() {
    let (_server, mut remote, _) = fixture();
    remote.set_authentication(None).unwrap();
    remote.target.container = Some(ContainerIdentity {
        engine_endpoint: b"test capability validation only".to_vec(),
        immutable_id: vec![b'a'; 64],
    });
    let full = remote.transport.capabilities.clone();
    assert!(
        full.iter()
            .any(|capability| capability == "container.binding.v1")
    );
    remote.transport.capabilities = Arc::new(
        full.iter()
            .filter(|capability| *capability != "container.binding.v1")
            .cloned()
            .collect(),
    );
    let missing_capability = remote.validate_connection_ready().unwrap_err();
    assert_eq!(missing_capability.code, ErrorCode::Unavailable);
    assert!(
        missing_capability
            .message
            .contains("container identity/runtime")
    );
    remote.transport.capabilities = full.clone();
    remote.validate_environment_ready().unwrap();
    remote
        .transport
        .authentication_ready
        .store(false, Ordering::SeqCst);
    let missing_auth = remote.validate_connection_ready().unwrap_err();
    assert!(
        missing_auth
            .message
            .contains("authentication has not been acknowledged")
    );
    remote.transport.capabilities = Arc::new(
        full.iter()
            .filter(|capability| *capability != "container.binding.v1")
            .cloned()
            .collect(),
    );
    assert!(
        remote
            .validate_connection_ready()
            .unwrap_err()
            .message
            .contains("authentication has not been acknowledged")
    );
    remote
        .transport
        .authentication_ready
        .store(true, Ordering::SeqCst);
    remote.transport.capabilities = full;
    crate::worker::run(Box::pin(remote.close_connection()));
}
#[test]
fn terminal_attach_rejects_changed_container_start_even_with_the_same_boot() {
    let (_server, remote, _) = fixture();
    remote.set_authentication(None).unwrap();
    let mut original = remote.lease().clone();
    original.runtime.container_start = Some(b"different container start".to_vec());
    let request = TerminalAttachRequest {
        session: TerminalSessionID([93; 16]),
        owner: original.clone(),
        runtime: original.runtime,
        last_output_sequence: 0,
    };
    let error = match crate::worker::run(remote.attach(request)) {
        Ok(_) => panic!("old container start must refuse before attachment"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::TargetChanged);
    assert_eq!(error.message, "terminal owner or runtime changed");
    remote
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    crate::worker::run(Box::pin(remote.close_connection()));
}
#[test]
fn environment_readiness_does_not_reactivate_a_released_view() {
    let (_server, remote, _) = fixture();
    remote.set_authentication(None).unwrap();
    let sibling = remote
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([71; 16]),
            lease: WorkspaceLeaseID([72; 16]),
            ..remote.lease.clone()
        })
        .unwrap();
    remote
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    assert!(
        remote.validate_environment_ready().is_err(),
        "released owner remained environment-ready"
    );
    assert!(
        crate::worker::run(remote.register(remote.lease.clone())).is_err(),
        "generic lease service revived released owner"
    );
    sibling.validate_environment_ready().unwrap();
    sibling
        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
        .unwrap();
    crate::worker::run(Box::pin(sibling.close_connection()));
}

#[test]
fn physical_idle_cannot_overtake_registration_or_release_transition() {
    let (_server, remote, _) = fixture();
    let lifetime = remote.transport.lifetime();
    let transition = remote.transport.registrations.lock().unwrap();
    // Exact release phase: admission revoked, wire request not yet queued.
    remote.transport.scope.active.store(false, Ordering::SeqCst);
    assert!(
        !lifetime.idle(),
        "physical retirement overtook a registered-owner transition"
    );
    drop(transition);
    assert!(lifetime.idle());
}

#[test]
fn git_existing_is_read_only_digest_checked_and_preserves_unknown() {
    use buntrail_services::operations::ledger::Ledger;
    let (server, mut remote, root) = fixture();
    let directory = server.dir.path().join("project");
    assert!(
        std::process::Command::new("git")
            .current_dir(&directory)
            .args(["init", "-b", "main"])
            .output()
            .unwrap()
            .status
            .success()
    );
    let worktree = run(remote.discover(root.clone())).unwrap().unwrap();
    let status = run(remote.status(worktree.clone())).unwrap();
    let mut mutation = GitMutationRequest {
        operation: OperationID([91; 16]),
        worktree,
        expected: status.state,
        command: GitMutation::Stage(vec![status.untracked[0].path.clone()]),
    };
    let ledger = Ledger::open(server.dir.path().join("state/files/operations")).unwrap();
    // Saturate only the ordinary queue; digest lookup must retain inspection admission.
    let (full, _receiver) = mpsc::sync_channel(1);
    let ordinary = std::mem::replace(&mut remote.transport.sender, full);
    let held = remote
        .transport
        .start(
            Message::Authentication {
                ssh_auth_sock: None,
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        run(remote.existing_receipt(mutation.clone())).unwrap(),
        None
    );
    remote.transport.sender = ordinary;
    drop(held);
    assert!(
        ledger.read(mutation.operation).unwrap().is_none(),
        "lookup accepted work"
    );
    let confirmed = run(GitService::mutate(&remote, mutation.clone()));
    assert!(
        matches!(confirmed.state, OperationState::Confirmed(_)),
        "{confirmed:?}"
    );
    std::fs::rename(&directory, server.dir.path().join("old-project")).unwrap();
    assert_eq!(
        run(remote.existing_receipt(mutation.clone())).unwrap(),
        Some(confirmed.clone())
    );
    for field in 0..5 {
        let mut different = mutation.clone();
        match field {
            0 => {
                different.command = GitMutation::Commit {
                    message: "changed".into(),
                }
            }
            1 => different.expected.index_fingerprint[0] ^= 1,
            2 => different.expected.worktree_fingerprint[0] ^= 1,
            3 => different.expected.head = GitHead::Unborn(b"other".to_vec()),
            _ => different.worktree.common_git_directory_identity.push(99),
        }
        assert_eq!(
            run(remote.existing_receipt(different)).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
    }
    let mut foreign = mutation.clone();
    foreign.worktree.target.security_binding.push(99); // Invalid domain input, never a verified target override.
    assert_eq!(
        run(remote.existing_receipt(foreign)).unwrap_err().code,
        ErrorCode::TargetChanged
    );
    let mut record = ledger.read(mutation.operation).unwrap().unwrap();
    record.created = 0; // Private ledger fixture: simulate an expired original receipt.
    {
        let _lock = ledger.lock().unwrap();
        ledger.write(&record).unwrap();
    }
    assert!(matches!(
        run(remote.existing_receipt(mutation.clone()))
            .unwrap()
            .unwrap()
            .state,
        OperationState::Unknown(_)
    ));
    assert_eq!(
        ledger.read(mutation.operation).unwrap().unwrap().receipt,
        confirmed,
        "lookup rewrote the ledger"
    );
    record.created = buntrail_services::operations::ledger::now();
    record.receipt.state = OperationState::Running; // Interrupted original operation fixture.
    {
        let _lock = ledger.lock().unwrap();
        ledger.write(&record).unwrap();
    }
    assert!(matches!(
        run(remote.existing_receipt(mutation.clone()))
            .unwrap()
            .unwrap()
            .state,
        OperationState::Unknown(_)
    ));
    assert_eq!(
        ledger
            .read(mutation.operation)
            .unwrap()
            .unwrap()
            .receipt
            .state,
        OperationState::Running
    );
    let caps = remote.transport.capabilities.clone();
    for supported in ["git.original_lookup", "git.existing"] {
        remote.transport.capabilities = Arc::new(
            caps.iter()
                .filter(|c| {
                    !matches!(c.as_str(), "git.existing" | "git.original_lookup")
                        || c.as_str() == supported
                })
                .cloned()
                .collect(),
        );
        assert!(matches!(
            run(remote.existing_receipt(mutation.clone()))
                .unwrap()
                .unwrap()
                .state,
            OperationState::Unknown(_)
        ));
        assert_eq!(
            ledger
                .read(mutation.operation)
                .unwrap()
                .unwrap()
                .receipt
                .state,
            OperationState::Running,
            "{supported} lookup rewrote the interrupted original"
        );
    }
    remote.transport.capabilities = Arc::new(
        caps.iter()
            .filter(|c| !matches!(c.as_str(), "git.existing" | "git.original_lookup"))
            .cloned()
            .collect(),
    );
    assert_eq!(
        run(remote.existing_receipt(mutation.clone()))
            .unwrap_err()
            .code,
        ErrorCode::Unavailable
    );
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    remote.transport.capabilities = caps;
    mutation.operation = OperationID([92; 16]);
    assert_eq!(
        run(remote.existing_receipt(mutation.clone())).unwrap(),
        None
    );
    assert!(ledger.read(mutation.operation).unwrap().is_none());
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn actual_helper_unprepared_worktree_guard_preserves_duplicates_and_unresolved_capacity() {
    let (server, mut remote, root) = fixture();
    let directory = server.dir.path().join("project");
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
        vec!["add", "file"],
        vec!["commit", "-m", "initial"],
        vec!["branch", "other"],
    ] {
        let output = std::process::Command::new("git")
            .current_dir(&directory)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let worktree = run(remote.discover(root)).unwrap().unwrap();
    let state = run(remote.status(worktree.clone())).unwrap().state;
    let original = GitMutationRequest {
        operation: OperationID([94; 16]),
        worktree: worktree.clone(),
        expected: state,
        command: GitMutation::Checkout {
            reference: b"other".to_vec(),
        },
    };
    for request in [
        original.clone(),
        GitMutationRequest {
            operation: OperationID([95; 16]),
            command: GitMutation::PullFastForward {
                remote: directory.as_os_str().as_encoded_bytes().to_vec(),
                reference: b"main".to_vec(),
            },
            ..original.clone()
        },
    ] {
        let receipt = run(GitService::mutate(&remote, request.clone()));
        assert!(
            matches!(
                receipt.state,
                OperationState::Failed(OperationFailure {
                    error: CoreError {
                        code: ErrorCode::Unavailable,
                        ..
                    },
                    effect: FailureEffect::ProvenNoEffect,
                    ..
                })
            ),
            "{receipt:?}"
        );
        assert_eq!(run(remote.existing_receipt(request)).unwrap(), None);
    }
    assert!(!directory.join(".git/FETCH_HEAD").exists());
    let local_files = Arc::new(
        buntrail_services::files::LocalFileService::new(
            remote.target.clone(),
            server.dir.path().join("state/files"),
        )
        .unwrap(),
    );
    let local_git = buntrail_services::git::LocalGitService::new(local_files.clone());
    let accepted = local_git.mutate_sync(original.clone(), &AtomicBool::new(false));
    assert!(matches!(accepted.state, OperationState::Confirmed(_)));
    let ledger = buntrail_services::operations::ledger::Ledger::open(
        server.dir.path().join("state/files/operations"),
    )
    .unwrap();
    let record_path = ledger.directory.join(format!("{}.json", "5e".repeat(16)));
    let durable = std::fs::read(&record_path).unwrap();
    std::fs::rename(&directory, server.dir.path().join("moved-project")).unwrap();
    let (full, _receiver) = mpsc::sync_channel(1);
    let ordinary = std::mem::replace(&mut remote.transport.sender, full);
    let held = remote
        .transport
        .start(
            Message::Authentication {
                ssh_auth_sock: None,
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(run(GitService::mutate(&remote, original.clone())), accepted);
    remote.transport.sender = ordinary;
    drop(held);
    let (full, _receiver) = mpsc::sync_channel(1);
    let controls = std::mem::replace(&mut remote.transport.control, full);
    let held = remote
        .transport
        .start(
            Message::ServiceCall {
                call: w::Call::Inspect,
            },
            Some(OperationID([96; 16])),
            None,
        )
        .unwrap();
    assert!(
        matches!(
            run(GitService::mutate(&remote, original.clone())).state,
            OperationState::Unknown(_)
        ),
        "full lookup control queue cannot invent a no-effect failure for an existing operation"
    );
    remote.transport.control = controls;
    drop(held);
    assert_eq!(std::fs::read(record_path).unwrap(), durable);
    assert_eq!(run(GitService::mutate(&remote, original)), accepted);
}

#[test]
fn owned_save_authenticates_exact_lease_and_records_original_actor() {
    let (server, mut remote, root) = fixture();
    let alias = server.dir.path().join("outside-worktree-alias");
    std::os::unix::fs::symlink("project/file", &alias).unwrap();
    let file = FileRef {
        target: root.target.clone(),
        path: TargetPath::posix(alias.as_os_str().as_encoded_bytes().to_vec()).unwrap(),
        resolved_identity: None,
    };
    let snapshot = crate::worker::run(FileService::read(&remote, file.clone())).unwrap();
    let request = SaveRequest {
        operation: OperationID([104; 16]),
        file: snapshot.file,
        expected_version: Some(snapshot.version),
        submitted_revision: BufferRevision(2),
        source: ByteSource::Memory(Arc::from(b"owned\n".as_slice())),
    };
    let capabilities = remote.transport.capabilities.clone();
    remote.transport.capabilities = Arc::new(
        capabilities
            .iter()
            .filter(|c| *c != "file.save_proof")
            .cloned()
            .collect(),
    );
    let unsupported = crate::worker::run(FileService::save(&remote, request.clone()));
    assert!(matches!(
        unsupported.state,
        OperationState::Failed(OperationFailure {
            error: CoreError {
                code: ErrorCode::Unavailable,
                ..
            },
            effect: FailureEffect::ProvenNoEffect,
            ..
        })
    ));
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"needle\n"
    );
    assert_eq!(
        std::fs::read_dir(server.dir.path().join("state/uploads"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    remote.transport.capabilities = capabilities;
    let receipt = crate::worker::run(FileService::save(&remote, request.clone()));
    assert!(
        matches!(
            receipt.state,
            OperationState::Confirmed(MutationResult::Saved(_))
        ),
        "{receipt:?}"
    );
    let record_path = server
        .dir
        .path()
        .join("state/files/operations")
        .join(format!("{}.json", "68".repeat(16)));
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
    let proof = record
        .get("save_proof")
        .expect("target must record exact authenticated actor at acceptance");
    assert_eq!(
        proof["actor"],
        serde_json::to_value(remote.lease()).unwrap()
    );
    assert!(!proof["after"].is_null());
    assert_ne!(
        proof["after"]["requested"], proof["after"]["canonical"],
        "outside alias lost its independent resolution evidence"
    );
    let original_owner = remote.lease.clone();
    remote.lease.lease = WorkspaceLeaseID([105; 16]);
    let snapshot = crate::worker::run(FileService::read(&remote, file)).unwrap();
    let forged = SaveRequest {
        operation: OperationID([106; 16]),
        file: snapshot.file,
        expected_version: Some(snapshot.version),
        submitted_revision: BufferRevision(3),
        source: ByteSource::Memory(Arc::from(b"forged\n".as_slice())),
    };
    let denied = crate::worker::run(FileService::save(&remote, forged));
    assert!(
        !matches!(denied.state, OperationState::Confirmed(_)),
        "unregistered body lease gained authority from another Context lease"
    );
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"owned\n"
    );
    assert!(
        !server
            .dir
            .path()
            .join("state/files/operations")
            .join(format!("{}.json", "6a".repeat(16)))
            .exists()
    );
    assert_eq!(
        std::fs::read_dir(server.dir.path().join("state/uploads"))
            .unwrap()
            .count(),
        0,
        "refused owned commit retained its completed upload"
    );
    remote.lease = original_owner;
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn staged_remote_save_freezes_effective_identity_before_dispatch() {
    let (server, remote, root) = fixture();
    let file = FileRef {
        path: TargetPath::posix(
            server
                .dir
                .path()
                .join("project/file")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        ..root
    };
    let snapshot = run(FileService::read(&remote, file)).unwrap();
    let alias = snapshot.file.clone();
    assert!(alias.resolved_identity.is_some());
    let mut request = SaveRequest {
        operation: OperationID([121; 16]),
        file: alias.clone(),
        expected_version: Some(snapshot.version),
        submitted_revision: BufferRevision(7),
        source: ByteSource::Memory(Arc::from(b"frozen\n".as_slice())),
    };
    let staged = remote
        .prepare_owned_save(request.clone())
        .expect("stage immutable save before sending upload");
    let approval = staged.approval().clone();
    assert_eq!(approval.actor, *remote.lease());
    assert_eq!(approval.operation, request.operation);
    let mut effective = request.clone();
    effective.file.resolved_identity = None;
    let expected = buntrail_services::files::save_request_digest(
        &effective.file,
        &effective.expected_version,
        effective.submitted_revision,
        sha2::Sha256::digest(b"frozen\n").into(),
    )
    .unwrap();
    assert_eq!(approval.request_digest, expected);
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"needle\n"
    );
    assert_eq!(
        std::fs::read_dir(server.dir.path().join("state/uploads"))
            .unwrap()
            .count(),
        0
    );
    let duplicate_request = request.clone();
    request.source = ByteSource::Memory(Arc::from(
        b"later caller bytes
"
        .as_slice(),
    ));
    request.submitted_revision = BufferRevision(99);
    let result = staged.execute();
    assert!(
        matches!(&result.state,OperationState::Confirmed(MutationResult::Saved(saved)) if saved.saved_revision==BufferRevision(7)),
        "{result:?}"
    );
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"frozen\n"
    );
    assert_eq!(
        remote
            .prepare_owned_save(duplicate_request)
            .unwrap()
            .execute(),
        result
    );
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            server
                .dir
                .path()
                .join("state/files/operations")
                .join(format!("{}.json", "79".repeat(16))),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        record["envelope"]["request_digest"],
        serde_json::to_value(approval.request_digest).unwrap()
    );
    assert_eq!(
        record["save_proof"]["actor"],
        serde_json::to_value(&approval.actor).unwrap()
    );
    assert_eq!(request.file, alias);
    remote
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    assert!(remote.prepare_owned_save(request).is_err());
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn actual_framed_proof_observe_save_prepare_later_save_execute() {
    let (server, remote, root) = fixture();
    let directory = server.dir.path().join("project");
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
        vec!["add", "file"],
        vec!["commit", "-m", "initial"],
        vec!["branch", "other"],
    ] {
        let out = std::process::Command::new("git")
            .current_dir(&directory)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let worktree = run(remote.discover(root.clone())).unwrap().unwrap();
    let file = FileRef {
        path: TargetPath::posix(
            directory
                .join("file")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        ..root
    };
    let snapshot = run(FileService::read(&remote, file.clone())).unwrap();
    let alias = snapshot.file.clone();
    let pending = remote
        .prepare_owned_save(SaveRequest {
            operation: OperationID([122; 16]),
            file: alias.clone(),
            expected_version: Some(snapshot.version),
            submitted_revision: BufferRevision(8),
            source: ByteSource::Memory(Arc::from(b"pending\n".as_slice())),
        })
        .unwrap();
    let approval = pending.approval().clone();
    let mut aliases = 0;
    let observation = remote
        .observe_owned(worktree.clone(), std::iter::once(&alias), |event| {
            if let ObservationEvent::Alias(record) = event {
                assert!(record.matches_alias(&alias)?);
                aliases += 1;
            }
            Ok(())
        })
        .expect("actual owned observation stream must finish");
    assert_eq!(aliases, 1);
    assert!(matches!(
        pending.execute().state,
        OperationState::Confirmed(_)
    ));
    let original = GitMutationRequest {
        operation: OperationID([123; 16]),
        worktree,
        expected: observation.state.clone(),
        command: GitMutation::Checkout {
            reference: b"other".to_vec(),
        },
    };
    let WorktreePreparation::Prepared(plan) = remote
        .prepare_worktree(
            &original,
            observation.id,
            observation.alias_digest,
            std::iter::once(&approval),
        )
        .unwrap()
    else {
        panic!("new work must prepare")
    };
    let second = remote
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([124; 16]),
            lease: WorkspaceLeaseID([125; 16]),
            ..remote.lease().clone()
        })
        .unwrap();
    let snapshot = run(FileService::read(&second, file)).unwrap();
    let later = second
        .prepare_owned_save(SaveRequest {
            operation: OperationID([126; 16]),
            file: snapshot.file,
            expected_version: Some(snapshot.version),
            submitted_revision: BufferRevision(9),
            source: ByteSource::Memory(Arc::from(b"later\n".as_slice())),
        })
        .unwrap();
    let later_approval = later.approval().clone();
    assert!(matches!(
        later.execute().state,
        OperationState::Confirmed(_)
    ));
    second
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    let result = remote
        .execute_worktree(
            original.clone(),
            plan,
            observation.alias_digest,
            std::iter::once(&later_approval),
        )
        .unwrap();
    assert!(
        matches!(result.state, OperationState::Confirmed(_)),
        "{result:?}"
    );
    assert_eq!(std::fs::read(directory.join("file")).unwrap(), b"later\n");
    let invalid = GitSaveApproval {
        actor: WorkspaceLease {
            client: ClientID([0; 16]),
            ..approval.actor
        },
        ..approval
    };
    let duplicate = remote
        .with_cancellation(Arc::new(AtomicBool::new(true)))
        .execute_worktree(
            original.clone(),
            [0; 16],
            [0; 32],
            std::iter::once(&invalid)
                .map(|_| panic!("existing receipt must precede approval iteration")),
        )
        .unwrap();
    assert_eq!(duplicate, result);
    let mut queued = remote.clone();
    let (full, _receiver) = mpsc::sync_channel(1);
    queued.transport.sender = full;
    let held = queued
        .transport
        .start(
            Message::Authentication {
                ssh_auth_sock: None,
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        queued
            .execute_worktree(
                original.clone(),
                [0; 16],
                [0; 32],
                std::iter::once(&invalid)
            )
            .unwrap(),
        result
    );
    drop(held);
    let mut queued = remote.clone();
    let (full, _receiver) = mpsc::sync_channel(1);
    queued.transport.control = full;
    let held = queued
        .transport
        .start(
            Message::ServiceCall {
                call: w::Call::Inspect,
            },
            Some(OperationID([133; 16])),
            None,
        )
        .unwrap();
    assert!(
        queued
            .execute_worktree(original, [0; 16], [0; 32], std::iter::once(&invalid))
            .is_err(),
        "full original-control queue must remain unresolved"
    );
    drop(held);
    remote.cancel_worktree_plan(plan).unwrap();
    remote.release_observation(observation.id).unwrap();
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
#[ignore = "requires the preserved historical helper artifact via BUNTRAIL_OLD_HELPER_BINARY"]
fn actual_old_helper_refuses_proof_before_frames_and_keeps_save_alive() {
    let binary = std::path::PathBuf::from(
        std::env::var_os("BUNTRAIL_OLD_HELPER_BINARY")
            .expect("provide the preserved historical helper artifact"),
    );
    assert_eq!(
        w::hex(&sha2::Sha256::digest(std::fs::read(&binary).unwrap())),
        "936f47047db5c3a22612a7090f831c3567f3c63bfec4dc8327f4108c45242956"
    );
    let (server, remote, root) = fixture_server(support::Server::start_binary(&binary));
    assert!(
        !remote
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "git.worktree_proof")
    );
    let ids = remote.transport.ids.load(Ordering::SeqCst);
    let worktree = WorktreeIdentity {
        target: remote.target.clone(),
        canonical_path: root.path.clone(),
        common_git_directory_identity: vec![],
    };
    assert_eq!(
        remote
            .observe_owned(worktree, std::iter::empty(), |_| Ok(()))
            .unwrap_err()
            .code,
        ErrorCode::Unavailable
    );
    assert_eq!(
        remote.transport.ids.load(Ordering::SeqCst),
        ids,
        "old helper refusal must allocate no request or send proof bytes"
    );
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    let file = FileRef {
        path: TargetPath::posix(
            server
                .dir
                .path()
                .join("project/file")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        ..root
    };
    let snapshot = run(FileService::read(&remote, file)).unwrap();
    let save = remote
        .prepare_owned_save(SaveRequest {
            operation: OperationID([127; 16]),
            file: snapshot.file,
            expected_version: Some(snapshot.version),
            submitted_revision: BufferRevision(10),
            source: ByteSource::Memory(Arc::from(b"old helper ordinary save\n".as_slice())),
        })
        .unwrap();
    let approval = save.approval().clone();
    assert!(matches!(save.execute().state, OperationState::Confirmed(_)));
    assert_eq!(approval.actor, *remote.lease());
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"old helper ordinary save\n"
    );
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn actual_chunked_owned_status_preserves_all_groups_and_exact_alias_hints() {
    let (server, remote, root) = fixture();
    let directory = server.dir.path().join("project");
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .current_dir(&directory)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-b", "main"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["add", "file"]);
    git(&["commit", "-m", "initial"]);
    git(&["checkout", "-b", "conflicting"]);
    std::fs::write(directory.join("conflict"), b"other\n").unwrap();
    git(&["add", "conflict"]);
    git(&["commit", "-m", "other"]);
    git(&["checkout", "main"]);
    std::fs::write(directory.join("conflict"), b"main\n").unwrap();
    git(&["add", "conflict"]);
    git(&["commit", "-m", "main"]);
    let out = std::process::Command::new("git")
        .current_dir(&directory)
        .args(["merge", "conflicting"])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(!out.status.success());
    std::fs::write(directory.join("file"), b"unstaged\n").unwrap();
    std::fs::write(directory.join("staged"), b"staged\n").unwrap();
    git(&["add", "staged"]);
    for n in 0..650 {
        std::fs::write(
            directory.join(format!("{n:04}-{}", "x".repeat(100))),
            b"untracked\n",
        )
        .unwrap();
    }
    let worktree = run(remote.discover(root.clone())).unwrap().unwrap();
    let file = FileRef {
        path: TargetPath::posix(
            directory
                .join("file")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        ..root
    };
    let aliases = [
        file.clone(),
        FileRef {
            resolved_identity: Some(Vec::new()),
            ..file.clone()
        },
        FileRef {
            resolved_identity: Some(vec![255; 196615]),
            ..file.clone()
        },
    ];
    let mut counts = [0usize; 5];
    let mut status_path_bytes = 0;
    let observation = remote
        .observe_owned(worktree.clone(), aliases.iter(), |event| {
            match event {
                ObservationEvent::Entry { group, entry } => {
                    counts[group as usize - 1] += 1;
                    if let TargetPath::PosixBytes(b) = entry.path {
                        status_path_bytes += b.len();
                    }
                }
                ObservationEvent::Alias(record) => {
                    assert!(record.matches_alias(&aliases[counts[4]])?);
                    counts[4] += 1;
                }
            }
            Ok(())
        })
        .unwrap();
    assert!(
        counts[..4].iter().all(|n| *n > 0),
        "all four status groups must be streamed: {counts:?}"
    );
    assert_eq!(counts[4], 3);
    assert!(
        status_path_bytes > 65536,
        "actual wire output must cross a chunk boundary"
    );
    remote.release_observation(observation.id).unwrap();
    let refused = remote.observe_owned(worktree.clone(), std::iter::once(&file), |_| {
        Err(CoreError::new(
            ErrorCode::Cancelled,
            "consumer discarded provisional events",
        ))
    });
    assert!(refused.is_err());
    let again = remote
        .observe_owned(worktree, std::iter::once(&file), |_| Ok(()))
        .unwrap();
    remote.release_observation(again.id).unwrap();
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn actual_proof_wire_reflected_creation_external_and_invalid_history() {
    for case in ["reflected", "creation", "external", "history"] {
        let (server, remote, root) = fixture();
        let directory = server.dir.path().join("project");
        for args in [
            vec!["init", "-b", "main"],
            vec!["config", "user.name", "Fixture"],
            vec!["config", "user.email", "fixture@example.invalid"],
            vec!["add", "file"],
            vec!["commit", "-m", "initial"],
            vec!["branch", "other"],
        ] {
            let out = std::process::Command::new("git")
                .current_dir(&directory)
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap();
            assert!(out.status.success());
        }
        let worktree = run(remote.discover(root.clone())).unwrap().unwrap();
        let file = FileRef {
            path: TargetPath::posix(
                directory
                    .join(if case == "creation" {
                        "created"
                    } else {
                        "file"
                    })
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec(),
            )
            .unwrap(),
            ..root
        };
        let snapshot = if case == "creation" {
            None
        } else {
            Some(run(FileService::read(&remote, file.clone())).unwrap())
        };
        let alias = snapshot
            .as_ref()
            .map(|s| s.file.clone())
            .unwrap_or(file.clone());
        let pending = remote
            .prepare_owned_save(SaveRequest {
                operation: OperationID([131; 16]),
                file: alias.clone(),
                expected_version: snapshot.map(|s| s.version),
                submitted_revision: BufferRevision(8),
                source: ByteSource::Memory(Arc::from(b"approved\n".as_slice())),
            })
            .unwrap();
        let approval = pending.approval().clone();
        let pending = if case == "reflected" {
            assert!(matches!(
                pending.execute().state,
                OperationState::Confirmed(_)
            ));
            None
        } else {
            Some(pending)
        };
        let observation = remote
            .observe_owned(worktree.clone(), std::iter::once(&alias), |_| Ok(()))
            .unwrap();
        if let Some(save) = pending {
            assert!(matches!(save.execute().state, OperationState::Confirmed(_)));
        }
        if case == "external" {
            let replacement = directory.join("replacement");
            std::fs::write(&replacement, b"approved\n").unwrap();
            std::fs::rename(replacement, directory.join("file")).unwrap();
        }
        if case == "history" {
            let path = server
                .dir
                .path()
                .join("state/files/operations")
                .join(format!("{}.json", "83".repeat(16)));
            let mut record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            record["save_proof"]["after"] = serde_json::Value::Null;
            std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
        }
        let original = GitMutationRequest {
            operation: OperationID([132; 16]),
            worktree,
            expected: observation.state.clone(),
            command: GitMutation::Checkout {
                reference: b"other".to_vec(),
            },
        };
        let preparation = remote.prepare_worktree(
            &original,
            observation.id,
            observation.alias_digest,
            std::iter::once(&approval),
        );
        if matches!(case, "external" | "history") {
            assert!(preparation.is_err(), "{case} incorrectly granted authority");
            assert!(run(remote.existing_receipt(original)).unwrap().is_none());
        } else {
            let WorktreePreparation::Prepared(plan) = preparation.unwrap() else {
                panic!("expected new plan")
            };
            let receipt = remote
                .execute_worktree(original, plan, observation.alias_digest, std::iter::empty())
                .unwrap();
            assert!(
                matches!(receipt.state, OperationState::Confirmed(_)),
                "{case}: {receipt:?}"
            );
        }
        remote.release_observation(observation.id).unwrap();
        crate::worker::run(Box::pin(remote.close_connection()));
    }
}

#[test]
fn actual_accepted_cursor_and_linked_owner_progress_while_git_is_blocked() {
    use std::os::unix::fs::PermissionsExt;
    let (server, remote, root) = fixture();
    let directory = server.dir.path().join("project");
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .current_dir(&directory)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-b", "main"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["add", "file"]);
    git(&["commit", "-m", "initial"]);
    git(&["branch", "other"]);
    let sibling = server.dir.path().join("linked");
    git(&[
        "worktree",
        "add",
        "-b",
        "sibling",
        sibling.to_str().unwrap(),
    ]);
    let second = remote
        .register_workspace(WorkspaceLease {
            workspace: WorkspaceID([141; 16]),
            lease: WorkspaceLeaseID([142; 16]),
            ..remote.lease().clone()
        })
        .unwrap();
    let a = run(remote.discover(root.clone())).unwrap().unwrap();
    let b = run(second.discover(FileRef {
        path: TargetPath::posix(sibling.as_os_str().as_encoded_bytes().to_vec()).unwrap(),
        ..root
    }))
    .unwrap()
    .unwrap();
    let mut path = match &b.canonical_path {
        TargetPath::PosixBytes(b) => b.clone(),
        _ => unreachable!(),
    };
    path.extend(b"/file");
    let file = FileRef {
        target: remote.target.clone(),
        path: TargetPath::posix(path).unwrap(),
        resolved_identity: None,
    };
    let snapshot = run(FileService::read(&second, file)).unwrap();
    let oa = remote
        .observe_owned(a.clone(), std::iter::empty(), |_| Ok(()))
        .unwrap();
    let ob = second
        .observe_owned(b.clone(), std::iter::once(&snapshot.file), |_| Ok(()))
        .unwrap();
    let ra = GitMutationRequest {
        operation: OperationID([143; 16]),
        worktree: a,
        expected: oa.state.clone(),
        command: GitMutation::Checkout {
            reference: b"other".to_vec(),
        },
    };
    let rb = GitMutationRequest {
        operation: OperationID([144; 16]),
        worktree: b,
        expected: ob.state.clone(),
        command: GitMutation::Checkout {
            reference: b"main".to_vec(),
        },
    };
    let WorktreePreparation::Prepared(pa) = remote
        .prepare_worktree(&ra, oa.id, oa.alias_digest, std::iter::empty())
        .unwrap()
    else {
        panic!()
    };
    let WorktreePreparation::Prepared(pb) = second
        .prepare_worktree(&rb, ob.id, ob.alias_digest, std::iter::empty())
        .unwrap()
    else {
        panic!()
    };
    let signal = server.dir.path().join("checkout-started");
    let release = server.dir.path().join("checkout-release");
    let hook = directory.join(".git/hooks/post-checkout");
    std::fs::write(&hook,format!("#!/bin/sh\n: > '{}'\ni=0\nwhile [ ! -e '{}' ] && [ $i -lt 1000 ]; do i=$((i+1)); sleep 0.01; done\n",signal.display(),release.display())).unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
    struct Executing {
        release: std::path::PathBuf,
        thread: Option<std::thread::JoinHandle<Result<OperationReceipt, CoreError>>>,
    }
    impl Drop for Executing {
        fn drop(&mut self) {
            let _ = std::fs::write(&self.release, b"release");
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }
    let executing = remote.clone();
    let original = ra.clone();
    let mut child = Executing {
        release: release.clone(),
        thread: Some(std::thread::spawn(move || {
            executing.execute_worktree(original, pa, oa.alias_digest, std::iter::empty())
        })),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !signal.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        signal.exists(),
        "owned post-checkout hook did not reach accepted command"
    );
    let record_path = server
        .dir
        .path()
        .join("state/files/operations")
        .join(format!("{}.json", "8f".repeat(16)));
    let accepted_bytes = std::fs::read(&record_path).unwrap();
    let observed = remote
        .execute_worktree(ra, [0; 16], [0; 32], std::iter::empty())
        .unwrap();
    assert!(
        matches!(observed.state, OperationState::Unknown(_)),
        "existing ledger inspect keeps in-flight acceptance unresolved: {observed:?}"
    );
    assert_eq!(std::fs::read(record_path).unwrap(), accepted_bytes);
    let save = second
        .prepare_owned_save(SaveRequest {
            operation: OperationID([145; 16]),
            file: snapshot.file,
            expected_version: Some(snapshot.version),
            submitted_revision: BufferRevision(11),
            source: ByteSource::Memory(Arc::from(b"linked owner save\n".as_slice())),
        })
        .unwrap();
    let approval = save.approval().clone();
    assert!(matches!(save.execute().state, OperationState::Confirmed(_)));
    remote
        .release_workspace(LeaseReleaseIntent::ViewDetachment)
        .unwrap();
    std::fs::write(&release, b"release").unwrap();
    let first = child.thread.take().unwrap().join().unwrap().unwrap();
    assert!(
        matches!(first.state, OperationState::Confirmed(_)),
        "postacceptance detach revoked accepted cursor: {first:?}"
    );
    let second_receipt = second
        .execute_worktree(rb, pb, ob.alias_digest, std::iter::once(&approval))
        .unwrap();
    assert!(
        matches!(second_receipt.state, OperationState::Confirmed(_)),
        "{second_receipt:?}"
    );
    second.release_observation(ob.id).unwrap();
    crate::worker::run(Box::pin(second.close_connection()));
}

#[test]
fn original_lookup_ready_requires_the_owned_transfer_identity() {
    let (_dir, mut remote, thread) = scripted_peer("proof-lookup", "wrong_stream");
    Arc::make_mut(&mut remote.transport.capabilities).push("git.worktree_proof".into());
    let original = GitMutationRequest {
        operation: OperationID([151; 16]),
        worktree: WorktreeIdentity {
            target: remote.target.clone(),
            canonical_path: TargetPath::posix(b"/fixture".to_vec()).unwrap(),
            common_git_directory_identity: vec![],
        },
        expected: GitExpectedState {
            head: GitHead::Unborn(b"main".to_vec()),
            index_fingerprint: [0; 32],
            worktree_fingerprint: [0; 32],
        },
        command: GitMutation::Checkout {
            reference: b"other".to_vec(),
        },
    };
    assert_eq!(
        remote
            .prepare_worktree(&original, [0; 16], [0; 32], std::iter::empty())
            .unwrap_err()
            .code,
        ErrorCode::InvalidArgument
    );
    let messages = thread.join().unwrap();
    assert_eq!(messages.len(), 1);
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
#[ignore = "requires the preserved historical helper artifact via BUNTRAIL_OLD_HELPER_BINARY"]
fn actual_b2b2_helper_refuses_original_lookup_and_keeps_save_alive() {
    let binary = std::path::PathBuf::from(
        std::env::var_os("BUNTRAIL_OLD_HELPER_BINARY")
            .expect("provide the preserved historical helper artifact"),
    );
    assert_eq!(
        w::hex(&sha2::Sha256::digest(std::fs::read(&binary).unwrap())),
        "fd3ce1e3e04fd30ae05cfcc5a4a72c76ff98e116ec60e9321e3bae36ec98e381"
    );
    let (server, remote, root) = fixture_server(support::Server::start_binary(&binary));
    assert!(
        !remote
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "git.original_lookup")
    );
    let ids = remote.transport.ids.load(Ordering::SeqCst);
    let worktree = WorktreeIdentity {
        target: remote.target.clone(),
        canonical_path: root.path.clone(),
        common_git_directory_identity: vec![],
    };
    assert!(
        remote
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "git.worktree_proof")
    );
    let original = GitMutationRequest {
        operation: OperationID([152; 16]),
        worktree,
        expected: GitExpectedState {
            head: GitHead::Unborn(b"main".to_vec()),
            index_fingerprint: [0; 32],
            worktree_fingerprint: [0; 32],
        },
        command: GitMutation::Checkout {
            reference: vec![b'x'; 65536],
        },
    };
    assert_eq!(
        remote.existing_original(&original).unwrap_err().code,
        ErrorCode::Unavailable
    );
    assert_eq!(
        remote.transport.ids.load(Ordering::SeqCst),
        ids,
        "old helper refusal must allocate no request or send proof bytes"
    );
    let pending_before = remote.transport.requests.load(Ordering::SeqCst);
    assert_eq!(
        run(GitService::existing_receipt(&remote, original))
            .unwrap_err()
            .code,
        ErrorCode::Unavailable
    );
    assert_eq!(
        remote.transport.requests.load(Ordering::SeqCst),
        pending_before,
        "oversize legacy original must not be sent"
    );
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    let file = FileRef {
        path: TargetPath::posix(
            server
                .dir
                .path()
                .join("project/file")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        ..root
    };
    let snapshot = run(FileService::read(&remote, file)).unwrap();
    let save = remote
        .prepare_owned_save(SaveRequest {
            operation: OperationID([127; 16]),
            file: snapshot.file,
            expected_version: Some(snapshot.version),
            submitted_revision: BufferRevision(10),
            source: ByteSource::Memory(Arc::from(b"old helper ordinary save\n".as_slice())),
        })
        .unwrap();
    let approval = save.approval().clone();
    assert!(matches!(save.execute().state, OperationState::Confirmed(_)));
    assert_eq!(approval.actor, *remote.lease());
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    assert_eq!(
        std::fs::read(server.dir.path().join("project/file")).unwrap(),
        b"old helper ordinary save\n"
    );
    crate::worker::run(Box::pin(remote.close_connection()));
}

#[test]
fn actual_helper_existing_only_streams_all_large_originals_without_replay() {
    use buntrail_services::operations::ledger::Ledger;
    let (server, remote, root) = fixture();
    let directory = server.dir.path().join("project");
    assert!(
        std::process::Command::new("git")
            .current_dir(&directory)
            .args(["init", "-b", "main"])
            .output()
            .unwrap()
            .status
            .success()
    );
    let worktree = run(remote.discover(root)).unwrap().unwrap();
    let status = run(remote.status(worktree.clone())).unwrap();
    let mut original = GitMutationRequest {
        operation: OperationID([153; 16]),
        worktree,
        expected: status.state,
        command: GitMutation::Stage(vec![status.untracked[0].path.clone()]),
    };
    let seed = original.operation;
    assert!(matches!(
        run(GitService::mutate(&remote, original.clone())).state,
        OperationState::Confirmed(_)
    ));
    let ledger = Ledger::open(server.dir.path().join("state/files/operations")).unwrap();
    let commands = [
        GitMutation::Stage(vec![]),
        GitMutation::Unstage(vec![]),
        GitMutation::Commit {
            message: "test".into(),
        },
        GitMutation::Checkout {
            reference: vec![b'x'; 65536],
        },
        GitMutation::Fetch {
            remote: b"origin".to_vec(),
        },
        GitMutation::Push {
            remote: b"origin".to_vec(),
            reference: b"main".to_vec(),
        },
        GitMutation::PullFastForward {
            remote: b"origin".to_vec(),
            reference: b"main".to_vec(),
        },
    ];
    for (index, command) in commands.into_iter().enumerate() {
        original.operation = OperationID([154 + index as u8; 16]);
        original.command = command;
        original.expected.head = GitHead::Unborn(vec![b'x'; 65536]);
        assert!(serde_json::to_vec(&original).unwrap().len() > 65536);
        assert_eq!(remote.existing_original(&original).unwrap(), None);
        assert!(ledger.read(original.operation).unwrap().is_none());
        // Synthetic interrupted history; no Git network command is executed by this test.
        let mut record = ledger.read(seed).unwrap().unwrap();
        record.envelope.id = original.operation;
        record.envelope.request_digest =
            sha2::Sha256::digest(serde_json::to_vec(&original).unwrap()).into();
        record.receipt.id = original.operation;
        record.receipt.state = OperationState::Running;
        {
            let _lock = ledger.lock().unwrap();
            ledger.write(&record).unwrap();
        }
        let path = ledger
            .directory
            .join(format!("{}.json", w::hex(&original.operation.0)));
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            remote.existing_original(&original).unwrap().unwrap().state,
            OperationState::Unknown(_)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        original.expected.index_fingerprint[0] ^= 1;
        assert_eq!(
            remote.existing_original(&original).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    assert_eq!(std::fs::read(directory.join("file")).unwrap(), b"needle\n");
    assert_eq!(remote.connection_state(), ConnectionState::Ready);
    crate::worker::run(Box::pin(remote.close_connection()));
}
