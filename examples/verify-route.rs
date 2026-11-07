//! Owned-fixture smoke check over an explicitly supplied SSH/container byte bridge.
#[cfg(not(unix))]
fn main() {
    eprintln!("This route fixture requires a Unix client");
    std::process::exit(2);
}
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use buntrail_core::{Text, contracts::*};
    use buntrail_engine::remote::RemoteServices;
    use buntrail_services::{connections::local::new_id, contracts::*, files};
    use serde::Deserialize;
    use std::{
        future::Future,
        net::Shutdown,
        os::unix::net::UnixStream,
        pin::Pin,
        process::{Command, Stdio},
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
        thread,
    };
    fn run<T>(mut future: Pin<Box<dyn Future<Output = T> + Send + '_>>) -> T {
        struct Unpark(thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let started = std::time::Instant::now();
        loop {
            assert!(
                started.elapsed().as_secs() < 120,
                "route fixture future timed out"
            );
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(value) => return value,
                Poll::Pending => thread::park_timeout(std::time::Duration::from_millis(20)),
            }
        }
    }
    #[derive(Deserialize)]
    struct Identity {
        target: TargetIdentity,
        container_start: Option<Vec<u8>>,
    }
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        return Err(
            "usage: verify-route IDENTITY_JSON OWNED_REMOTE_ROOT BRIDGE_COMMAND [ARG...]".into(),
        );
    }
    let identity: Identity = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let root = &args[1];
    if !root.starts_with("/tmp/buntrail-consolidation.")
        || !root
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/.-_".contains(&b))
    {
        return Err("An owned temporary remote root is required".into());
    }
    let mut child = Command::new(&args[2])
        .args(&args[3..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let (client, bridge) = UnixStream::pair()?;
    let mut reader = bridge.try_clone()?;
    let mut writer = bridge.try_clone()?;
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    let send = thread::spawn(move || {
        let _ = std::io::copy(&mut reader, &mut input);
    });
    let receive = thread::spawn(move || {
        let _ = std::io::copy(&mut output, &mut writer);
        let _ = writer.shutdown(Shutdown::Write);
    });
    let temporary = tempfile::tempdir()?;
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let remote = RemoteServices::connect(
            client,
            identity.target.clone(),
            WorkspaceLease {
                client: ClientID(new_id()?),
                workspace: WorkspaceID(new_id()?),
                lease: WorkspaceLeaseID(new_id()?),
                runtime: RuntimeGeneration {
                    supervisor_boot: [0; 16],
                    container_start: identity.container_start,
                },
            },
            temporary.path().join("spools"),
        )?;
        remote.set_authentication(None)?;
        remote.validate_environment_ready()?;
        let file = FileRef {
            target: identity.target,
            path: TargetPath::posix(format!("{root}/route-file.txt").into_bytes())?,
            resolved_identity: None,
        };
        let text = "Buntrail remote 🐇\r\n".repeat(500_000);
        let original = DocumentSnapshot {
            document: DocumentID(new_id()?),
            revision: BufferRevision(0),
            text: Arc::new(Text::from(text.clone())),
            encoding: Encoding::Utf8 { bom: true },
            line_endings: LineEndings::CrLf,
            language: None,
        };
        let bytes = files::encode(&original)?;
        assert!(bytes.len() > 8 * 1024 * 1024);
        let created = run(FileService::mutate(
            &remote,
            FileMutationRequest {
                operation: OperationID(new_id()?),
                file: file.clone(),
                expected: ExpectedPathState::Absent,
                command: FileMutation::Create {
                    source: ByteSource::Memory(bytes.clone().into()),
                },
            },
        ));
        assert!(
            matches!(
                created.state,
                OperationState::Confirmed(MutationResult::FileChanged { .. })
            ),
            "{created:?}"
        );
        let read = run(remote.read(file.clone()))?;
        let (loaded, _budget) = remote.load_snapshot(read, original.document, None)?;
        assert_eq!(loaded.text.as_str(), text);
        assert_eq!(loaded.encoding, original.encoding);
        let operation = OperationID(new_id()?);
        let saved = run(remote.save(SaveRequest {
            operation,
            file: file.clone(),
            expected_version: Some(loaded.version),
            submitted_revision: BufferRevision(7),
            source: ByteSource::Memory(bytes.clone().into()),
        }));
        assert!(
            matches!(
                saved.state,
                OperationState::Confirmed(MutationResult::Saved(_))
            ),
            "{saved:?}"
        );
        let inspected = run(remote.inspect(operation, file.target.clone()))?;
        assert_eq!(inspected, saved);
        let directory = FileRef {
            target: file.target.clone(),
            path: TargetPath::posix(format!("{root}/repo").into_bytes())?,
            resolved_identity: None,
        };
        let session = TerminalSessionID(new_id()?);
        let opened = run(TerminalService::open(
            &remote,
            TerminalOpenRequest {
                operation: OperationID(new_id()?),
                session,
                owner: remote.lease().clone(),
                target: file.target.clone(),
                runtime: remote.runtime(),
                directory: TargetPath::posix(root.as_bytes().to_vec())?,
                shell: TargetPath::posix(b"/bin/sh".to_vec())?,
                size: TerminalSize {
                    columns: 80.try_into()?,
                    rows: 24.try_into()?,
                },
            },
        ));
        assert!(
            matches!(opened.state, OperationState::Confirmed(_)),
            "{opened:?}"
        );
        let attach = TerminalAttachRequest {
            session,
            owner: remote.lease().clone(),
            runtime: remote.runtime(),
            last_output_sequence: 0,
        };
        let mut events = run(remote.attach(attach.clone()))?;
        run(remote.resize(
            session,
            TerminalSize {
                columns: 100.try_into()?,
                rows: 30.try_into()?,
            },
        ))?;
        run(remote.input(session, b"mkdir repo && cd repo && git init -q && git config user.name Fixture && git config user.email fixture@example.invalid && printf 'live-needle\\n' > fixture.rs && printf '\\nBUNTRAIL_LIVE_READY\\n'; exit\n".to_vec()))?;
        let mut output = Vec::new();
        let mut sequence = 0;
        loop {
            match run(Box::pin(std::future::poll_fn(|cx| {
                events.as_mut().poll_next(cx)
            })))
            .ok_or("terminal stream ended before exit")??
            {
                TerminalEvent::Output { sequence: n, bytes } => {
                    assert!(n > sequence);
                    sequence = n;
                    output.extend(bytes);
                }
                TerminalEvent::ProcessExit { .. } => break,
                TerminalEvent::Gap { .. } | TerminalEvent::RuntimeLost | TerminalEvent::Expired => {
                    panic!("unexpected terminal history loss")
                }
                _ => {}
            }
        }
        assert!(
            String::from_utf8_lossy(&output).contains("\r\nBUNTRAIL_LIVE_READY\r\n"),
            "{}",
            String::from_utf8_lossy(&output)
        );
        drop(events);
        let mut replay = run(remote.attach(attach))?;
        let mut replayed = Vec::new();
        loop {
            match run(Box::pin(std::future::poll_fn(|cx| {
                replay.as_mut().poll_next(cx)
            })))
            .ok_or("replay ended before exit")??
            {
                TerminalEvent::Output { bytes, .. } => replayed.extend(bytes),
                TerminalEvent::ProcessExit { .. } => break,
                TerminalEvent::Gap { .. } | TerminalEvent::RuntimeLost | TerminalEvent::Expired => {
                    panic!("unexpected replay loss")
                }
                _ => {}
            }
        }
        assert_eq!(replayed, output);
        drop(replay);
        run(remote.end(session, TerminalEndIntent::ExplicitClose))?;
        let mut search = run(remote.search(SearchRequest {
            request: RequestId(1),
            root: directory.clone(),
            expression: "live-needle".into(),
            mode: SearchMode::Literal,
            case_sensitive: true,
            whole_word: false,
            includes: vec!["*.rs".into()],
            excludes: vec![],
            hidden: false,
            ignored: false,
        }))?;
        let mut matches = 0;
        let mut complete = false;
        while let Some(event) = run(Box::pin(std::future::poll_fn(|cx| {
            search.as_mut().poll_next(cx)
        }))) {
            match event? {
                SearchEvent::Match(found) => {
                    assert_eq!(found.file.target, file.target);
                    assert_eq!(found.line, 1);
                    matches += 1;
                }
                SearchEvent::Complete => complete = true,
                SearchEvent::Progress { .. } => {}
                other => panic!("unexpected search outcome: {other:?}"),
            }
        }
        assert!(complete && matches == 1);
        let worktree = run(remote.discover(directory))?.ok_or("fixture repository missing")?;
        for command in [
            GitMutation::Stage(vec![TargetPath::posix(
                format!("{root}/repo/fixture.rs").into_bytes(),
            )?]),
            GitMutation::Commit {
                message: "live route fixture".into(),
            },
        ] {
            let expected = run(remote.status(worktree.clone()))?.state;
            let operation = OperationID(new_id()?);
            let receipt = run(GitService::mutate(
                &remote,
                GitMutationRequest {
                    operation,
                    worktree: worktree.clone(),
                    expected,
                    command,
                },
            ));
            assert!(
                matches!(receipt.state, OperationState::Confirmed(_)),
                "{receipt:?}"
            );
            assert_eq!(
                run(remote.inspect(operation, file.target.clone()))?,
                receipt
            );
        }
        let history = run(remote.history(HistoryQuery {
            worktree: worktree.clone(),
            ref_filter: vec![],
            cursor: None,
        }))?;
        assert_eq!(history.commits.len(), 1);
        assert_eq!(history.commits[0].subject, "live route fixture");
        assert!(
            run(remote.read_query(worktree, GitRead::StatusPorcelain))?
                .stdout
                .is_empty()
        );
        println!(
            "Verified framed create/read/save/inspect, {} bytes, BOM/CRLF; terminal input/resize/drain/replay; filtered streaming search; Git stage/commit/inspect/history; target/runtime binding",
            bytes.len()
        );
        use std::io::Write;
        std::io::stdout().flush()?;
        if let Ok(seconds) = std::env::var("BUNTRAIL_ROUTE_IDLE_SECONDS") {
            let seconds: u64 = seconds.parse()?;
            assert!(seconds <= 70);
            thread::sleep(std::time::Duration::from_secs(seconds));
        }
        remote.release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)?;
        run(Box::pin(remote.close_connection()));
        Ok(())
    })();
    let _ = bridge.shutdown(Shutdown::Both);
    let _ = child.kill();
    let _ = child.wait();
    let _ = send.join();
    let _ = receive.join();
    result
}
