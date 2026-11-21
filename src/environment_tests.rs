use super::*;
use crate::remote_test_support as support;
use std::time::Instant;
fn until(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < end, "fixture timed out");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn locator() -> EnvironmentLocator {
    EnvironmentLocator {
        route: RouteKind::Ssh,
        ssh_profile: Some("internal-owned-unix".into()),
        engine: None,
        engine_context: None,
        container_selector: None,
        effective_user: None,
    }
}
pub(crate) struct Fixture {
    _server: support::Server,
    pub(crate) connector: EnvironmentConnector,
    attempt: Option<Arc<Attempt>>,
    anchor: Arc<RemoteServices>,
    retirement_started: Arc<AtomicBool>,
    retirement_allowed: Arc<AtomicBool>,
}
impl Fixture {
    fn new() -> Self {
        Self::using_local(None)
    }
    pub(crate) fn using_local(existing: Option<Arc<LocalConnection>>) -> Self {
        Self::using_transport(existing, false)
    }
    fn using_transport(existing: Option<Arc<LocalConnection>>, live_ssh: bool) -> Self {
        Self::using_endpoint(existing, live_ssh, |endpoint| endpoint)
    }
    fn using_endpoint(
        existing: Option<Arc<LocalConnection>>,
        live_ssh: bool,
        wrap: impl FnOnce(std::os::unix::net::UnixStream) -> std::os::unix::net::UnixStream,
    ) -> Self {
        // Real Darwin Unix helper/protocol; this substitutes only physical SSH retirement.
        let server = support::Server::start();
        let local = existing.unwrap_or_else(|| {
            Arc::new(LocalConnection::open(server.dir.path().join("local")).unwrap())
        });
        let connector = EnvironmentConnector::new(
            local.clone(),
            SshConfiguration {
                askpass: PathBuf::from("/unused-internal-fixture"),
                config: None,
                packages: vec![],
                bundle: None,
                spool_directory: server.dir.path().join("spools"),
            },
        )
        .unwrap();
        let attempt_cancel = Arc::new(AtomicBool::new(false));
        let channel: Option<buntrail_services::connections::ssh::SshChannel> = if live_ssh {
            #[cfg(feature = "test-support")]
            {
                std::fs::write(
                    std::env::var_os("BUNTRAIL_SSH_FIXTURE_HELPER_SOCKET_FILE").unwrap(),
                    format!("{}\n", server.socket.display()),
                )
                .unwrap();
                Some(
                    SshConnector::new(
                        SshProfile {
                            name: "bridge".into(),
                            config: Some(PathBuf::from(
                                std::env::var_os("BUNTRAIL_SSH_FIXTURE_CONFIG").unwrap(),
                            )),
                        },
                        None,
                        attempt_cancel.clone(),
                    )
                    .unwrap()
                    .fixture_byte_channel()
                    .unwrap(),
                )
            }
            #[cfg(not(feature = "test-support"))]
            {
                panic!("explicit test-support feature required")
            }
        } else {
            None
        };
        let endpoint = channel
            .as_ref()
            .map(|c| c.stream().unwrap())
            .unwrap_or_else(|| server.connect());
        let mut remote = RemoteServices::connect(
            wrap(endpoint),
            local.identity.clone(),
            WorkspaceLease {
                client: local.client,
                workspace: WorkspaceID([81; 16]),
                lease: WorkspaceLeaseID([82; 16]),
                runtime: RuntimeGeneration {
                    supervisor_boot: [0; 16],
                    container_start: None,
                },
            },
            server.dir.path().join("spools"),
        )
        .unwrap();
        let retirement_started = Arc::new(AtomicBool::new(false));
        let retirement_allowed = Arc::new(AtomicBool::new(true));
        let start = retirement_started.clone();
        let allow = retirement_allowed.clone();
        if let Some(channel) = channel {
            remote.bind_physical(channel).unwrap();
        } else {
            remote
                .bind_retirement(Box::new(move || {
                    Box::pin(async move {
                        start.store(true, Ordering::SeqCst);
                        until(|| allow.load(Ordering::SeqCst));
                    })
                }))
                .unwrap();
        }
        remote.set_authentication(None).unwrap();
        remote.validate_environment_ready().unwrap();
        remote
            .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
            .unwrap();
        remote.physical().unwrap().ready();
        let anchor = Arc::new(remote);
        let attempt = Arc::new(Attempt {
            welcome: false,
            parent_target: None,
            id: 41,
            prompt: Mutex::new(None),
            expected: None,
            selected_container: None,
            state: State::new(ConnectionState::Ready),
            broker: Mutex::new(None),
            cancel: attempt_cancel,
            result: Mutex::new(Completion {
                value: Some(Ok(anchor.clone())),
                waker: None,
            }),
            dependents: AtomicUsize::new(0),
            reservation_released: AtomicBool::new(false),
        });
        connector.state.lock().unwrap().entries.push(PoolEntry {
            locator: locator(),
            attempt: Arc::downgrade(&attempt),
            route: Arc::downgrade(&anchor),
        });
        Self {
            _server: server,
            connector,
            attempt: Some(attempt),
            anchor,
            retirement_started,
            retirement_allowed,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.retirement_allowed.store(true, Ordering::SeqCst);
        self.connector.shutdown();
        until(|| self.connector.shutdown_ready());
    }
}
#[test]
fn cancelled_completed_waiter_does_not_publish_ready_or_close_sibling() {
    let mut fixture = Fixture::new();
    let first = fixture.connector.start(locator(), None, None).unwrap();
    let second = fixture.connector.start(locator(), None, None).unwrap();
    assert_eq!(first.attempt_id(), second.attempt_id());
    fixture.attempt.take();
    until(|| {
        first
            .registration
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .value
            .is_some()
    });
    first.cancel();
    assert!(
        matches!(
            run(Box::pin(first)),
            Err(CoreError {
                code: ErrorCode::Cancelled,
                ..
            })
        ),
        "cancelled completed waiter published readiness"
    );
    let second = run(Box::pin(second)).unwrap();
    second.services.validate_environment_ready().unwrap();
    assert!(!fixture.retirement_started.load(Ordering::SeqCst));
    let env = second.environment();
    run(env.owner.close(ConnectionCloseReason::ResolvedFinalClose)).unwrap();
    until(|| fixture.retirement_started.load(Ordering::SeqCst));
}
#[test]
fn cancelled_welcome_home_releases_its_view_and_preserves_a_sibling() {
    use buntrail_protocol::{
        services::Call,
        types::{Header, Message},
    };
    use std::{
        io::{Read, Write},
        os::unix::net::UnixStream,
        sync::mpsc,
    };

    let (blocked, is_blocked) = mpsc::channel();
    let (resume, allow) = mpsc::channel();
    let fixture = Fixture::using_endpoint(None, false, |mut helper| {
        let (application, mut proxy) = UnixStream::pair().unwrap();
        let (mut from_helper, mut to_application) =
            (helper.try_clone().unwrap(), proxy.try_clone().unwrap());
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut from_helper, &mut to_application);
        });
        std::thread::spawn(move || {
            let mut owner = None;
            let mut sizes = [0; 8];
            while proxy.read_exact(&mut sizes).is_ok() {
                let length = u32::from_be_bytes(sizes[..4].try_into().unwrap()) as usize;
                let payload = u32::from_be_bytes(sizes[4..].try_into().unwrap()) as usize;
                let mut frame = vec![0; length + payload];
                proxy.read_exact(&mut frame).unwrap();
                let header: Header = serde_json::from_slice(&frame[..length]).unwrap();
                match header.message {
                    Message::Register {
                        workspace_id,
                        lease_id,
                        ..
                    } => {
                        owner = Some((workspace_id, lease_id));
                    }
                    Message::ServiceCall {
                        call: Call::WelcomeHome {},
                    } => {
                        blocked.send(owner.take().unwrap()).unwrap();
                        allow.recv_timeout(Duration::from_secs(5)).unwrap();
                    }
                    _ => {}
                }
                if helper
                    .write_all(&sizes)
                    .and_then(|_| helper.write_all(&frame))
                    .is_err()
                {
                    break;
                }
            }
            let _ = helper.shutdown(std::net::Shutdown::Both);
        });
        application
    });
    let sibling = run(Box::pin(
        fixture.connector.start(locator(), None, None).unwrap(),
    ))
    .unwrap();
    let welcome = fixture
        .connector
        .start_welcome(locator(), None, None)
        .unwrap();
    let (workspace, lease) = is_blocked.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut owner = sibling.services.lease().clone();
    owner.workspace = WorkspaceID(buntrail_protocol::services::id(&workspace).unwrap());
    owner.lease = WorkspaceLeaseID(buntrail_protocol::services::id(&lease).unwrap());
    assert_ne!(sibling.services.lease(), &owner);
    assert_ne!(welcome.state(), ConnectionState::Ready);
    welcome.cancel();
    resume.send(()).unwrap();
    assert!(matches!(run(Box::pin(welcome)), Err(error) if error.code == ErrorCode::Cancelled));
    assert_eq!(
        sibling.services.observe_owner(owner).unwrap().presence,
        LeasePresence::Absent
    );
    sibling.services.validate_environment_ready().unwrap();
}

#[test]
fn last_view_close_waits_for_physical_retirement_and_preserves_siblings() {
    let mut fixture = Fixture::new();
    fixture.retirement_allowed.store(false, Ordering::SeqCst);
    let a = fixture.connector.start(locator(), None, None).unwrap();
    let b = fixture.connector.start(locator(), None, None).unwrap();
    fixture.attempt.take();
    let a = run(Box::pin(a)).unwrap();
    let b = run(Box::pin(b)).unwrap();
    assert!(a.services.shares_connection(&b.services));
    assert_ne!(a.services.lease(), b.services.lease());
    run(a
        .environment()
        .owner
        .close(ConnectionCloseReason::ResolvedFinalClose))
    .unwrap();
    b.services.validate_environment_ready().unwrap();
    assert!(!fixture.retirement_started.load(Ordering::SeqCst));
    let (tx, rx) = std::sync::mpsc::channel();
    let environment = b.environment();
    let closer = std::thread::spawn(move || {
        tx.send(run(environment
            .owner
            .close(ConnectionCloseReason::ResolvedFinalClose)))
            .unwrap();
    });
    until(|| fixture.retirement_started.load(Ordering::SeqCst));
    assert!(
        rx.try_recv().is_err(),
        "close completed before physical retirement"
    );
    fixture.retirement_allowed.store(true, Ordering::SeqCst);
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    closer.join().unwrap();
    assert!(fixture.anchor.physical().unwrap().retired());
}
#[test]
fn final_close_waits_for_retained_stream_before_physical_retirement() {
    let mut fixture = Fixture::new();
    let request = fixture.connector.start(locator(), None, None).unwrap();
    fixture.attempt.take();
    let environment = run(Box::pin(request)).unwrap();
    let watch = run(environment.services.watch(FileRef {
        target: environment.services.target().clone(),
        path: TargetPath::posix(
            fixture
                ._server
                .dir
                .path()
                .as_os_str()
                .as_encoded_bytes()
                .to_vec(),
        )
        .unwrap(),
        resolved_identity: None,
    }))
    .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let generic = environment.environment();
    let thread = std::thread::spawn(move || {
        tx.send(run(generic
            .owner
            .close(ConnectionCloseReason::ResolvedFinalClose)))
            .unwrap();
    });
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        rx.try_recv().is_err(),
        "last close completed while a stream still owned the physical route"
    );
    assert!(!fixture.retirement_started.load(Ordering::SeqCst));
    drop(watch);
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    thread.join().unwrap();
    assert!(fixture.anchor.physical().unwrap().retired());
}
#[test]
fn dependent_does_not_publish_environment_ready_before_its_registration() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt.as_ref().unwrap();
    let result = attempt.result.lock().unwrap().value.take().unwrap();
    let request = fixture.connector.start(locator(), None, None).unwrap();
    let observed = request.state();
    attempt.result.lock().unwrap().complete(result);
    request.cancel();
    assert_ne!(
        observed,
        ConnectionState::Ready,
        "physical readiness escaped before dependent registration"
    );
    drop(request);
}
#[test]
fn distinct_profile_cannot_replace_live_or_inflight_target_auth_policy() {
    let fixture = Fixture::new();
    let existing = fixture.attempt.as_ref().unwrap();
    let target = fixture.anchor.target().clone();
    let mut other = locator();
    other.ssh_profile = Some("another-saved-policy".into());
    let candidate = Arc::new(Attempt {
        welcome: false,
        parent_target: None,
        id: 99,
        prompt: Mutex::new(None),
        expected: None,
        selected_container: None,
        state: State::new(ConnectionState::Resolving),
        broker: Mutex::new(None),
        cancel: Arc::new(AtomicBool::new(false)),
        result: Mutex::new(Completion::new()),
        dependents: AtomicUsize::new(1),
        reservation_released: AtomicBool::new(false),
    });
    let mut state = fixture.connector.state.lock().unwrap();
    state.retirements.push(PhysicalRecord {
        target: target.clone(),
        locator: locator(),
        done: fixture.anchor.physical().unwrap().done.clone(),
    });
    let result = claim_target(&mut state, &candidate, &target, &other);
    drop(state);
    candidate.cancel.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture.anchor.connection_state(),
        ConnectionState::Ready,
        "candidate cancelled existing profile"
    );
    assert_eq!(result.unwrap_err().code, ErrorCode::Busy);
    let mut state = ConnectorState {
        entries: vec![],
        retirements: vec![],
        claims: vec![],
        queries: vec![],
        closed: false,
    };
    claim_target(&mut state, existing, &target, &locator()).unwrap();
    assert_eq!(
        claim_target(&mut state, &candidate, &target, &other)
            .unwrap_err()
            .code,
        ErrorCode::Busy
    );
}
#[test]
fn completed_connect_handle_does_not_pin_physical_attempt_after_last_view_closes() {
    let mut fixture = Fixture::new();
    let mut request = fixture.connector.start(locator(), None, None).unwrap();
    fixture.attempt.take();
    let environment = run(Box::pin(&mut request)).unwrap();
    run(environment
        .environment()
        .owner
        .close(ConnectionCloseReason::ResolvedFinalClose))
    .unwrap();
    assert!(
        environment.services.physical().unwrap().retired(),
        "completed request handle pinned the connection attempt"
    );
    assert_eq!(request.state(), ConnectionState::Ready);
}
#[test]
fn connector_admission_is_shared_across_profiles_and_returns_after_cancellation() {
    let fixture = Fixture::new();
    let attempt = fixture.attempt.as_ref().unwrap();
    let result = attempt.result.lock().unwrap().value.take().unwrap();
    let requests: Vec<_> = (0..100)
        .map(|_| fixture.connector.start(locator(), None, None).unwrap())
        .collect();
    let mut other = locator();
    other.ssh_profile = Some("different-pending-profile".into());
    let overflow = fixture.connector.start(other, None, None);
    for request in requests {
        request.cancel();
    }
    attempt.result.lock().unwrap().complete(result);
    assert!(
        matches!(
            overflow,
            Err(CoreError {
                code: ErrorCode::Busy,
                ..
            })
        ),
        "another profile bypassed the shared live-dependent bound"
    );
}

#[test]
fn welcome_upgrade_retries_once_and_respects_cancellation() {
    for (cancel_during_wait, second_miss) in [(false, false), (true, false), (false, true)] {
        let cancel = AtomicBool::new(false);
        let mut opens = 0;
        let mut waits = 0;
        let grace = Duration::from_millis(if cancel_during_wait { 250 } else { 5 });
        let start = Instant::now();
        let result = std::thread::scope(|scope| {
            with_helper_upgrade(
                &cancel,
                grace,
                || {
                    waits += 1;
                    if cancel_during_wait {
                        scope.spawn(|| {
                            std::thread::sleep(Duration::from_millis(5));
                            cancel.store(true, Ordering::SeqCst);
                        });
                    }
                },
                || {
                    opens += 1;
                    if opens == 1 || second_miss {
                        Err(ConnectError::OlderHelper)
                    } else {
                        assert!(start.elapsed() >= grace);
                        Ok(())
                    }
                },
            )
        });
        assert_eq!(waits, 1);
        assert_eq!(opens, if cancel_during_wait { 1 } else { 2 });
        match (cancel_during_wait, second_miss) {
            (true, _) => assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled),
            (false, true) => assert_eq!(
                result.unwrap_err().message,
                "The target is still running an older Buntrail helper. Finish any open Buntrail work on the target, then reconnect."
            ),
            _ => result.unwrap(),
        }
    }
}
