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
struct Fixture {
    _server: support::Server,
    connector: EnvironmentConnector,
    attempt: Option<Arc<Attempt>>,
    anchor: Arc<RemoteServices>,
    retirement_started: Arc<AtomicBool>,
    retirement_allowed: Arc<AtomicBool>,
}
impl Fixture {
    fn new() -> Self {
        Self::using_local(None)
    }
    fn using_local(existing: Option<Arc<LocalConnection>>) -> Self {
        Self::using_transport(existing, false)
    }
    fn using_transport(existing: Option<Arc<LocalConnection>>, live_ssh: bool) -> Self {
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
            endpoint,
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
