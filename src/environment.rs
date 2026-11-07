//! Remote composition belongs in Engine; transports and process ownership remain in Services.
mod container_picker;
use crate::{
    remote::{Lifetime, RemoteServices},
    worker::run,
};
use buntrail_core::contracts::{
    async_types::{EventStream, ServiceFuture},
    *,
};
pub use buntrail_services::connections::authentication::{
    AuthenticationKind, AuthenticationRequest, AuthenticationResponse,
};
pub use buntrail_services::connections::bootstrap::BundledPackages;
pub use buntrail_services::connections::container_identity::ContainerState;
use buntrail_services::{
    connections::{
        authentication::AuthenticationBroker,
        bootstrap::{HelperBootstrapper, HelperPackage, Platform},
        container_engine::ContainerEngineClient,
        container_transport::{ContainerConnector, EngineHost},
        local::{LocalConnection, new_id},
        ssh::{SshConnector, SshProfile},
    },
    contracts::*,
};
use container_picker::SelectedContainer;
pub use container_picker::{ContainerInventory, ContainerQuery};
use futures_core::Stream;
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

const MAX_CONNECTIONS: usize = 100;
fn unavailable(message: &str) -> CoreError {
    CoreError::new(ErrorCode::Unavailable, message)
}
fn cancelled() -> CoreError {
    CoreError::new(ErrorCode::Cancelled, "connection request cancelled")
}
fn io(error: impl std::fmt::Display) -> CoreError {
    CoreError::new(ErrorCode::Io, &error.to_string())
}

struct State {
    value: ConnectionState,
    revision: u64,
    waiters: Vec<Waker>,
}
impl State {
    fn new(value: ConnectionState) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            value,
            revision: 1,
            waiters: vec![],
        }))
    }
    fn publish(shared: &Arc<Mutex<Self>>, value: ConnectionState) {
        let mut state = shared.lock().unwrap();
        if state.value == value {
            return;
        }
        state.value = value;
        state.revision += 1;
        for wake in state.waiters.drain(..) {
            wake.wake();
        }
    }
}
struct States {
    state: Arc<Mutex<State>>,
    revision: u64,
}
impl Stream for States {
    type Item = Result<ConnectionState, CoreError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let shared = self.state.clone();
        let mut state = shared.lock().unwrap();
        if state.revision != self.revision {
            self.revision = state.revision;
            return Poll::Ready(Some(Ok(state.value.clone())));
        }
        if matches!(
            state.value,
            ConnectionState::Disconnected
                | ConnectionState::Failed(_)
                | ConnectionState::TargetChanged { .. }
        ) {
            return Poll::Ready(None);
        }
        if !state.waiters.iter().any(|w| w.will_wake(cx.waker())) {
            if state.waiters.len() >= MAX_CONNECTIONS {
                return Poll::Ready(Some(Err(CoreError::new(
                    ErrorCode::Busy,
                    "connection state subscriber capacity reached",
                ))));
            }
            state.waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}
#[derive(Default)]
struct Done {
    finished: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}
impl Done {
    fn notify(&self) {
        for w in self.waiters.lock().unwrap().drain(..) {
            w.wake();
        }
    }
    fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
        self.notify();
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut waiters = self.waiters.lock().unwrap();
        if self.finished.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            if !waiters.iter().any(|w| w.will_wake(cx.waker())) && waiters.len() < MAX_CONNECTIONS {
                waiters.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}
struct Unused(Arc<PhysicalOwner>);
impl Future for Unused {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let polled = self.0.done.poll(cx);
        if self.0.lifetime.connected.load(Ordering::SeqCst)
            && (self.0.reservations.load(Ordering::SeqCst) != 0
                || self.0.lifetime.has_active_views())
        {
            Poll::Ready(())
        } else {
            polled
        }
    }
}
struct Retired(Arc<Done>);
impl Future for Retired {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.0.poll(cx)
    }
}

pub(crate) struct PhysicalOwner {
    admission: Arc<Mutex<()>>,
    lifetime: Arc<Lifetime>,
    reservations: Arc<AtomicUsize>,
    state: Arc<Mutex<State>>,
    done: Arc<Done>,
}
impl PhysicalOwner {
    pub(crate) fn start(
        lifetime: Lifetime,
        retire: Box<dyn FnOnce() -> ServiceFuture<'static, ()> + Send>,
    ) -> Result<Arc<Self>, CoreError> {
        let owner = Arc::new(Self {
            admission: Arc::new(Mutex::new(())),
            lifetime: Arc::new(lifetime),
            reservations: Arc::new(AtomicUsize::new(1)),
            state: State::new(ConnectionState::Negotiating),
            done: Arc::new(Done::default()),
        });
        let lifetime = owner.lifetime.clone();
        let reservations = owner.reservations.clone();
        let state = owner.state.clone();
        let done = owner.done.clone();
        let admission = owner.admission.clone();
        std::thread::Builder::new()
            .name("buntrail-connection-retirement".into())
            .spawn(move || {
                loop {
                    {
                        let _admission = admission.lock().unwrap();
                        if !lifetime.connected.load(Ordering::SeqCst)
                            || (reservations.load(Ordering::SeqCst) == 0 && lifetime.idle())
                        {
                            lifetime.connected.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                State::publish(&state, ConnectionState::Disconnected);
                run(Box::pin(lifetime.shutdown()));
                run(retire());
                done.finish();
            })
            .map_err(io)?;
        Ok(owner)
    }
    fn reserve(&self) -> Result<(), CoreError> {
        let _admission = self.admission.lock().unwrap();
        if !self.lifetime.connected.load(Ordering::SeqCst) {
            return Err(CoreError::new(
                ErrorCode::Closed,
                "physical connection is retiring; reconnect explicitly",
            ));
        }
        self.reservations.fetch_add(1, Ordering::SeqCst);
        self.done.notify();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn finish_fixture_startup(&self) {
        assert_eq!(self.reservations.fetch_sub(1, Ordering::SeqCst), 1);
        self.done.notify();
    }
    pub(crate) fn wait_unused(self: &Arc<Self>) -> ServiceFuture<'static, ()> {
        Box::pin(Unused(self.clone()))
    }
    fn ready(&self) {
        State::publish(&self.state, ConnectionState::Ready);
    }
    pub(crate) fn stop(&self) {
        self.lifetime.connected.store(false, Ordering::SeqCst);
    }
    pub(crate) fn shutdown(&self) -> ServiceFuture<'static, ()> {
        self.stop();
        Box::pin(Retired(self.done.clone()))
    }
    fn retired(&self) -> bool {
        self.done.finished.load(Ordering::SeqCst)
    }
}
impl Drop for PhysicalOwner {
    fn drop(&mut self) {
        self.stop();
    }
}
impl ConnectionOwner for PhysicalOwner {
    fn states(&self) -> EventStream<ConnectionState> {
        Box::pin(States {
            state: self.state.clone(),
            revision: 0,
        })
    }
    fn close(&self, reason: ConnectionCloseReason) -> ServiceFuture<'_, Result<(), CoreError>> {
        Box::pin(async move {
            if reason != ConnectionCloseReason::WindowDetachment {
                self.shutdown().await;
            }
            Ok(())
        })
    }
}

/// Immutable application-owned artifacts. Missing platform packages are unavailable, never substituted.
#[derive(Clone)]
pub struct SshConfiguration {
    pub askpass: PathBuf,
    pub packages: Vec<HelperPackage>,
    pub bundle: Option<Arc<BundledPackages>>,
    pub config: Option<PathBuf>,
    pub spool_directory: PathBuf,
}

impl SshConfiguration {
    /// Called only after verified platform selection on the existing connection worker.
    fn selected_package(
        &self,
        platform: Platform,
        cancel: &AtomicBool,
    ) -> Result<HelperPackage, CoreError> {
        if let Some(bundle) = &self.bundle {
            return bundle.load(platform, cancel);
        }
        self.packages.iter().find(|package| package.platform == platform).cloned().ok_or_else(|| unavailable("trusted Linux helper/search artifacts for the verified architecture and libc are unavailable in this application"))
    }
}

#[derive(Default)]
struct ConnectionBudget {
    normal: AtomicUsize,
    transition: AtomicUsize,
    resuming: Mutex<Vec<(ClientID, WorkspaceID, WorkspaceLeaseID)>>,
}
pub(crate) struct ConnectionPermit {
    budget: Arc<ConnectionBudget>,
    transition: bool,
}
impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let count = if self.transition {
            &self.budget.transition
        } else {
            &self.budget.normal
        };
        count.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ConnectionBudget {
    fn acquire(self: &Arc<Self>, transition: bool) -> Result<Arc<ConnectionPermit>, CoreError> {
        let (count, limit) = if transition {
            (&self.transition, 4)
        } else {
            (&self.normal, MAX_CONNECTIONS)
        };
        count.try_update(Ordering::SeqCst,Ordering::SeqCst,|n|(n<limit).then_some(n+1)).map_err(|_|CoreError::new(ErrorCode::Busy,"live connection admission is full; retire an existing handle or finish a transition"))?;
        Ok(Arc::new(ConnectionPermit {
            budget: self.clone(),
            transition,
        }))
    }
    fn resume(self: &Arc<Self>, owner: WorkspaceLease) -> Result<Arc<ResumeAdmission>, CoreError> {
        self.owner_admission(owner.client, owner.workspace, owner.lease)
    }
    fn owner_admission(
        self: &Arc<Self>,
        client: ClientID,
        workspace: WorkspaceID,
        lease: WorkspaceLeaseID,
    ) -> Result<Arc<ResumeAdmission>, CoreError> {
        let owner = (client, workspace, lease);
        let mut owners = self.resuming.lock().unwrap();
        if owners.contains(&owner) {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "this owner already has an explicit resume in progress",
            ));
        }
        owners.push(owner);
        Ok(Arc::new(ResumeAdmission {
            budget: self.clone(),
            owner,
        }))
    }
}
struct ResumeAdmission {
    budget: Arc<ConnectionBudget>,
    owner: (ClientID, WorkspaceID, WorkspaceLeaseID),
}
impl Drop for ResumeAdmission {
    fn drop(&mut self) {
        self.budget
            .resuming
            .lock()
            .unwrap()
            .retain(|o| o != &self.owner);
    }
}
struct PoolEntry {
    locator: EnvironmentLocator,
    attempt: Weak<Attempt>,
    route: Weak<RemoteServices>,
}
struct PhysicalRecord {
    target: TargetIdentity,
    locator: EnvironmentLocator,
    done: Arc<Done>,
}
struct TargetClaim {
    target: TargetIdentity,
    locator: EnvironmentLocator,
    attempt: Weak<Attempt>,
}
struct ConnectorState {
    entries: Vec<PoolEntry>,
    retirements: Vec<PhysicalRecord>,
    claims: Vec<TargetClaim>,
    queries: Vec<Weak<container_picker::QueryState>>,
    closed: bool,
}
#[derive(Clone)]
pub struct EnvironmentConnector {
    budget: Arc<ConnectionBudget>,
    handles: Arc<()>,
    local: Arc<LocalConnection>,
    configuration: SshConfiguration,
    state: Arc<Mutex<ConnectorState>>,
    workers: Arc<AtomicUsize>,
}
struct Completion<T> {
    value: Option<Result<T, CoreError>>,
    waker: Option<Waker>,
}
impl<T> Completion<T> {
    fn new() -> Self {
        Self {
            value: None,
            waker: None,
        }
    }
    fn complete(&mut self, result: Result<T, CoreError>) {
        self.value = Some(result);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }
}
struct PromptClaim {
    request: AuthenticationRequest,
    waiter: Option<u64>,
}
struct Attempt {
    id: u64,
    prompt: Mutex<Option<PromptClaim>>,
    expected: Option<TargetIdentity>,
    selected_container: Option<SelectedContainer>,
    state: Arc<Mutex<State>>,
    broker: Mutex<Option<Arc<AuthenticationBroker>>>,
    cancel: Arc<AtomicBool>,
    result: Mutex<Completion<Arc<RemoteServices>>>,
    dependents: AtomicUsize,
    reservation_released: AtomicBool,
}
impl Attempt {
    fn cancel_startup(&self) {
        // Once physical ownership is published, request retirement must not cancel its live channel.
        if !matches!(self.result.lock().unwrap().value, Some(Ok(_))) {
            self.cancel.store(true, Ordering::SeqCst);
            if let Some(broker) = self.broker.lock().unwrap().as_ref() {
                broker.cancel();
            }
        }
    }
    fn release_reservation(&self) {
        if let Some(Ok(route)) = &self.result.lock().unwrap().value
            && !self.reservation_released.swap(true, Ordering::SeqCst)
            && let Some(owner) = route.physical()
        {
            owner.reservations.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.release_reservation();
        self.cancel_startup();
    }
}
struct Worker(Arc<AtomicUsize>);
impl Drop for Worker {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn spawn(
    workers: &Arc<AtomicUsize>,
    work: impl FnOnce() + Send + 'static,
) -> Result<(), CoreError> {
    workers.fetch_add(1, Ordering::SeqCst);
    let guard = Worker(workers.clone());
    std::thread::Builder::new()
        .name("buntrail-environment".into())
        .spawn(move || {
            let _guard = guard;
            work();
        })
        .map(|_| ())
        .map_err(io)
}

/// Native consumers retain this handle; prompt IDs are meaningful only within `attempt`.
pub struct ConnectionPrompt {
    pub attempt: u64,
    pub request: AuthenticationRequest,
}
pub struct ConnectRequest {
    permit: Arc<ConnectionPermit>,
    transition: Option<Arc<ConnectionPermit>>,
    resume_admission: Option<Arc<ResumeAdmission>>,
    attached: AtomicBool,
    waiter: u64,
    attempt: Arc<Attempt>,
    cancel: Arc<AtomicBool>,
    registration: Option<Arc<Mutex<Completion<Arc<RemoteServices>>>>>,
    locator: EnvironmentLocator,
    local_state: Arc<Mutex<State>>,
    resume: Option<WorkspaceLease>,
    saved: Option<(WorkspaceID, WorkspaceLeaseID)>,
    workers: Arc<AtomicUsize>,
    finished: bool,
}
#[derive(Clone)]
pub struct RemoteEnvironment {
    pub locator: EnvironmentLocator,
    pub services: Arc<RemoteServices>,
    _workers: Arc<AtomicUsize>,
    _owner_admission: Option<Arc<ResumeAdmission>>,
}
impl RemoteEnvironment {
    #[cfg(feature = "test-support")]
    pub fn fixture(services: Arc<RemoteServices>, locator: EnvironmentLocator) -> Self {
        Self {
            locator,
            services,
            _workers: Arc::new(AtomicUsize::new(0)),
            _owner_admission: None,
        }
    }

    pub fn environment(&self) -> ConnectedEnvironment {
        let service = self.services.clone();
        ConnectedEnvironment {
            identity: service.target().clone(),
            runtime: service.runtime(),
            capabilities: Capabilities(vec![
                Capability::Files,
                Capability::Watch,
                Capability::Search,
                Capability::Git,
                Capability::Terminal,
                Capability::Operations,
                Capability::Leases,
            ]),
            owner: Arc::new(ViewOwner(service.clone())),
            files: ServiceAvailability::Available(service.clone()),
            search: ServiceAvailability::Available(service.clone()),
            git: ServiceAvailability::Available(service.clone()),
            terminal: ServiceAvailability::Available(service.clone()),
            operations: ServiceAvailability::Available(service.clone()),
            leases: ServiceAvailability::Available(service),
        }
    }
}
struct ViewOwner(Arc<RemoteServices>);
impl ConnectionOwner for ViewOwner {
    fn states(&self) -> EventStream<ConnectionState> {
        self.0
            .physical()
            .expect("connected physical owner")
            .states()
    }
    fn close(&self, reason: ConnectionCloseReason) -> ServiceFuture<'_, Result<(), CoreError>> {
        Box::pin(async move {
            self.0
                .release_workspace(if reason == ConnectionCloseReason::WindowDetachment {
                    LeaseReleaseIntent::ViewDetachment
                } else {
                    LeaseReleaseIntent::ResolvedWorkspaceClosure
                })?;
            let owner = self.0.physical().unwrap();
            owner.wait_unused().await;
            Ok(())
        })
    }
}
impl EnvironmentConnector {
    pub fn new(
        local: Arc<LocalConnection>,
        configuration: SshConfiguration,
    ) -> Result<Self, CoreError> {
        if configuration.bundle.is_some() && !configuration.packages.is_empty() {
            return Err(unavailable("select one trusted bundled package source"));
        }
        if configuration.packages.len() > 4 {
            return Err(unavailable(
                "at most one trusted package per supported platform is allowed",
            ));
        }
        for (i, package) in configuration.packages.iter().enumerate() {
            package.validate()?;
            if configuration.packages[..i]
                .iter()
                .any(|p| p.platform == package.platform)
            {
                return Err(unavailable("duplicate trusted platform package"));
            }
        }
        Ok(Self {
            budget: Arc::new(ConnectionBudget::default()),
            handles: Arc::new(()),
            local,
            configuration,
            state: Arc::new(Mutex::new(ConnectorState {
                entries: vec![],
                retirements: vec![],
                claims: vec![],
                queries: vec![],
                closed: false,
            })),
            workers: Arc::new(AtomicUsize::new(0)),
        })
    }
    pub fn start(
        &self,
        locator: EnvironmentLocator,
        expected: Option<TargetIdentity>,
        resume: Option<WorkspaceLease>,
    ) -> Result<ConnectRequest, CoreError> {
        self.start_admitted(locator, expected, resume, None, false, None, None)
    }
    pub fn start_saved(
        &self,
        record: &buntrail_core::workspaces::WorkspaceRecord,
    ) -> Result<ConnectRequest, CoreError> {
        self.start_admitted(
            record.locator.environment.clone(),
            Some(record.target.clone()),
            None,
            None,
            false,
            Some((record.id, record.lease)),
            None,
        )
    }
    pub fn start_observer(
        &self,
        locator: EnvironmentLocator,
        expected: TargetIdentity,
    ) -> Result<ConnectRequest, CoreError> {
        self.start_admitted(locator, Some(expected), None, None, true, None, None)
    }
    // Independent saved/resume/registration/container identities must remain explicit at admission.
    #[allow(clippy::too_many_arguments)]
    fn start_admitted(
        &self,
        locator: EnvironmentLocator,
        expected: Option<TargetIdentity>,
        resume: Option<WorkspaceLease>,
        existing: Option<Arc<ConnectionPermit>>,
        observer: bool,
        saved: Option<(WorkspaceID, WorkspaceLeaseID)>,
        selected_container: Option<SelectedContainer>,
    ) -> Result<ConnectRequest, CoreError> {
        let (permit, transition) = if let Some(existing) = existing {
            if !Arc::ptr_eq(&existing.budget, &self.budget) {
                return Err(CoreError::new(
                    ErrorCode::InvalidArgument,
                    "owner belongs to another connector",
                ));
            }
            (existing, Some(self.budget.acquire(true)?))
        } else {
            (
                self.budget.acquire(observer).or_else(|error| {
                    if resume.is_some() {
                        self.budget.acquire(true)
                    } else {
                        Err(error)
                    }
                })?,
                None,
            )
        };
        let resume_admission = if let Some((workspace, lease)) = saved {
            Some(
                self.budget
                    .owner_admission(self.local.client, workspace, lease)?,
            )
        } else {
            resume
                .clone()
                .map(|owner| self.budget.resume(owner))
                .transpose()?
        };
        static NEXT: AtomicU64 = AtomicU64::new(1);
        static NEXT_WAITER: AtomicU64 = AtomicU64::new(1);
        let valid = match locator.route {
            RouteKind::Ssh => {
                locator.ssh_profile.as_ref().is_some_and(|p| !p.is_empty())
                    && locator.engine.is_none()
                    && locator.engine_context.is_none()
                    && locator.container_selector.is_none()
                    && locator.effective_user.is_none()
            }
            RouteKind::LocalContainer => {
                locator.ssh_profile.is_none()
                    && locator.engine.is_some()
                    && locator
                        .container_selector
                        .as_ref()
                        .is_some_and(|s| !s.is_empty())
            }
            RouteKind::SshContainer => {
                locator.ssh_profile.as_ref().is_some_and(|p| !p.is_empty())
                    && locator.engine.is_some()
                    && locator
                        .container_selector
                        .as_ref()
                        .is_some_and(|s| !s.is_empty())
            }
            RouteKind::Local => false,
        };
        if !valid {
            return Err(unavailable(
                "Choose a saved SSH profile or an engine and existing container for this route",
            ));
        }
        if resume
            .as_ref()
            .is_some_and(|o| o.client != self.local.client)
        {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "workspace client changed",
            ));
        }
        let mut pool = self.state.lock().unwrap();
        if pool.closed {
            return Err(CoreError::new(
                ErrorCode::Closed,
                "environment connector is closed",
            ));
        }
        pool.entries
            .retain(|e| e.attempt.strong_count() > 0 || e.route.strong_count() > 0);
        pool.retirements
            .retain(|record| !record.done.finished.load(Ordering::SeqCst));
        if pool.retirements.len() >= MAX_CONNECTIONS {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "wait for prior physical connection retirement",
            ));
        }
        let index = pool.entries.iter().position(|e| e.locator == locator);
        let existing = index
            .and_then(|i| pool.entries[i].attempt.upgrade())
            .filter(|a| !a.cancel.load(Ordering::SeqCst));
        let attempt = if let Some(attempt) = existing {
            if attempt.expected != expected || attempt.selected_container != selected_container {
                return Err(CoreError::new(
                    ErrorCode::Busy,
                    "a connection with another target expectation is in progress",
                ));
            }
            attempt
        } else {
            if pool.entries.len() >= MAX_CONNECTIONS && index.is_none() {
                return Err(CoreError::new(
                    ErrorCode::Busy,
                    "connection capacity reached",
                ));
            }
            let attempt = Arc::new(Attempt {
                id: NEXT.fetch_add(1, Ordering::SeqCst),
                prompt: Mutex::new(None),
                expected: expected.clone(),
                selected_container: selected_container.clone(),
                state: State::new(ConnectionState::Resolving),
                broker: Mutex::new(None),
                cancel: Arc::new(AtomicBool::new(false)),
                result: Mutex::new(Completion::new()),
                dependents: AtomicUsize::new(0),
                reservation_released: AtomicBool::new(false),
            });
            let reusable = index
                .and_then(|i| pool.entries[i].route.upgrade())
                .filter(|r| r.connection_state() == ConnectionState::Ready);
            let entry = PoolEntry {
                locator: locator.clone(),
                attempt: Arc::downgrade(&attempt),
                route: reusable.as_ref().map(Arc::downgrade).unwrap_or_default(),
            };
            if let Some(i) = index {
                pool.entries[i] = entry;
            } else {
                pool.entries.push(entry);
            }
            if let Some(route) = reusable {
                if let Some(selected) = selected_container.as_ref()
                    && !selected.matches_existing(route.target(), &route.runtime())
                {
                    return Err(CoreError::new(
                        ErrorCode::TargetChanged,
                        "Selected container no longer matches the active physical target/runtime",
                    ));
                }
                route.validate_connection_ready()?;
                route.physical().unwrap().reserve()?;
                State::publish(&attempt.state, ConnectionState::Ready);
                attempt.result.lock().unwrap().complete(Ok(route));
            } else {
                let worker = attempt.clone();
                let config = self.configuration.clone();
                let client = self.local.client;
                let registry = self.state.clone();
                let location = locator.clone();
                spawn(&self.workers, move || {
                    let result = connect_ssh(&worker, config, client, &location, &registry);
                    if let Ok(route) = &result {
                        let mut registry = registry.lock().unwrap();
                        registry.retirements.push(PhysicalRecord {
                            target: route.target().clone(),
                            locator: location.clone(),
                            done: route.physical().unwrap().done.clone(),
                        });
                        if let Some(entry) = registry
                            .entries
                            .iter_mut()
                            .find(|e| e.attempt.as_ptr() == Arc::as_ptr(&worker))
                        {
                            entry.route = Arc::downgrade(route);
                        }
                    }
                    if let Err(error) = &result
                        && !matches!(
                            worker.state.lock().unwrap().value,
                            ConnectionState::TargetChanged { .. }
                        )
                    {
                        State::publish(&worker.state, ConnectionState::Failed(error.clone()));
                    }
                    worker.result.lock().unwrap().complete(result);
                    if worker.dependents.load(Ordering::SeqCst) == 0 {
                        worker.release_reservation();
                    }
                })?;
            }
            attempt
        };
        if attempt.dependents.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
            attempt.dependents.fetch_sub(1, Ordering::SeqCst);
            return Err(CoreError::new(
                ErrorCode::Busy,
                "connection dependent capacity reached",
            ));
        }
        drop(pool);
        let mut request = ConnectRequest {
            permit,
            transition,
            resume_admission,
            attached: AtomicBool::new(true),
            waiter: NEXT_WAITER.fetch_add(1, Ordering::SeqCst),
            attempt,
            cancel: Arc::new(AtomicBool::new(false)),
            registration: None,
            locator: locator.clone(),
            local_state: State::new(if resume.is_some() {
                ConnectionState::Reconnecting
            } else {
                ConnectionState::Resolving
            }),
            resume,
            saved,
            workers: self.workers.clone(),
            finished: false,
        };
        // Metadata is retained by the result mailbox, never encoded into a route or global selection.
        request.begin_registration(locator, expected)?;
        Ok(request)
    }
    pub fn shutdown(&self) {
        let mut pool = self.state.lock().unwrap();
        pool.closed = true;
        for query in pool.queries.iter().filter_map(Weak::upgrade) {
            query.cancel();
        }
        for entry in &pool.entries {
            if let Some(attempt) = entry.attempt.upgrade() {
                attempt.cancel.store(true, Ordering::SeqCst);
                if let Some(broker) = attempt.broker.lock().unwrap().as_ref() {
                    broker.cancel();
                }
            }
            if let Some(route) = entry.route.upgrade()
                && let Some(owner) = route.physical()
            {
                owner.stop();
            }
        }
    }
    pub fn shutdown_ready(&self) -> bool {
        self.workers.load(Ordering::SeqCst) == 0 && {
            let state = self.state.lock().unwrap();
            state
                .retirements
                .iter()
                .all(|r| r.done.finished.load(Ordering::SeqCst))
                && state
                    .entries
                    .iter()
                    .filter_map(|e| e.route.upgrade())
                    .all(|r| r.physical().is_none_or(|p| p.retired()))
        }
    }
}
impl ConnectRequest {
    pub fn attempt_id(&self) -> u64 {
        self.attempt.id
    }
    pub fn state(&self) -> ConnectionState {
        self.local_state.lock().unwrap().value.clone()
    }
    pub fn states(&self) -> EventStream<ConnectionState> {
        Box::pin(States {
            state: self.local_state.clone(),
            revision: 0,
        })
    }
    pub fn take_authentication(&self) -> Option<ConnectionPrompt> {
        let mut claim = self.attempt.prompt.lock().unwrap();
        if self.cancel.load(Ordering::SeqCst) {
            return None;
        }
        if claim.is_none() {
            let request = self
                .attempt
                .broker
                .lock()
                .unwrap()
                .as_ref()?
                .take_request()?;
            *claim = Some(PromptClaim {
                request,
                waiter: None,
            });
        }
        let claim = claim.as_mut()?;
        if claim.waiter.is_some() {
            return None;
        }
        claim.waiter = Some(self.waiter);
        State::publish(&self.attempt.state, ConnectionState::Authenticating);
        State::publish(&self.local_state, ConnectionState::Authenticating);
        Some(ConnectionPrompt {
            attempt: self.attempt.id,
            request: AuthenticationRequest {
                id: claim.request.id,
                kind: claim.request.kind.clone(),
                prompt: claim.request.prompt.clone(),
            },
        })
    }
    pub fn respond(&self, attempt: u64, prompt: u64, response: AuthenticationResponse) -> bool {
        let mut claim = self.attempt.prompt.lock().unwrap();
        if attempt != self.attempt.id
            || self.cancel.load(Ordering::SeqCst)
            || !claim
                .as_ref()
                .is_some_and(|c| c.request.id == prompt && c.waiter == Some(self.waiter))
        {
            return false;
        }
        let accepted = self
            .attempt
            .broker
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|b| b.respond(prompt, response));
        if accepted {
            *claim = None;
        }
        accepted
    }
    fn detach_attempt(&self) {
        if self.attached.swap(false, Ordering::SeqCst)
            && self.attempt.dependents.fetch_sub(1, Ordering::SeqCst) == 1
        {
            self.attempt.cancel_startup();
            self.attempt.release_reservation();
        }
    }
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
        if let Some(claim) = self.attempt.prompt.lock().unwrap().as_mut()
            && claim.waiter == Some(self.waiter)
        {
            claim.waiter = None;
        }
        if !self.finished {
            State::publish(&self.local_state, ConnectionState::Failed(cancelled()));
        }
        self.detach_attempt();
    }
    fn begin_registration(
        &mut self,
        locator: EnvironmentLocator,
        expected: Option<TargetIdentity>,
    ) -> Result<(), CoreError> {
        self.locator = locator;
        let completion = Arc::new(Mutex::new(Completion::new()));
        self.registration = Some(completion.clone());
        let permit = self.permit.clone();
        let transition = self.transition.clone();
        let resume_admission = self.resume_admission.clone();
        let attempt = self.attempt.clone();
        let cancel = self.cancel.clone();
        let resume = self.resume.clone();
        let saved = self.saved;
        let status = self.local_state.clone();
        let release_intent = if resume.is_some() || saved.is_some() {
            LeaseReleaseIntent::ViewDetachment
        } else {
            LeaseReleaseIntent::ResolvedWorkspaceClosure
        };
        spawn(&self.workers, move || {
            let _transition = transition;
            let _resume_admission = resume_admission;
            let result = (|| {
                let anchor = loop {
                    if cancel.load(Ordering::SeqCst) || attempt.cancel.load(Ordering::SeqCst) {
                        return Err(cancelled());
                    }
                    let current = attempt.state.lock().unwrap().value.clone();
                    State::publish(
                        &status,
                        if current == ConnectionState::Ready {
                            ConnectionState::Negotiating
                        } else if current == ConnectionState::Resolving && resume.is_some() {
                            ConnectionState::Reconnecting
                        } else {
                            current
                        },
                    );
                    if let Some(result) = attempt.result.lock().unwrap().value.clone() {
                        break result?;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };
                if let Some(expected) = expected.filter(|e| e != anchor.target()) {
                    State::publish(
                        &status,
                        ConnectionState::TargetChanged {
                            expected,
                            observed: anchor.target().clone(),
                        },
                    );
                    return Err(CoreError::new(
                        ErrorCode::TargetChanged,
                        "verified SSH target changed; explicit target selection is required",
                    ));
                }
                anchor.validate_connection_ready()?;
                let lease = if let Some(owner) = resume {
                    let observer = anchor.anchored_workspace(WorkspaceLease {
                        client: anchor.lease().client,
                        workspace: WorkspaceID(new_id()?),
                        lease: WorkspaceLeaseID(new_id()?),
                        runtime: anchor.runtime(),
                    })?;
                    let observed = observer.observe_owner(owner.clone());
                    observer.release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)?;
                    let observed = observed?;
                    if !matches!(
                        observed.presence,
                        LeasePresence::Active | LeasePresence::Detached { .. }
                    ) {
                        return Err(CoreError::new(
                            ErrorCode::TargetChanged,
                            "original owner/runtime is absent; terminal recreation and automatic resume are disabled",
                        ));
                    }
                    owner
                } else if let Some((workspace, lease)) = saved {
                    WorkspaceLease {
                        client: anchor.lease().client,
                        workspace,
                        lease,
                        runtime: anchor.runtime(),
                    }
                } else {
                    WorkspaceLease {
                        client: anchor.lease().client,
                        workspace: WorkspaceID(new_id()?),
                        lease: WorkspaceLeaseID(new_id()?),
                        runtime: anchor.runtime(),
                    }
                };
                let mut view = anchor.anchored_workspace(lease)?;
                view.bind_connection_permit(permit);
                let view = Arc::new(view);
                let mut output = completion.lock().unwrap();
                if cancel.load(Ordering::SeqCst) || attempt.cancel.load(Ordering::SeqCst) {
                    drop(output);
                    view.release_workspace(release_intent)?;
                    return Err(cancelled());
                }
                State::publish(&status, ConnectionState::Ready);
                output.complete(Ok(view));
                Ok(())
            })();
            if let Err(error) = result {
                if !matches!(
                    status.lock().unwrap().value,
                    ConnectionState::TargetChanged { .. }
                ) {
                    State::publish(&status, ConnectionState::Failed(error.clone()));
                }
                completion.lock().unwrap().complete(Err(error));
            }
        })
    }
}
impl Future for ConnectRequest {
    type Output = Result<RemoteEnvironment, CoreError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finished {
            return Poll::Ready(Err(CoreError::new(
                ErrorCode::Closed,
                "connect request already consumed",
            )));
        }
        let registration = self.registration.as_ref().unwrap().clone();
        let mut output = registration.lock().unwrap();
        if let Some(result) = output.value.take() {
            if self.cancel.load(Ordering::SeqCst)
                && let Ok(view) = &result
            {
                let view = view.clone();
                let completion = registration.clone();
                let intent = if self.resume.is_some() || self.saved.is_some() {
                    LeaseReleaseIntent::ViewDetachment
                } else {
                    LeaseReleaseIntent::ResolvedWorkspaceClosure
                };
                if let Err(error) = spawn(&self.workers, move || {
                    let result = view.release_workspace(intent);
                    if result.is_ok()
                        && let Some(owner) = view.physical()
                    {
                        run(owner.wait_unused());
                    }
                    completion
                        .lock()
                        .unwrap()
                        .complete(Err(result.err().unwrap_or_else(cancelled)));
                }) {
                    output.complete(Err(error));
                }
                output.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            self.finished = true;
            self.detach_attempt();
            self.transition.take();
            let owner_admission = if self.saved.is_some() {
                self.resume_admission.take()
            } else {
                self.resume_admission.take();
                None
            };
            Poll::Ready(result.map(|services| RemoteEnvironment {
                locator: self.locator.clone(),
                services,
                _workers: self.workers.clone(),
                _owner_admission: owner_admission,
            }))
        } else {
            output.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
impl Drop for ConnectRequest {
    fn drop(&mut self) {
        self.cancel();
        if let Some(output) = &self.registration
            && let Some(Ok(view)) = output.lock().unwrap().value.take()
        {
            let intent = if self.resume.is_some() || self.saved.is_some() {
                LeaseReleaseIntent::ViewDetachment
            } else {
                LeaseReleaseIntent::ResolvedWorkspaceClosure
            };
            let _ = spawn(&self.workers, move || {
                let _ = view.release_workspace(intent);
            });
        }
    }
}
fn claim_target(
    state: &mut ConnectorState,
    attempt: &Arc<Attempt>,
    target: &TargetIdentity,
    locator: &EnvironmentLocator,
) -> Result<(), CoreError> {
    if state.closed {
        return Err(CoreError::new(
            ErrorCode::Closed,
            "connector is shutting down",
        ));
    }
    state
        .retirements
        .retain(|record| !record.done.finished.load(Ordering::SeqCst));
    state.claims.retain(|claim| {
        claim.attempt.upgrade().is_some_and(|a| {
            !a.result
                .lock()
                .unwrap()
                .value
                .as_ref()
                .is_some_and(Result::is_err)
        })
    });
    let conflict = state
        .retirements
        .iter()
        .find(|r| &r.target == target)
        .map(|r| &r.locator)
        .or_else(|| {
            state
                .claims
                .iter()
                .find(|c| &c.target == target && c.attempt.as_ptr() != Arc::as_ptr(attempt))
                .map(|c| &c.locator)
        });
    if let Some(existing) = conflict {
        return Err(CoreError::new(
            ErrorCode::Busy,
            if existing != locator {
                "this verified target already uses another SSH profile; use its existing locator or wait for physical retirement before switching authentication policy"
            } else {
                "wait for the original physical connection to retire before reconnecting"
            },
        ));
    }
    if state.claims.len() >= MAX_CONNECTIONS {
        return Err(CoreError::new(
            ErrorCode::Busy,
            "verified target attempt capacity reached",
        ));
    }
    state.claims.push(TargetClaim {
        target: target.clone(),
        locator: locator.clone(),
        attempt: Arc::downgrade(attempt),
    });
    Ok(())
}
fn connect_ssh(
    attempt: &Arc<Attempt>,
    configuration: SshConfiguration,
    client: ClientID,
    locator: &EnvironmentLocator,
    registry: &Arc<Mutex<ConnectorState>>,
) -> Result<Arc<RemoteServices>, CoreError> {
    let broker = if locator.route == RouteKind::LocalContainer {
        None
    } else {
        Some(AuthenticationBroker::new(configuration.askpass.clone())?)
    };
    *attempt.broker.lock().unwrap() = broker.clone();
    let result = (|| {
        let ssh = if locator.route == RouteKind::LocalContainer {
            None
        } else {
            Some(SshConnector::new(
                SshProfile {
                    name: locator.ssh_profile.clone().unwrap(),
                    config: configuration.config.clone(),
                },
                broker.clone(),
                attempt.cancel.clone(),
            )?)
        };
        let container = if let Some(engine) = locator.engine.as_ref() {
            let host = if let Some(ssh) = ssh.as_ref() {
                EngineHost::ssh(ssh.clone())?
            } else {
                EngineHost::local(attempt.cancel.clone())?
            };
            let client = ContainerEngineClient::new(
                host,
                engine.clone(),
                locator.engine_context.clone(),
                locator.effective_user.clone(),
            )?;
            let selected = client.resolve(locator.container_selector.as_ref().unwrap())?;
            if let Some(expected) = attempt.selected_container.as_ref() {
                if expected.host_binding != client.host.binding()
                    || expected.pinned != client.pinned
                {
                    return Err(CoreError::new(
                        ErrorCode::TargetChanged,
                        "Selected container engine endpoint/host changed before connect",
                    ));
                }
                expected.descriptor.revalidate(&selected)?;
            }
            let connector = ContainerConnector::new(client, selected)?;
            if let Some(expected) = attempt.selected_container.as_ref() {
                expected.descriptor.revalidate(&connector.selected)?;
            }
            Some(connector)
        } else {
            None
        };
        let probe = if let Some(container) = container.as_ref() {
            container.probe()?
        } else {
            ssh.as_ref().unwrap().probe()?
        };
        let container_start = container
            .as_ref()
            .and_then(|c| c.binding.start_generation.clone());
        let platform = Platform::from_probe(&probe)?;
        let target = probe.identity.ok_or_else(|| {
            unavailable("SSH probe did not establish a supported target identity")
        })?;
        if let Some(expected) = attempt.expected.as_ref().filter(|e| *e != &target) {
            State::publish(
                &attempt.state,
                ConnectionState::TargetChanged {
                    expected: expected.clone(),
                    observed: target,
                },
            );
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "verified SSH target changed before bootstrap",
            ));
        }
        claim_target(&mut registry.lock().unwrap(), attempt, &target, locator)?;
        let package = configuration.selected_package(platform, &attempt.cancel)?;
        State::publish(&attempt.state, ConnectionState::PreparingHelper);
        let bootstrap = if let Some(container) = container {
            HelperBootstrapper::container(container, package)?
        } else {
            HelperBootstrapper::new(ssh.unwrap(), package)?
        };
        let descriptor = run(bootstrap.ensure_available(target.clone()))?;
        let bridge = bootstrap.bridge(&descriptor)?;
        State::publish(&attempt.state, ConnectionState::Negotiating);
        let lease = WorkspaceLease {
            client,
            workspace: WorkspaceID(new_id()?),
            lease: WorkspaceLeaseID(new_id()?),
            runtime: RuntimeGeneration {
                supervisor_boot: [0; 16],
                container_start,
            },
        };
        let mut remote = RemoteServices::connect(
            bridge.channel.stream()?,
            target,
            lease,
            configuration.spool_directory,
        )?;
        remote.bind_physical(bridge.channel)?;
        let owner = remote.physical().unwrap();
        if let Err(error) = remote
            .set_authentication(bridge.authentication)
            .and_then(|_| remote.validate_environment_ready())
        {
            run(owner.shutdown());
            return Err(error);
        }
        // This anchor cannot launch work; each dependent gets its own registered lease.
        if let Err(error) = remote.release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure) {
            run(owner.shutdown());
            return Err(error);
        }
        owner.ready();
        State::publish(&attempt.state, ConnectionState::Ready);
        Ok(Arc::new(remote))
    })();
    if let Some(broker) = broker {
        run(Box::pin(broker.close()));
    }
    *attempt.broker.lock().unwrap() = None;
    *attempt.prompt.lock().unwrap() = None;
    result
}
impl EnvironmentService for EnvironmentConnector {
    fn connect(
        &self,
        locator: EnvironmentLocator,
        expected: Option<TargetIdentity>,
    ) -> ServiceFuture<'_, Result<ConnectedEnvironment, CoreError>> {
        Box::pin(async move {
            if locator.route == RouteKind::Local {
                return self.local.connect(locator, expected).await;
            }
            Ok(self.start(locator, expected, None)?.await?.environment())
        })
    }
}
impl Drop for EnvironmentConnector {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handles) == 1 {
            self.shutdown();
        }
    }
}

#[cfg(test)]
#[path = "environment_tests.rs"]
mod tests;
