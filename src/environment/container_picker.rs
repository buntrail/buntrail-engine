//! Picker work uses the connector's admission, worker retirement and authentication broker.
use super::*;
use buntrail_services::connections::{
    container_engine::{PinnedEngine, StartOutcome},
    container_identity::ContainerDescriptor,
};

#[derive(Clone)]
pub struct ContainerInventory {
    pub locator: EnvironmentLocator,
    pub host_binding: Vec<u8>,
    pub pinned: PinnedEngine,
    pub containers: Vec<ContainerDescriptor>,
    pub start_uncertain: bool,
    pub started: bool,
    pub message: String,
}
#[derive(Clone, PartialEq, Eq)]
pub(super) struct SelectedContainer {
    pub host_binding: Vec<u8>,
    pub pinned: PinnedEngine,
    pub descriptor: ContainerDescriptor,
}
impl SelectedContainer {
    pub(super) fn matches_existing(
        &self,
        target: &TargetIdentity,
        runtime: &RuntimeGeneration,
    ) -> bool {
        self.descriptor.state == ContainerState::Running
            && self
                .descriptor
                .actual_uid
                .is_none_or(|uid| target.effective_user == uid.to_be_bytes())
            && target.security_binding == self.host_binding
            && target.container.as_ref().is_some_and(|container| {
                container.engine_endpoint == self.descriptor.engine_endpoint
                    && container.immutable_id == self.descriptor.immutable_id.as_bytes()
            })
            && runtime.container_start == self.descriptor.start_generation
    }
}
impl ContainerInventory {
    pub(super) fn selected(&self, index: usize) -> Result<SelectedContainer, CoreError> {
        if self.start_uncertain {
            return Err(unavailable(
                "Original Start result is uncertain; inspect this exact container ID before connecting",
            ));
        }
        Ok(SelectedContainer {
            host_binding: self.host_binding.clone(),
            pinned: self.pinned.clone(),
            descriptor: self
                .containers
                .get(index)
                .ok_or_else(|| {
                    CoreError::new(ErrorCode::InvalidArgument, "Choose an existing container")
                })?
                .clone(),
        })
    }
}
pub(super) struct QueryState {
    id: u64,
    cancel: Arc<AtomicBool>,
    done: Arc<Done>,
    result: Mutex<Option<Result<Arc<ContainerInventory>, CoreError>>>,
    broker: Mutex<Option<Arc<AuthenticationBroker>>>,
    prompt: Mutex<Option<AuthenticationRequest>>,
}
impl QueryState {
    pub(super) fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
        if let Some(b) = self.broker.lock().unwrap().as_ref() {
            b.cancel();
        }
    }
}
pub struct ContainerQuery {
    state: Arc<QueryState>,
    _permit: Arc<ConnectionPermit>,
}
impl Drop for ContainerQuery {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl ContainerQuery {
    pub fn cancel(&self) {
        self.state.cancel();
    }
    pub fn result(&self) -> Option<Result<Arc<ContainerInventory>, CoreError>> {
        self.state.result.lock().unwrap().clone()
    }
    pub fn finished(&self) -> bool {
        self.state.done.finished.load(Ordering::SeqCst)
    }
    pub fn take_authentication(&self) -> Option<ConnectionPrompt> {
        if self.state.cancel.load(Ordering::SeqCst) {
            return None;
        }
        let mut prompt = self.state.prompt.lock().unwrap();
        if prompt.is_some() {
            return None;
        }
        let request = self.state.broker.lock().unwrap().as_ref()?.take_request()?;
        let copied = AuthenticationRequest {
            id: request.id,
            kind: request.kind.clone(),
            prompt: request.prompt.clone(),
        };
        *prompt = Some(request);
        Some(ConnectionPrompt {
            attempt: self.state.id,
            request: copied,
        })
    }
    pub fn respond(&self, attempt: u64, prompt: u64, response: AuthenticationResponse) -> bool {
        let mut pending = self.state.prompt.lock().unwrap();
        if self.state.cancel.load(Ordering::SeqCst)
            || attempt != self.state.id
            || pending.as_ref().is_none_or(|p| p.id != prompt)
        {
            return false;
        }
        let accepted = self
            .state
            .broker
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|b| b.respond(prompt, response));
        if accepted {
            *pending = None;
        }
        accepted
    }
}
impl EnvironmentConnector {
    pub fn container_inventory(
        &self,
        locator: EnvironmentLocator,
    ) -> Result<ContainerQuery, CoreError> {
        self.container_query(locator, None)
    }
    pub fn start_container(
        &self,
        inventory: Arc<ContainerInventory>,
        index: usize,
    ) -> Result<ContainerQuery, CoreError> {
        let selected = inventory.selected(index)?;
        self.container_query(inventory.locator.clone(), Some(selected))
    }
    fn container_query(
        &self,
        locator: EnvironmentLocator,
        start: Option<SelectedContainer>,
    ) -> Result<ContainerQuery, CoreError> {
        let valid = match locator.route {
            RouteKind::LocalContainer => locator.ssh_profile.is_none(),
            RouteKind::SshContainer => locator.ssh_profile.as_ref().is_some_and(|p| !p.is_empty()),
            _ => false,
        };
        if !valid || locator.engine.is_none() {
            return Err(CoreError::new(
                ErrorCode::InvalidArgument,
                "Choose a container engine and its local or saved SSH route",
            ));
        }
        let permit = self.budget.acquire(false)?;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let state = Arc::new(QueryState {
            id: NEXT.fetch_add(1, Ordering::SeqCst),
            cancel: Arc::new(AtomicBool::new(false)),
            done: Arc::new(Done::default()),
            result: Mutex::new(None),
            broker: Mutex::new(None),
            prompt: Mutex::new(None),
        });
        let mut registry = self.state.lock().unwrap();
        if registry.closed {
            return Err(CoreError::new(
                ErrorCode::Closed,
                "Connector is shutting down",
            ));
        }
        registry.queries.retain(|q| q.strong_count() != 0);
        if registry.queries.len() >= MAX_CONNECTIONS {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "Container picker capacity reached",
            ));
        }
        registry.queries.push(Arc::downgrade(&state));
        drop(registry);
        let configuration = self.configuration.clone();
        let worker = state.clone();
        let worker_permit = permit.clone();
        spawn(&self.workers, move || {
            let _permit = worker_permit;
            let result = (|| {
                let broker = if locator.route == RouteKind::SshContainer {
                    Some(AuthenticationBroker::new(configuration.askpass.clone())?)
                } else {
                    None
                };
                *worker.broker.lock().unwrap() = broker.clone();
                let outcome = (|| {
                    let host = if locator.route == RouteKind::SshContainer {
                        EngineHost::ssh(SshConnector::new(
                            SshProfile {
                                name: locator.ssh_profile.clone().unwrap(),
                                config: configuration.config.clone(),
                            },
                            broker.clone(),
                            worker.cancel.clone(),
                        )?)?
                    } else {
                        EngineHost::local(worker.cancel.clone())?
                    };
                    let client = ContainerEngineClient::new(
                        host,
                        locator.engine.clone().unwrap(),
                        locator.engine_context.clone(),
                        locator.effective_user.clone(),
                    )?;
                    let mut inventory = ContainerInventory {
                        locator: locator.clone(),
                        host_binding: client.host.binding().to_vec(),
                        pinned: client.pinned.clone(),
                        containers: vec![],
                        start_uncertain: false,
                        started: false,
                        message: String::new(),
                    };
                    if let Some(selected) = start {
                        if inventory.host_binding != selected.host_binding
                            || inventory.pinned != selected.pinned
                        {
                            return Err(CoreError::new(
                                ErrorCode::TargetChanged,
                                "Chosen engine endpoint or host changed before Start",
                            ));
                        }
                        match client.start(&selected.descriptor) {
                            StartOutcome::Started(v) => {
                                inventory.started = true;
                                inventory.containers.push(v);
                                inventory.message =
                                    "Container started; verify the environment and choose a folder"
                                        .into();
                            }
                            StartOutcome::StartedUnsupported { selected, error } => {
                                inventory.started = true;
                                inventory.containers.push(selected);
                                inventory.message = format!(
                                    "Container started, but environment verification is unavailable: {}",
                                    error.message
                                );
                            }
                            StartOutcome::Failed(error) => return Err(error),
                            StartOutcome::Uncertain { selected, error } => {
                                inventory.containers.push(selected);
                                inventory.start_uncertain = true;
                                inventory.message = format!(
                                    "Start result is uncertain: {}. Refresh and inspect this original ID; no automatic retry was issued.",
                                    error.message
                                );
                            }
                        }
                    } else if let Some(selector) = locator.container_selector.as_ref() {
                        inventory.containers.push(client.resolve(selector)?);
                    } else {
                        inventory.containers = client.list()?;
                    }
                    Ok(Arc::new(inventory))
                })();
                if let Some(broker) = broker {
                    run(Box::pin(broker.close()));
                }
                *worker.broker.lock().unwrap() = None;
                *worker.prompt.lock().unwrap() = None;
                outcome
            })();
            *worker.result.lock().unwrap() = Some(result);
            worker.done.finish();
        })?;
        Ok(ContainerQuery {
            state,
            _permit: permit,
        })
    }
    pub fn connect_selected_container(
        &self,
        inventory: Arc<ContainerInventory>,
        index: usize,
    ) -> Result<ConnectRequest, CoreError> {
        let selected = inventory.selected(index)?;
        let mut locator = inventory.locator.clone();
        locator.container_selector = Some(selected.descriptor.immutable_id.clone());
        self.start_admitted(locator, None, None, None, false, None, Some(selected))
    }
}
