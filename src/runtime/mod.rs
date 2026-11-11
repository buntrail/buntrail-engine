//! Current ABI routes into verified services; no document state lives here.
use crate::{Result, environment::*, fail, number, remote::RemoteServices, string, worker::run};
use buntrail_core::{Text, contracts::*};
use buntrail_services::{FileBinding, Services, connections::local::new_id, contracts::*, files};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context as TaskContext, Poll, Waker},
};
mod explorer_adapter;
mod files_adapter;
mod git_adapter;
mod search_adapter;
mod terminal_adapter;
#[cfg(test)]
mod tests;
mod workspace_adapter;
fn error(e: CoreError) -> crate::Failure {
    fail("service_error", e.message)
}
pub(super) struct Runtime {
    services: Arc<Services>,
    documents: Arc<Mutex<crate::Documents>>,
    admission: std::sync::RwLock<()>,
    git_gate: buntrail_core::git::worktree_gate::WorktreeMutationGate,
    context: OnceLock<std::result::Result<Arc<Context>, String>>,
    registry: Mutex<Registry>,
    terminals: Mutex<terminal_adapter::Terminals>,
}
struct Context {
    connector: EnvironmentConnector,
}
#[derive(Default)]
struct Registry {
    boot: Option<[u8; 16]>,
    next: u64,
    attempts: HashMap<String, Arc<Mutex<ConnectRequest>>>,
    remotes: HashMap<String, Route>,
}
struct Route {
    services: Arc<RemoteServices>,
    _owner: Option<RemoteEnvironment>,
    locator: EnvironmentLocator,
}
impl Registry {
    fn id(&mut self, prefix: &str) -> Result<String> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Remote IDs exhausted"))?;
        let boot = match self.boot {
            Some(id) => id,
            None => {
                let id = new_id().map_err(error)?;
                self.boot = Some(id);
                id
            }
        };
        Ok(format!(
            "{prefix}-{}-{}",
            buntrail_protocol::services::uuid(boot),
            self.next
        ))
    }
}
impl Runtime {
    pub fn new(services: Arc<Services>, documents: Arc<Mutex<crate::Documents>>) -> Self {
        Self {
            services,
            documents,
            admission: Default::default(),
            git_gate: Default::default(),
            context: OnceLock::new(),
            registry: Mutex::new(Registry::default()),
            terminals: Mutex::new(Default::default()),
        }
    }
    fn context(&self) -> Result<Arc<Context>> {
        self.context
            .get_or_init(|| {
                let local = self.services.local_connection()?;
                let directory = self.services.storage_directory()?;
                let binary = std::env::current_exe().map_err(|e| e.to_string())?;
                let parent = binary.parent().ok_or("Executable directory unavailable")?;
                let resources = if parent.file_name().is_some_and(|n| n == "MacOS") {
                    parent.parent().unwrap().join("Resources")
                } else {
                    parent.to_owned()
                };
                let askpass = std::env::var_os("BUNTRAIL_ASKPASS_BINARY")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| resources.join("buntrail-askpass"));
                let packages = std::env::var_os("BUNTRAIL_HELPER_PACKAGES")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| resources.join("HelperPackages"));
                let connector = EnvironmentConnector::new(
                    local.clone(),
                    SshConfiguration {
                        askpass,
                        packages: vec![],
                        bundle: Some(Arc::new(BundledPackages::new(packages))),
                        config: None,
                        spool_directory: directory.join("remote-spools"),
                    },
                )
                .map_err(|e| e.message)?;
                Ok(Arc::new(Context { connector }))
            })
            .as_ref()
            .cloned()
            .map_err(|e| fail("unavailable", e.clone()))
    }
    fn route(&self, id: &str) -> Result<Arc<RemoteServices>> {
        self.registry
            .lock()
            .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?
            .remotes
            .get(id)
            .map(|route| route.services.clone())
            .ok_or_else(|| {
                fail(
                    "not_found",
                    "Remote environment is unavailable; reconnect its verified identity",
                )
            })
    }
    fn locator(request: &Value) -> Result<EnvironmentLocator> {
        if let Some(locator) = request.get("locator") {
            return serde_json::from_value(locator.clone())
                .map_err(|e| fail("invalid_request", e.to_string()));
        }
        let transport = string(request, "transport")?;
        let target = string(request, "target")?.to_owned();
        let engine = match transport {
            "ssh" => None,
            "docker" | "ssh-docker" => Some(ContainerEngine::Docker),
            "podman" | "ssh-podman" => Some(ContainerEngine::Podman),
            _ => return Err(fail("invalid_request", "Unsupported remote transport")),
        };
        let ssh = transport == "ssh" || transport.starts_with("ssh-");
        Ok(EnvironmentLocator {
            route: if transport == "ssh" {
                RouteKind::Ssh
            } else if ssh {
                RouteKind::SshContainer
            } else {
                RouteKind::LocalContainer
            },
            ssh_profile: if ssh { Some(target.clone()) } else { None },
            engine: engine.clone(),
            engine_context: request
                .get("context")
                .and_then(Value::as_str)
                .map(str::to_owned),
            container_selector: if engine.is_some() {
                Some(if ssh {
                    string(request, "container")?.to_owned()
                } else {
                    target
                })
            } else {
                None
            },
            effective_user: request
                .get("user")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }
    fn begin(&self, request: &Value) -> Result<Value> {
        let saved = if let Some(workspace) = request.get("workspace") {
            let id: WorkspaceID = serde_json::from_value(workspace.clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?;
            Some(
                self.services
                    .local_connection()
                    .map_err(|e| fail("unavailable", e))?
                    .store
                    .load()
                    .map_err(error)?
                    .records
                    .into_iter()
                    .find(|r| r.id == id)
                    .ok_or_else(|| fail("not_found", "Saved workspace not found"))?,
            )
        } else {
            None
        };
        let saved = if saved.is_none() && request["new_target"] != true {
            if let Some(root) = request.get("root").and_then(Value::as_str) {
                let locator = Self::locator(request)?;
                self.services
                    .local_connection()
                    .map_err(|e| fail("unavailable", e))?
                    .store
                    .load()
                    .map_err(error)?
                    .records
                    .into_iter()
                    .find(|record| {
                        record.locator.environment == locator
                            && files_adapter::text_path(&record.locator.root).is_ok_and(|path| {
                                path.trim_end_matches('/') == root.trim_end_matches('/')
                            })
                    })
            } else {
                None
            }
        } else {
            saved
        };
        let locator = if let Some(record) = &saved {
            record.locator.environment.clone()
        } else {
            Self::locator(request)?
        };
        if locator.route == RouteKind::Local {
            return Err(fail(
                "invalid_request",
                "Use local services for a local environment",
            ));
        }
        let expected: Option<TargetIdentity> = request
            .get("expected")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| fail("invalid_request", e.to_string()))?;
        let resume: Option<WorkspaceLease> = request
            .get("resume")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| fail("invalid_request", e.to_string()))?;
        if request.get("workspace").is_some() && request["new_target"] == true {
            return Err(fail(
                "invalid_request",
                "Choose either a saved workspace or a new target",
            ));
        }
        if let Some(record) = &saved {
            if expected
                .as_ref()
                .is_some_and(|expected| expected != &record.target)
            {
                return Err(fail(
                    "wrong_target",
                    "Explicit expected target differs from the saved workspace",
                ));
            }
            if let Some(owner) = &resume
                && (owner.workspace != record.id
                    || owner.lease != record.lease
                    || owner.client
                        != self
                            .services
                            .local_connection()
                            .map_err(|e| fail("unavailable", e))?
                            .client)
            {
                return Err(fail(
                    "wrong_workspace",
                    "Resume owner differs from the saved workspace",
                ));
            }
        }
        if let Some(record) = &saved {
            let matches = {
                let registry = self
                    .registry
                    .lock()
                    .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?;
                registry
                    .remotes
                    .iter()
                    .filter(|(_, route)| {
                        route.services.lease().workspace == record.id
                            && route.services.lease().lease == record.lease
                            && route.services.target() == &record.target
                    })
                    .map(|(id, route)| (id.clone(), route.services.clone()))
                    .collect::<Vec<_>>()
            };
            if let Some((id, ready)) = matches
                .iter()
                .find(|(_, remote)| remote.validate_environment_ready().is_ok())
            {
                if resume.as_ref().is_some_and(|owner| owner != ready.lease()) {
                    return Err(fail(
                        "runtime_lost",
                        "The original resume runtime is unavailable",
                    ));
                }
                return Ok(json!({"id":id}));
            }
            for (id, remote) in matches {
                let _ = remote.detach_workspace();
                self.terminals.lock().unwrap().detach(&id, true);
                self.registry.lock().unwrap().remotes.remove(&id);
                if let Some(owner) = remote.physical() {
                    run(owner.wait_unused());
                }
            }
        }
        let context = self.context()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?;
        if registry.attempts.len() + registry.remotes.len() >= 32 {
            return Err(fail("busy", "Remote environment capacity reached"));
        }
        let connect = if let Some(record) = saved {
            if let Some(owner) = resume {
                context.connector.start(
                    record.locator.environment,
                    Some(record.target),
                    Some(owner),
                )
            } else {
                context.connector.start_saved(&record)
            }
        } else {
            context.connector.start(locator, expected, resume)
        }
        .map_err(error)?;
        let id = registry.id("connection")?;
        registry
            .attempts
            .insert(id.clone(), Arc::new(Mutex::new(connect)));
        Ok(json!({"id":id}))
    }
    fn attempt(&self, id: &str) -> Result<Arc<Mutex<ConnectRequest>>> {
        self.registry
            .lock()
            .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?
            .attempts
            .get(id)
            .cloned()
            .ok_or_else(|| fail("not_found", "Connection attempt is no longer active"))
    }
    fn poll(&self, id: &str) -> Result<Value> {
        let ready = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?;
            registry
                .remotes
                .get(id)
                .map(|route| (route.services.clone(), route.locator.clone()))
        };
        if let Some((remote, locator)) = ready {
            remote.validate_environment_ready().map_err(error)?;
            return Ok(
                json!({"done":true,"reused":true,"id":id,"target":remote.target(),"runtime":remote.runtime(),"lease":remote.lease(),"locator":locator}),
            );
        }
        let attempt = self.attempt(id)?;
        let mut request = attempt
            .lock()
            .map_err(|_| fail("engine_failed", "Connection attempt poisoned"))?;
        let state = request.state();
        if let Some(prompt) = request.take_authentication() {
            return Ok(
                json!({"done":false,"state":state,"prompt":{"attempt":prompt.attempt,"id":prompt.request.id,"kind":prompt.request.kind,"text":prompt.request.prompt}}),
            );
        }
        match Pin::new(&mut *request).poll(&mut TaskContext::from_waker(Waker::noop())) {
            Poll::Pending => Ok(json!({"done":false,"state":state})),
            Poll::Ready(result) => {
                let mut registry = self
                    .registry
                    .lock()
                    .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?;
                registry.attempts.remove(id);
                let environment = result.map_err(error)?;
                let remote = registry.id("remote")?;
                let value = json!({"done":true,"id":remote,"target":environment.services.target(),"runtime":environment.services.runtime(),"lease":environment.services.lease(),"locator":environment.locator});
                registry.remotes.insert(
                    remote,
                    Route {
                        services: environment.services.clone(),
                        locator: environment.locator.clone(),
                        _owner: Some(environment),
                    },
                );
                Ok(value)
            }
        }
    }
    pub fn call(&self, request: &Value) -> Result<Value> {
        self.call_with_cancellation(request, None)
    }
    pub fn call_with_cancellation(
        &self,
        request: &Value,
        cancellation: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<Value> {
        if request["command"].as_str().is_some_and(|c| {
            c.starts_with("workspace.") || c.starts_with("preferences.") || c == "capabilities"
        }) {
            return self.workspace_call(None, request);
        }
        let _admission = if request["command"]
            .as_str()
            .is_some_and(|c| c.starts_with("fs."))
        {
            Some(self.document_admission()?)
        } else {
            None
        };
        if request["command"]
            .as_str()
            .is_some_and(|c| c.starts_with("git"))
        {
            return self.git_call(None, request, cancellation);
        }
        if request["command"].as_str().is_some_and(|c| {
            matches!(
                c,
                "fs.import"
                    | "fs.pending"
                    | "fs.inspect_operation"
                    | "fs.trash"
                    | "fs.explorer_capabilities"
            ) || (c.starts_with("fs.")
                && request.get("root").is_some()
                && matches!(c, "fs.create" | "fs.mkdir" | "fs.rename" | "fs.remove"))
        }) {
            return self.explorer_call(None, request);
        }
        if request["command"]
            .as_str()
            .is_some_and(|c| c.starts_with("fs."))
        {
            return self
                .services
                .call(request)
                .map_err(|e| fail("service_error", e));
        }
        if string(request, "command")?.starts_with("terminal.") {
            return self.terminal_call(None, request);
        }
        if request["command"] == "search" {
            return self.search_collect(None, request, cancellation);
        }
        match string(request, "command")? {
            "remote.begin" => self.begin(request),
            "remote.poll" => self.poll(string(request, "id")?),
            "remote.respond" => {
                let attempt = self.attempt(string(request, "id")?)?;
                let attempt = attempt
                    .lock()
                    .map_err(|_| fail("engine_failed", "Connection attempt poisoned"))?;
                let response = match string(request, "kind")? {
                    "cancel" => AuthenticationResponse::Cancelled,
                    "confirm" => AuthenticationResponse::Confirm(
                        request["accepted"]
                            .as_bool()
                            .ok_or_else(|| fail("invalid_request", "accepted must be a boolean"))?,
                    ),
                    "secret" => {
                        let secret = string(request, "secret")?;
                        if secret.len() > 4096 {
                            return Err(fail(
                                "invalid_request",
                                "Authentication response exceeds limit",
                            ));
                        }
                        AuthenticationResponse::Secret(secret.as_bytes().to_vec())
                    }
                    _ => return Err(fail("invalid_request", "Invalid authentication response")),
                };
                if !attempt.respond(
                    number(request, "attempt")?,
                    number(request, "prompt")?,
                    response,
                ) {
                    return Err(fail("stale_revision", "Authentication prompt expired"));
                }
                Ok(Value::Null)
            }
            "remote.cancel" => {
                let id = string(request, "id")?;
                if self.registry.lock().unwrap().remotes.contains_key(id) {
                    return Ok(Value::Null);
                }
                let attempt = self
                    .registry
                    .lock()
                    .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?
                    .attempts
                    .remove(id)
                    .ok_or_else(|| fail("not_found", "Connection attempt is no longer active"))?;
                attempt
                    .lock()
                    .map_err(|_| fail("engine_failed", "Connection attempt poisoned"))?
                    .cancel();
                Ok(Value::Null)
            }
            "remote.start" => {
                let begin = self.begin(request)?;
                let id = string(&begin, "id")?;
                loop {
                    let result = self.poll(id)?;
                    if result["done"] == true {
                        return Ok(result);
                    }
                    if result.get("prompt").is_some() {
                        let _ = self.call(&json!({"command":"remote.cancel","id":id}));
                        return Err(fail(
                            "authentication_required",
                            "Use the interactive connection flow to answer authentication",
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            }
            "remote.call" => {
                let remote = self.route(string(request, "id")?)?;
                let remote = if let Some(cancel) = cancellation.clone() {
                    Arc::new(remote.with_cancellation(cancel))
                } else {
                    remote
                };
                remote.validate_environment_ready().map_err(error)?;
                if request["request"]["command"]
                    .as_str()
                    .is_some_and(|c| c.starts_with("terminal."))
                {
                    return self.terminal_call(Some(string(request, "id")?), &request["request"]);
                }
                if request["request"]["command"].as_str().is_some_and(|c| {
                    c.starts_with("workspace.")
                        || c.starts_with("preferences.")
                        || c == "capabilities"
                }) {
                    return self.workspace_call(Some(string(request, "id")?), &request["request"]);
                }
                if request["request"]["command"] == "recovery.inspect" {
                    let document = serde_json::from_value(request["request"]["document"].clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))?;
                    return self
                        .services
                        .inspect_recovery(document, |op, target| run(remote.inspect(op, target)))
                        .map_err(|e| fail("recovery_failed", e));
                }
                if request["request"]["command"] == "search" {
                    return self.search_collect(
                        Some(string(request, "id")?),
                        &request["request"],
                        cancellation,
                    );
                }
                if request["request"]["command"]
                    .as_str()
                    .is_some_and(|c| c.starts_with("git"))
                {
                    return self.git_call(
                        Some(string(request, "id")?),
                        &request["request"],
                        cancellation,
                    );
                }
                let _admission = self.document_admission()?;
                if request["request"]["command"].as_str().is_some_and(|c| {
                    matches!(
                        c,
                        "fs.pending"
                            | "fs.inspect_operation"
                            | "fs.trash"
                            | "fs.explorer_capabilities"
                            | "fs.import"
                    ) || (request["request"].get("root").is_some()
                        && matches!(c, "fs.create" | "fs.mkdir" | "fs.rename" | "fs.remove"))
                }) {
                    return self.explorer_call(Some(string(request, "id")?), &request["request"]);
                }
                self.files_call(
                    &remote,
                    request
                        .get("request")
                        .filter(|v| v.is_object())
                        .ok_or_else(|| fail("invalid_request", "request must be an object"))?,
                )
            }
            "remote.close" | "remote.detach" => {
                let id = string(request, "id")?;
                let remote = self.route(id)?;
                let detach = request["command"] == "remote.detach";
                let confirmed = if detach {
                    remote.detach_workspace().is_ok()
                } else {
                    remote
                        .release_workspace(LeaseReleaseIntent::ResolvedWorkspaceClosure)
                        .map_err(error)?;
                    if let Some(owner) = remote.physical() {
                        run(owner.wait_unused());
                    }
                    true
                };
                self.terminals.lock().unwrap().detach(id, detach);
                let saved = {
                    let mut registry = self
                        .registry
                        .lock()
                        .map_err(|_| fail("engine_failed", "Remote registry poisoned"))?;
                    let route = registry.remotes.remove(id).ok_or_else(|| {
                        fail("not_found", "Remote environment is no longer active")
                    })?;
                    json!({"locator":route.locator,"target":remote.target(),"runtime":remote.runtime(),"lease":remote.lease(),"confirmed":confirmed})
                };
                Ok(saved)
            }
            _ => Err(fail("invalid_request", "Unknown remote operation")),
        }
    }
}
