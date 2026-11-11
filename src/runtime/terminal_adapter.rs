use super::*;
use buntrail_core::{contracts::async_types::EventStream, terminal::layout::*, workspaces::*};
use std::task::{Context as PollContext, Poll, Waker};

#[derive(Default)]
pub(super) struct Terminals {
    workspaces: HashMap<String, Arc<Mutex<TerminalWorkspace>>>,
    sessions: HashMap<String, Arc<TerminalPane>>,
    detached: HashMap<WorkspaceID, DetachedTerminalWorkspace>,
}
pub(super) struct TerminalWorkspace {
    pub(super) record: WorkspaceRecord,
    pub(super) owner: WorkspaceLease,
    service: Arc<dyn TerminalService>,
}
struct DetachedTerminalWorkspace {
    target: TargetIdentity,
    owner: WorkspaceLease,
    layout: TerminalLayoutState,
    starts: Vec<TerminalStart>,
    generation: u64,
    expires: std::time::Instant,
}
impl TerminalWorkspace {
    fn advance_generation(&mut self) -> Result<()> {
        self.record.generation = self
            .record
            .generation
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Workspace generation exhausted"))?;
        Ok(())
    }
}
struct TerminalPane {
    workspace: Arc<Mutex<TerminalWorkspace>>,
    session: TerminalSessionID,
    stream: Mutex<Output>,
}
#[derive(Default)]
struct Output {
    stream: Option<EventStream<TerminalEvent>>,
    sequence: u64,
    drained: bool,
}
impl Terminals {
    pub(super) fn detach(&mut self, route: &str, retain_for_reattach: bool) {
        let prefix = format!("{route}:");
        let workspaces = self
            .workspaces
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        self.detached
            .retain(|_, saved| saved.expires > std::time::Instant::now());
        for workspace in &workspaces {
            let w = workspace.lock().unwrap();
            if retain_for_reattach && !w.record.terminal_layout.tabs.is_empty() {
                self.detached.insert(
                    w.record.id,
                    DetachedTerminalWorkspace {
                        target: w.record.target.clone(),
                        owner: w.owner.clone(),
                        layout: w.record.terminal_layout.clone(),
                        starts: w.record.terminal_starts.clone(),
                        generation: w.record.generation,
                        expires: std::time::Instant::now()
                            + std::time::Duration::from_secs(30 * 60),
                    },
                );
            } else {
                self.detached.remove(&w.record.id);
            }
        }
        self.sessions
            .retain(|_, pane| !workspaces.iter().any(|w| Arc::ptr_eq(w, &pane.workspace)));
        self.workspaces.retain(|key, _| !key.starts_with(&prefix));
    }
}
fn id(id: TerminalSessionID) -> String {
    buntrail_protocol::services::uuid(id.0)
}
fn layout_value(workspace: &TerminalWorkspace) -> Value {
    let mut layout = json!(workspace.record.terminal_layout);
    layout["workspace"] = json!(workspace.record.id);
    layout["generation"] = json!(workspace.record.generation);
    layout
}
fn size(r: &Value) -> Result<TerminalSize> {
    let dimension = |key, default| {
        r.get(key)
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u16::try_from(n).ok())
                    .and_then(std::num::NonZeroU16::new)
            })
            .unwrap_or_else(|| std::num::NonZeroU16::new(default))
            .ok_or_else(|| {
                fail(
                    "invalid_request",
                    "Terminal dimension must be between 1 and 65535",
                )
            })
    };
    Ok(TerminalSize {
        columns: dimension("columns", 80)?,
        rows: dimension("rows", 24)?,
    })
}
impl Runtime {
    pub(super) fn terminal_workspace(
        &self,
        route: Option<&str>,
        r: &Value,
    ) -> Result<Arc<Mutex<TerminalWorkspace>>> {
        let path = if route.is_none() {
            std::fs::canonicalize(string(r, "root")?)
                .map_err(|e| fail("not_found", e.to_string()))?
                .to_str()
                .ok_or_else(|| fail("unavailable", "Workspace path is not UTF-8"))?
                .to_owned()
        } else {
            string(r, "root")?.to_owned()
        };
        let root = TargetPath::posix(path.as_bytes().to_vec()).map_err(error)?;
        let key = format!("{}:{}", route.unwrap_or("local"), path);
        let mut terminals = self
            .terminals
            .lock()
            .map_err(|_| fail("engine_failed", "Terminal registry poisoned"))?;
        if let Some(workspace) = terminals.workspaces.get(&key) {
            return Ok(workspace.clone());
        }
        let local = self
            .services
            .local_connection()
            .map_err(|e| fail("unavailable", e))?;
        let (mut record, owner, service): (_, _, Arc<dyn TerminalService>) =
            if let Some(route) = route {
                let remote = self.route(route)?;
                let locator = self.registry.lock().unwrap().remotes[route].locator.clone();
                let owner = remote.lease().clone();
                let catalog = local.store.load().map_err(error)?;
                let record = match catalog
                    .records
                    .into_iter()
                    .find(|w| w.id == owner.workspace)
                {
                    Some(record)
                        if record.target == *remote.target()
                            && record.lease == owner.lease
                            && record.locator.root == root =>
                    {
                        record
                    }
                    Some(_) => {
                        return Err(fail(
                            "wrong_workspace",
                            "Remote owner already has a different workspace root",
                        ));
                    }
                    None => local
                        .store
                        .register_resolved(WorkspaceRecord {
                            id: owner.workspace,
                            generation: 0,
                            lease: owner.lease,
                            locator: WorkspaceLocator {
                                environment: locator,
                                root,
                            },
                            target: remote.target().clone(),
                            display_name: string(r, "root")?.into(),
                            documents: vec![],
                            selected: None,
                            show_hidden: true,
                            show_icons: true,
                            sidebar_visible: true,
                            terminal_visible: true,
                            terminal_height: None,
                            terminal_layout: Default::default(),
                            terminal_starts: vec![],
                            graph_filters: vec![],
                            git_recoveries: vec![],
                            git_recovery_generation: 0,
                        })
                        .map_err(error)?,
                };
                (record, owner, remote)
            } else {
                let record = match local
                    .store
                    .open(root, local.identity.clone())
                    .map_err(error)?
                {
                    WorkspaceState::Ready(record) => record,
                    WorkspaceState::Missing(_) => {
                        return Err(fail("not_found", "Workspace directory unavailable"));
                    }
                };
                local.register(record.id, record.lease).map_err(error)?;
                let owner = WorkspaceLease {
                    client: local.client,
                    workspace: record.id,
                    lease: record.lease,
                    runtime: local.runtime.clone(),
                };
                (record, owner, local.terminal.clone())
            };
        if let Some(saved) = terminals.detached.remove(&record.id)
            && saved.expires > std::time::Instant::now()
            && saved.target == record.target
            && saved.owner == owner
        {
            record.terminal_layout = saved.layout;
            record.terminal_starts = saved.starts;
            record.generation = record.generation.max(saved.generation);
        }
        let workspace = Arc::new(Mutex::new(TerminalWorkspace {
            record,
            owner,
            service,
        }));
        // Restoring a layout only reconstructs handles. It never starts a process.
        for session in workspace.lock().unwrap().record.terminal_layout.sessions() {
            terminals.sessions.insert(
                id(session),
                Arc::new(TerminalPane {
                    workspace: workspace.clone(),
                    session,
                    stream: Mutex::new(Output::default()),
                }),
            );
        }
        terminals.workspaces.insert(key, workspace.clone());
        Ok(workspace)
    }
    pub(super) fn persist_workspace(&self, workspace: &mut TerminalWorkspace) -> Result<()> {
        workspace.advance_generation()?;
        self.services
            .local_connection()
            .map_err(|e| fail("unavailable", e))?
            .store
            .update(workspace.record.clone())
            .map_err(error)
    }
    pub(super) fn terminal_call(&self, route: Option<&str>, r: &Value) -> Result<Value> {
        let command = string(r, "command")?;
        if command == "terminal.layout" {
            let workspace = self.terminal_workspace(route, r)?;
            return Ok(layout_value(&workspace.lock().unwrap()));
        }
        if command == "terminal.start" {
            let directory = r
                .get("directory")
                .map(|value| {
                    let path = value.as_str().ok_or_else(|| {
                        fail(
                            "invalid_request",
                            "directory must be an absolute UTF-8 path",
                        )
                    })?;
                    TargetPath::posix(path.as_bytes().to_vec()).map_err(error)
                })
                .transpose()?;
            let workspace = self.terminal_workspace(route, r)?;
            let session = TerminalSessionID(new_id().map_err(error)?);
            let operation = OperationID(new_id().map_err(error)?);
            let (service, request) = {
                let mut w = workspace.lock().unwrap();
                let mut layout = w.record.terminal_layout.clone();
                if let Some(split) = r.get("split").and_then(Value::as_str) {
                    let target =
                        TerminalSessionID(buntrail_protocol::services::id(split).map_err(error)?);
                    let orientation = match r["orientation"].as_str() {
                        Some("Right") => SplitOrientation::Right,
                        Some("Below") => SplitOrientation::Below,
                        _ => return Err(fail("invalid_request", "Invalid split orientation")),
                    };
                    layout
                        .split(
                            target,
                            session,
                            orientation,
                            operation,
                            w.owner.runtime.clone(),
                        )
                        .map_err(error)?;
                } else {
                    layout
                        .new_tab(
                            TerminalTabID(new_id().map_err(error)?),
                            session,
                            operation,
                            w.owner.runtime.clone(),
                        )
                        .map_err(error)?;
                }
                let directory = directory.unwrap_or_else(|| w.record.locator.root.clone());
                let shell =
                    TargetPath::posix(r["shell"].as_str().unwrap_or("/bin/sh").as_bytes().to_vec())
                        .map_err(error)?;
                let request = TerminalOpenRequest {
                    operation,
                    session,
                    owner: w.owner.clone(),
                    target: w.record.target.clone(),
                    runtime: w.owner.runtime.clone(),
                    directory: directory.clone(),
                    shell: shell.clone(),
                    size: size(r)?,
                };
                w.record.terminal_layout = layout;
                w.record.terminal_starts.push(TerminalStart {
                    session,
                    directory,
                    shell,
                });
                w.advance_generation()?;
                (w.service.clone(), request)
            };
            let pane = Arc::new(TerminalPane {
                workspace: workspace.clone(),
                session,
                stream: Mutex::new(Output::default()),
            });
            self.terminals
                .lock()
                .unwrap()
                .sessions
                .insert(id(session), pane);
            let expected_runtime = request.runtime.clone();
            let receipt = run(service.open(request));
            if !matches!(&receipt.state,OperationState::Confirmed(MutationResult::TerminalOpened {session: opened,runtime}) if *opened==session && *runtime==expected_runtime)
            {
                return Err(fail(
                    "terminal_uncertain",
                    format!(
                        "Terminal {} creation requires inspection: {:?}; it will not be restarted",
                        id(session),
                        receipt.state
                    ),
                ));
            }
            self.terminal_call(
                route,
                &json!({"command":"terminal.attach","id":id(session),"sequence":0}),
            )?;
            return Ok(json!({"id":id(session),"layout":layout_value(&workspace.lock().unwrap())}));
        }
        let pane = self
            .terminals
            .lock()
            .unwrap()
            .sessions
            .get(string(r, "id")?)
            .cloned()
            .ok_or_else(|| {
                fail(
                    "not_found",
                    "Unknown terminal; restore its verified workspace first",
                )
            })?;
        let (service, owner, runtime) = {
            let w = pane.workspace.lock().unwrap();
            (w.service.clone(), w.owner.clone(), w.owner.runtime.clone())
        };
        // A route handle is transport state, never permission to address another workspace.
        if let Some(route) = route {
            if self.route(route)?.lease() != &owner {
                return Err(fail("wrong_workspace", "Terminal owner differs"));
            }
        } else if owner.runtime
            != self
                .services
                .local_connection()
                .map_err(|e| fail("unavailable", e))?
                .runtime
        {
            return Err(fail("wrong_workspace", "Terminal is not local"));
        }
        if matches!(
            command,
            "terminal.attach"
                | "terminal.read"
                | "terminal.write"
                | "terminal.resize"
                | "terminal.close"
        ) {
            let w = pane.workspace.lock().unwrap();
            if w.record
                .terminal_layout
                .runtimes
                .iter()
                .find(|(id, _, _)| *id == pane.session)
                .is_none_or(|(_, _, original)| original != &runtime)
            {
                return Err(fail(
                    "runtime_lost",
                    "The original terminal runtime is unavailable; start a new shell explicitly",
                ));
            }
        }
        match command {
            "terminal.attach" => {
                let mut output = pane.stream.lock().unwrap();
                let sequence = r
                    .get("sequence")
                    .map(|_| number(r, "sequence"))
                    .transpose()?
                    .unwrap_or(output.sequence);
                output.stream = Some(
                    run(service.attach(TerminalAttachRequest {
                        session: pane.session,
                        owner,
                        runtime,
                        last_output_sequence: sequence,
                    }))
                    .map_err(error)?,
                );
                output.sequence = sequence;
                output.drained = false;
                Ok(Value::Null)
            }
            "terminal.read" => {
                let mut output = pane.stream.lock().unwrap();
                let mut events = vec![];
                let mut bytes = vec![];
                let mut truncated = false;
                for _ in 0..16 {
                    let Some(stream) = output.stream.as_mut() else {
                        break;
                    };
                    match stream
                        .as_mut()
                        .poll_next(&mut PollContext::from_waker(Waker::noop()))
                    {
                        Poll::Pending => break,
                        Poll::Ready(None) => {
                            output.drained = true;
                            break;
                        }
                        Poll::Ready(Some(event)) => {
                            let event = event.map_err(error)?;
                            match &event {
                                TerminalEvent::Output {
                                    sequence,
                                    bytes: data,
                                } => {
                                    output.sequence = *sequence;
                                    bytes.extend_from_slice(data);
                                }
                                TerminalEvent::Gap { last, .. } => {
                                    output.sequence = *last;
                                    truncated = true;
                                }
                                TerminalEvent::ProcessExit { .. }
                                | TerminalEvent::Ended { .. }
                                | TerminalEvent::Expired
                                | TerminalEvent::RuntimeLost => output.drained = true,
                                _ => {}
                            }
                            events.push(event);
                        }
                    }
                }
                Ok(
                    json!({"bytes":bytes,"events":events,"sequence":output.sequence,"truncated":truncated,"drained":output.drained,"alive":!output.drained}),
                )
            }
            "terminal.write" => {
                let bytes = if let Some(text) = r["text"].as_str() {
                    text.as_bytes().to_vec()
                } else {
                    serde_json::from_value::<Vec<u8>>(r["bytes"].clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))?
                };
                if bytes.len() > 65536 {
                    return Err(fail("too_large", "Terminal input chunk exceeds 64 KiB"));
                }
                run(service.input(pane.session, bytes)).map_err(error)?;
                Ok(Value::Null)
            }
            "terminal.resize" => {
                run(service.resize(pane.session, size(r)?)).map_err(error)?;
                Ok(Value::Null)
            }
            "terminal.focus" => {
                let mut w = pane.workspace.lock().unwrap();
                w.record
                    .terminal_layout
                    .focus(pane.session)
                    .map_err(error)?;
                w.advance_generation()?;
                Ok(layout_value(&w))
            }
            "terminal.layout.resize" => {
                let tab = serde_json::from_value(r["tab"].clone())
                    .map_err(|e| fail("invalid_request", e.to_string()))?;
                let depth = u32::try_from(number(r, "depth")?)
                    .map_err(|_| fail("invalid_request", "Invalid split depth"))?;
                let ratio = r["ratio"]
                    .as_f64()
                    .ok_or_else(|| fail("invalid_request", "Invalid split ratio"))?;
                let mut w = pane.workspace.lock().unwrap();
                w.record
                    .terminal_layout
                    .resize_at(tab, number(r, "path")?, depth, ratio)
                    .map_err(error)?;
                w.advance_generation()?;
                Ok(layout_value(&w))
            }
            "terminal.close" | "terminal.layout.close" => {
                // Ending is explicit; hide and detach never call this operation.
                if command == "terminal.close" {
                    run(service.end(pane.session, TerminalEndIntent::ExplicitClose))
                        .map_err(error)?;
                }
                let mut w = pane.workspace.lock().unwrap();
                w.record
                    .terminal_layout
                    .close(pane.session)
                    .map_err(error)?;
                w.record
                    .terminal_starts
                    .retain(|s| s.session != pane.session);
                w.advance_generation()?;
                let layout = layout_value(&w);
                drop(w);
                self.terminals
                    .lock()
                    .unwrap()
                    .sessions
                    .remove(string(r, "id")?);
                Ok(layout)
            }
            _ => Err(fail("invalid_request", "Unknown terminal operation")),
        }
    }
}

#[cfg(test)]
mod detached_tests {
    use super::*;
    fn directory_engine() -> (tempfile::TempDir, crate::Engine) {
        let state = tempfile::tempdir().unwrap();
        let services = Arc::new(Services::with_state_directory(state.path().to_owned()));
        let mut engine = crate::Engine::new();
        engine.services = services.clone();
        engine.runtime = Arc::new(Runtime::new(services, engine.documents.clone()));
        (state, engine)
    }

    #[test]
    fn terminal_start_directory_defaults_to_workspace_root() {
        let (_state, engine) = directory_engine();
        let root = tempfile::tempdir().unwrap();
        let request = json!({"command":"terminal.start","root":root.path(),"shell":"/bin/sh"});
        let started = engine.call(request.clone());
        assert_eq!(started["ok"], true, "{started}");
        let workspace = engine.runtime.terminal_workspace(None, &request).unwrap();
        let w = workspace.lock().unwrap();
        assert_eq!(w.record.terminal_starts[0].directory, w.record.locator.root);
        drop(w);
        assert_eq!(
            engine.call(json!({"command":"workspace.shutdown"}))["ok"],
            true
        );
    }

    #[test]
    fn terminal_start_directory_preserves_workspace_identity() {
        let (_state, engine) = directory_engine();
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("nested folder/日本語");
        std::fs::create_dir_all(&directory).unwrap();
        let request = json!({"command":"terminal.start","root":root.path(),"directory":directory,"shell":"/bin/sh"});
        let workspace = engine.runtime.terminal_workspace(None, &request).unwrap();
        let (before, owner) = {
            let w = workspace.lock().unwrap();
            (w.record.clone(), w.owner.clone())
        };
        let started = engine.call(request);
        assert_eq!(started["ok"], true, "{started}");
        let w = workspace.lock().unwrap();
        assert_eq!(w.record.id, before.id);
        assert_eq!(w.record.locator, before.locator);
        assert_eq!(w.record.target, before.target);
        assert_eq!(w.owner, owner);
        assert_eq!(
            w.record.terminal_starts[0].directory,
            TargetPath::posix(directory.to_str().unwrap().as_bytes().to_vec()).unwrap()
        );
        drop(w);
        assert_eq!(
            engine.call(json!({"command":"workspace.shutdown"}))["ok"],
            true
        );
    }

    #[test]
    fn terminal_start_rejects_invalid_directory() {
        let (_state, engine) = directory_engine();
        let root = tempfile::tempdir().unwrap();
        let request = json!({"command":"terminal.layout","root":root.path()});
        let before = engine.call(request.clone());
        assert_eq!(before["ok"], true, "{before}");
        for directory in [
            Value::Null,
            json!(7),
            json!(false),
            json!([]),
            json!({}),
            json!(""),
            json!("relative/path"),
            json!("/bad\0path"),
        ] {
            let result = engine
                .call(json!({"command":"terminal.start","root":root.path(),"directory":directory}));
            assert_eq!(result["ok"], false, "accepted {directory}: {result}");
            assert_eq!(
                engine.call(request.clone()),
                before,
                "invalid directory changed layout"
            );
        }
    }

    #[test]
    fn detached_context_rejects_changed_identity_and_expiry() {
        for change in 0..6 {
            let state = tempfile::tempdir().unwrap();
            let root = tempfile::tempdir().unwrap();
            let services = Arc::new(Services::with_state_directory(state.path().to_owned()));
            let mut engine = crate::Engine::new();
            engine.services = services.clone();
            engine.runtime = Arc::new(Runtime::new(services, engine.documents.clone()));
            let started = engine
                .call(json!({"command":"terminal.start","root":root.path(),"shell":"/bin/sh"}));
            assert_eq!(started["ok"], true, "{started}");
            {
                let mut terminals = engine.runtime.terminals.lock().unwrap();
                terminals.detach("local", true);
                let saved = terminals.detached.values_mut().next().unwrap();
                match change {
                    0 => saved.target.security_binding.push(1),
                    1 => saved.owner.client = ClientID([1; 16]),
                    2 => saved.owner.workspace = WorkspaceID([2; 16]),
                    3 => saved.owner.lease = WorkspaceLeaseID([3; 16]),
                    4 => saved.owner.runtime.supervisor_boot = [4; 16],
                    _ => {
                        saved.expires =
                            std::time::Instant::now() - std::time::Duration::from_secs(1)
                    }
                }
            }
            let layout = engine.call(json!({"command":"terminal.layout","root":root.path()}));
            assert_eq!(layout["ok"], true);
            assert!(
                layout["value"]["tabs"].as_array().unwrap().is_empty(),
                "accepted foreign/expired context {change}"
            );
            let attach = engine.call(
                json!({"command":"terminal.attach","id":started["value"]["id"],"sequence":0}),
            );
            assert_eq!(
                attach["ok"], false,
                "attached foreign/expired context {change}"
            );
            assert_eq!(
                engine.call(json!({"command":"workspace.shutdown"}))["ok"],
                true
            );
        }
    }
}
