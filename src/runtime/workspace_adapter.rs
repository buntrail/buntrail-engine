use super::*;
use buntrail_core::{contracts::EditorViewport, workspaces::*};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: u64,
    viewport: Option<EditorViewport>,
}
impl Runtime {
    pub(crate) fn verified_target(&self, id: &str) -> Result<TargetIdentity> {
        let route = self.route(id)?;
        route.validate_environment_ready().map_err(error)?;
        Ok(route.target().clone())
    }
    pub(super) fn workspace_call(&self, route: Option<&str>, r: &Value) -> Result<Value> {
        let command = string(r, "command")?;
        if command == "workspace.shutdown" {
            if route.is_some() {
                return Err(fail(
                    "invalid_request",
                    "Detach remote workspace views explicitly",
                ));
            }
            self.services
                .shutdown_local()
                .map_err(|e| fail("busy", e))?;
            return Ok(json!({"closed":true}));
        }
        let local = self
            .services
            .local_connection()
            .map_err(|e| fail("unavailable", e))?;
        let remote = route.map(|id| self.route(id)).transpose()?;
        if let Some(remote) = &remote {
            remote.validate_environment_ready().map_err(error)?;
        }
        let target = remote
            .as_ref()
            .map_or_else(|| local.identity.clone(), |s| s.target().clone());
        let capabilities = json!({"files":true,"search":true,"git":remote.is_some()||local.git_available,"terminal":true,"sessions":true,"owned_terminals":true,"encoded_documents":true});
        if command == "capabilities" {
            return Ok(capabilities);
        }
        if let Some(expected) = r.get("expected").filter(|v| !v.is_null()) {
            let expected: TargetIdentity = serde_json::from_value(expected.clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?;
            if expected != target {
                return Err(fail(
                    "wrong_target",
                    "Saved workspace target differs from the verified environment",
                ));
            }
        }
        if command == "workspace.welcome_list" {
            let path: TargetPath = serde_json::from_value(r["path"].clone())
                .map_err(|_| fail("invalid_request", "Invalid directory path"))?;
            let file = FileRef {
                target,
                path,
                resolved_identity: None,
            };
            let resolution = match &remote {
                Some(s) => run(s.resolve(file, FileResolvePurpose::Root)),
                None => local.files.resolve_sync(file, FileResolvePurpose::Root),
            }
            .map_err(error)?;
            let entries = match &remote {
                Some(s) => run(s.list(resolution.root)),
                None => local.files.list_sync(resolution.root),
            }
            .map_err(error)?;
            let mut rows: Vec<_> = entries
                .into_iter()
                .filter(|e| e.kind == FileKind::Directory)
                .map(|e| json!({"name":e.display_name,"path":e.file.path}))
                .collect();
            rows.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            return Ok(json!({"path":resolution.resolved_path,"entries":rows}));
        }
        if command == "workspace.welcome_folder" {
            use std::os::unix::ffi::OsStrExt;
            let parent: TargetPath = if let Some(path) = r.get("parent_path") {
                serde_json::from_value(path.clone())
                    .map_err(|_| fail("invalid_request", "Invalid parent directory"))?
            } else {
                TargetPath::posix(string(r, "parent")?.as_bytes().to_vec()).map_err(error)?
            };
            let TargetPath::PosixBytes(bytes) = &parent else {
                return Err(fail("invalid_request", "Choose a POSIX directory"));
            };
            TargetPath::posix(bytes.clone()).map_err(error)?;
            let name = r.get("name").map(|_| string(r, "name")).transpose()?;
            let repository = r
                .get("repository")
                .map(|_| string(r, "repository"))
                .transpose()?;
            let path = if let Some(remote) = remote {
                let file = FileRef {
                    target,
                    path: parent,
                    resolved_identity: None,
                };
                remote
                    .welcome_folder(
                        file,
                        name.map(str::to_owned),
                        repository.map(str::to_owned),
                        OperationID(new_id().map_err(error)?),
                    )
                    .map_err(error)?
            } else {
                let child = buntrail_services::welcome_folder(
                    std::path::Path::new(std::ffi::OsStr::from_bytes(bytes)),
                    name,
                    repository,
                )
                .map_err(|e| fail("service_error", e))?;
                TargetPath::posix(child.as_os_str().as_encoded_bytes().to_vec()).map_err(error)?
            };
            return Ok(json!({"path":path}));
        }
        let catalog = local.store.load().map_err(error)?;
        if command == "workspace.catalog" {
            return Ok(json!({"client":catalog.client,"records":catalog.records}));
        }
        let saved = r
            .get("workspace")
            .map(|id| {
                serde_json::from_value::<WorkspaceID>(id.clone())
                    .map_err(|e| fail("invalid_request", e.to_string()))
            })
            .transpose()?
            .map(|id| {
                catalog
                    .records
                    .iter()
                    .find(|w| w.id == id)
                    .cloned()
                    .ok_or_else(|| fail("not_found", "Saved workspace not found"))
            })
            .transpose()?;
        let root = if let Some(saved) = &saved {
            if saved.target != target
                || (route.is_none() && saved.locator.environment.route != RouteKind::Local)
            {
                return Err(fail(
                    "wrong_target",
                    "Saved workspace requires its verified target",
                ));
            }
            files_adapter::text_path(&saved.locator.root)?
        } else {
            string(r, "root")?.to_owned()
        };
        let file = FileRef {
            target: target.clone(),
            path: TargetPath::posix(root.as_bytes().to_vec()).map_err(error)?,
            resolved_identity: None,
        };
        let resolved = match &remote {
            Some(s) => run(s.resolve(file, FileResolvePurpose::Root)),
            None => local.files.resolve_sync(file, FileResolvePurpose::Root),
        };
        let resolution = match resolved {
            Ok(value) if value.kind == Some(FileKind::Directory) => value,
            _ if saved.is_some() => {
                return Ok(
                    json!({"state":"Missing","record":saved.unwrap(),"capabilities":capabilities}),
                );
            }
            Ok(_) => return Err(fail("not_found", "Workspace root is not a directory")),
            Err(e) => return Err(error(e)),
        };
        if command == "workspace.open" && r["welcome"] == true {
            let mut directory = resolution.root.clone();
            if let TargetPath::PosixBytes(bytes) = &mut directory.path {
                bytes.extend_from_slice(b"/.");
            }
            match &remote {
                Some(s) => run(s.list(directory.clone())),
                None => local.files.list_sync(directory),
            }
            .map_err(error)?;
        }
        let root = files_adapter::text_path(&resolution.resolved_path)?;
        if command == "workspace.open"
            && r["welcome"] == true
            && let Some(remote) = &remote
            && let Some(existing) = catalog.records.iter().find(|record| {
                record.target == target && record.locator.root == resolution.resolved_path
            })
            && existing.id != remote.lease().workspace
        {
            return Ok(json!({"state":"Reconnect","workspace":existing.id}));
        }
        let workspace = self.terminal_workspace(route, &json!({"root":root}))?;
        if let Some(saved) = &saved
            && workspace.lock().unwrap().record.id != saved.id
        {
            return Err(fail(
                "wrong_workspace",
                "Reconnect the saved workspace registration before restoring it",
            ));
        }
        if command == "workspace.open" {
            let w = workspace.lock().unwrap();
            let discovery = match &remote {
                Some(s) => run(s.discover(resolution.root)),
                None => local
                    .git
                    .discover_sync(resolution.root, &std::sync::atomic::AtomicBool::new(false)),
            };
            let (worktree, git_error) = match discovery {
                Ok(worktree) => (worktree, None),
                Err(e) => (None, Some(e.message)),
            };
            return Ok(
                json!({"state":"Ready","record":w.record,"runtime":w.owner.runtime,"capabilities":capabilities,"worktree":worktree,"git_error":git_error,"preferences":local.preferences.effective(w.record.id).map_err(error)?}),
            );
        }
        if command == "preferences.get" {
            return Ok(json!(
                local
                    .preferences
                    .effective(workspace.lock().unwrap().record.id)
                    .map_err(error)?
            ));
        }
        if command == "preferences.set" {
            let value: buntrail_core::preferences::PreferencesOverride =
                serde_json::from_value(r["override"].clone())
                    .map_err(|e| fail("invalid_request", e.to_string()))?;
            let id = workspace.lock().unwrap().record.id;
            local.preferences.set_override(id, value).map_err(error)?;
            return Ok(json!(local.preferences.effective(id).map_err(error)?));
        }
        if command != "workspace.snapshot" {
            return Err(fail("invalid_request", "Unknown workspace operation"));
        }
        let _admission = self.document_admission()?;
        let entries: Vec<Entry> = serde_json::from_value(r["documents"].clone())
            .map_err(|e| fail("invalid_request", e.to_string()))?;
        if entries.len() > 64 {
            return Err(fail("too_large", "Workspace document limit is 64"));
        }
        let selected = r
            .get("selected")
            .filter(|v| !v.is_null())
            .map(|_| number(r, "selected"))
            .transpose()?;
        let mut w = workspace.lock().unwrap();
        let mut documents = self
            .documents
            .lock()
            .map_err(|_| fail("engine_failed", "Document registry poisoned"))?;
        let mut seen = std::collections::HashSet::new();
        let mut saved_documents = vec![];
        let mut saved_selection = None;
        for entry in entries {
            if !seen.insert(entry.id) {
                return Err(fail("invalid_request", "Duplicate workspace document"));
            }
            let Some(document) = documents.open.get_mut(&entry.id) else {
                continue;
            };
            if document.remote.as_deref() != route {
                return Err(fail(
                    "wrong_target",
                    "Document belongs to another environment view",
                ));
            }
            let Some(token) = document
                .version
                .as_deref()
                .filter(|v| v.starts_with("bt2:"))
            else {
                continue;
            };
            let binding = Services::file_binding(token).map_err(|e| fail("invalid_request", e))?;
            if binding.file.target != target {
                return Err(fail(
                    "wrong_target",
                    "Document target differs from workspace",
                ));
            }
            let persistent = match document.persistent_id {
                Some(id) => id,
                None => {
                    let id = DocumentID(new_id().map_err(error)?);
                    document.persistent_id = Some(id);
                    id
                }
            };
            let viewport = entry.viewport.filter(|v| {
                if document.saving
                    || document.pending_save.is_some()
                    || document.buffer.checkpoint() != document.saved_state
                    || v.revision_anchor.0 != document.buffer.revision()
                    || v.selection.revision != v.revision_anchor
                    || !v.horizontal_offset.is_finite()
                    || !v.vertical_offset.is_finite()
                    || !(0.0..=1e9).contains(&v.horizontal_offset)
                    || !(0.0..=1e9).contains(&v.vertical_offset)
                    || v.folded_ranges.len() > 10000
                {
                    return false;
                }
                let text = document.buffer.snapshot().text;
                v.selection.start <= v.selection.end
                    && usize::try_from(v.selection.start)
                        .ok()
                        .is_some_and(|p| text.utf16_to_char(p).is_ok())
                    && usize::try_from(v.selection.end)
                        .ok()
                        .is_some_and(|p| text.utf16_to_char(p).is_ok())
            });
            saved_documents.push(WorkspaceDocument {
                id: persistent,
                file: binding.file,
                version: Some(binding.version),
                viewport,
            });
            if selected == Some(entry.id) {
                saved_selection = Some(persistent);
            }
        }
        drop(documents);
        w.record.documents = saved_documents;
        w.record.selected = saved_selection;
        let record = &mut w.record;
        for (key, destination) in [
            ("sidebar_visible", &mut record.sidebar_visible),
            ("terminal_visible", &mut record.terminal_visible),
        ] {
            if let Some(value) = r.get(key) {
                *destination = value
                    .as_bool()
                    .ok_or_else(|| fail("invalid_request", format!("{key} must be a boolean")))?;
            }
        }
        if let Some(height) = r.get("terminal_height") {
            let height = height
                .as_f64()
                .filter(|h| h.is_finite() && (40.0..=10000.0).contains(h))
                .ok_or_else(|| fail("invalid_request", "Invalid terminal panel height"))?;
            w.record.terminal_height = Some(height);
        }
        self.persist_workspace(&mut w)?;
        Ok(json!({"record":w.record}))
    }
}
