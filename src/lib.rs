//! Authoritative document and workspace orchestration.
//!
//! `terminal.start` accepts an optional `directory` JSON string: an absolute,
//! NUL-free UTF-8 launch path on the selected target. Omission uses the verified
//! workspace `root`. The directory does not change workspace identity, ownership,
//! lease or runtime. Malformed supplied values (including null) are errors before
//! terminal layout changes. Launch failures retain existing uncertain-operation
//! handling; callers must inspect rather than retry uncertain starts.
//!
//! See the repository README for usage, prerequisites and platform limitations.
//! Authoritative documents with worker-routed local and remote services.
mod document;
#[cfg(unix)]
pub mod environment;
mod jobs;
mod language;
#[cfg(unix)]
mod recovery;
#[cfg(unix)]
pub mod remote;
#[cfg(all(unix, test))]
#[path = "test_support/remote.rs"]
mod remote_test_support;
#[cfg(unix)]
mod runtime;
#[cfg(unix)]
mod save;
#[cfg(unix)]
mod worker;
use buntrail_core::{
    Snapshot,
    contracts::{BufferRange, EditResult, EditTransaction},
};
use buntrail_services::Services;
use document::Document;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub struct Failure {
    pub code: &'static str,
    pub message: String,
}
type Result<T> = std::result::Result<T, Failure>;
fn fail(code: &'static str, message: impl Into<String>) -> Failure {
    Failure {
        code,
        message: message.into(),
    }
}
fn string<'a>(r: &'a Value, key: &str) -> Result<&'a str> {
    r.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| fail("invalid_request", format!("{key} must be a string")))
}
fn number(r: &Value, key: &str) -> Result<u64> {
    r.get(key).and_then(Value::as_u64).ok_or_else(|| {
        fail(
            "invalid_request",
            format!("{key} must be a nonnegative integer"),
        )
    })
}

#[derive(Default)]
struct Documents {
    next_id: u64,
    open: HashMap<u64, Document>,
}

struct AnalysisInput {
    id: u64,
    snapshot: Snapshot,
    association: u64,
    path: Option<String>,
    language: String,
}
pub struct Engine {
    #[cfg(unix)]
    runtime: Arc<runtime::Runtime>,
    jobs: jobs::Jobs,
    documents: Arc<Mutex<Documents>>,
    services: Arc<Services>,
    language: buntrail_language::LanguageAdapter,
}
impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
impl Engine {
    pub fn new() -> Self {
        let services = Arc::new(Services::new());
        let documents = Arc::new(Mutex::new(Documents::default()));
        Self {
            #[cfg(unix)]
            runtime: Arc::new(runtime::Runtime::new(services.clone(), documents.clone())),
            jobs: jobs::Jobs::default(),
            documents,
            services,
            language: buntrail_language::LanguageAdapter::default(),
        }
    }
    fn documents(&self) -> Result<MutexGuard<'_, Documents>> {
        self.documents.lock().map_err(|_| {
            fail(
                "engine_failed",
                "Engine state is poisoned; preserve recovery data and reopen",
            )
        })
    }
    pub fn call(&self, request: Value) -> Value {
        match self.dispatch(&request) {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(error) => json!({"ok":false,"error":{"code":error.code,"message":error.message}}),
        }
    }
    fn dispatch(&self, r: &Value) -> Result<Value> {
        let command = string(r, "command")?;
        #[cfg(unix)]
        let _admission = if command.starts_with("document.") {
            Some(self.runtime.document_admission()?)
        } else {
            None
        };
        #[cfg(unix)]
        if command.starts_with("remote.")
            || command.starts_with("terminal.")
            || command == "search"
            || command.starts_with("git")
            || command.starts_with("fs.")
            || command.starts_with("workspace.")
            || command.starts_with("preferences.")
            || command == "capabilities"
        {
            return self.runtime.call(r);
        }
        match command {
            "service.start" => {
                let request = r
                    .get("request")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| fail("invalid_request", "request must be an object"))?
                    .clone();
                let operation = string(&request, "command")?;
                if operation.starts_with("document.") || operation.starts_with("service.") {
                    return Err(fail(
                        "invalid_request",
                        "Jobs accept service operations only",
                    ));
                }
                let services = self.services.clone();
                #[cfg(unix)]
                let runtime = self.runtime.clone();
                let id = self.jobs.start(move |job| {
                    #[cfg(unix)]
                    if runtime.stream_job(&request, job)? {
                        return Ok(());
                    }
                    let value = job.run(Box::pin(async {
                        #[cfg(unix)]
                        if request["command"].as_str().is_some_and(|c| {
                            c.starts_with("remote.")
                                || c.starts_with("terminal.")
                                || c == "search"
                                || c.starts_with("git")
                                || c.starts_with("fs.")
                                || c.starts_with("workspace.")
                                || c.starts_with("preferences.")
                                || c == "capabilities"
                        }) {
                            return runtime
                                .call_with_cancellation(&request, Some(job.cancellation()));
                        }
                        services
                            .call(&request)
                            .map_err(|e| fail("service_error", e))
                    }))?;
                    job.emit(value)
                })?;
                return Ok(json!({"id":id}));
            }
            "service.poll" => return self.jobs.get(number(r, "id")?)?.poll(),
            "service.cancel" => {
                self.jobs.get(number(r, "id")?)?.cancel()?;
                return Ok(Value::Null);
            }
            "service.release" => {
                self.jobs.release(number(r, "id")?)?;
                return Ok(Value::Null);
            }
            _ => {}
        }
        #[cfg(unix)]
        match command {
            "document.recovery" => {
                return self.checkpoint_recovery(
                    number(r, "id")?,
                    r.get("expected_owner")
                        .map(|_| number(r, "expected_owner"))
                        .transpose()?,
                );
            }
            "document.discard_recovery" => return self.discard_document_recovery(number(r, "id")?),
            "document.inspect_save" => return self.inspect_save(number(r, "id")?),
            "document.close" => return self.close_document(number(r, "id")?, r["discard"] == true),
            _ => {}
        }
        if command == "document.save" {
            return self.save(number(r, "id")?);
        }
        if command == "document.analyze" || command == "document.editor_analyze" {
            let id = number(r, "id")?;
            let requested = if command == "document.analyze" {
                Some(string(r, "language")?)
            } else {
                None
            };
            let input = self.capture_analysis(id, requested)?;
            let result =
                language::analyze(&self.language, id, input.snapshot.clone(), &input.language)?;
            self.validate_analysis(&input)?;
            return Ok(result);
        }
        if !command.starts_with("document.") {
            return self
                .services
                .call(r)
                .map_err(|message| fail("service_error", message));
        }
        let mut documents = self.documents()?;
        if command == "document.open" {
            let text = string(r, "text")?;
            if text.len() > 64 * 1024 * 1024 {
                return Err(fail("too_large", "Document exceeds 64 MiB"));
            }
            let path = r
                .get("path")
                .filter(|p| !p.is_null())
                .map(|_| string(r, "path").map(str::to_owned))
                .transpose()?;
            let remote = r
                .get("remote")
                .filter(|v| !v.is_null())
                .map(|_| string(r, "remote").map(str::to_owned))
                .transpose()?;
            let version = r
                .get("version")
                .filter(|v| !v.is_null())
                .map(|_| string(r, "version").map(str::to_owned))
                .transpose()?;
            if path.is_some() && version.is_none() {
                return Err(fail(
                    "invalid_request",
                    "A bound document requires its observed file version",
                ));
            }
            let persistent = r
                .get("persistent_id")
                .filter(|v| !v.is_null())
                .map(|v| {
                    serde_json::from_value::<buntrail_core::contracts::DocumentID>(v.clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))
                })
                .transpose()?;
            if persistent.is_some()
                && documents
                    .open
                    .values()
                    .any(|d| d.persistent_id == persistent)
            {
                return Err(fail("already_open", "Persistent document is already open"));
            }
            let id = documents
                .next_id
                .checked_add(1)
                .ok_or_else(|| fail("exhausted", "Document IDs exhausted"))?;
            documents.next_id = id;
            let mut document = Document::new(text, path, version, remote);
            if let Some(source) = r.get("encoding_source").filter(|v| !v.is_null()) {
                let source = source
                    .as_str()
                    .ok_or_else(|| fail("invalid_request", "encoding_source must be a string"))?;
                #[cfg(unix)]
                {
                    Services::file_binding(source).map_err(|e| fail("invalid_request", e))?;
                    document.encoding_source = Some(source.to_owned());
                }
                #[cfg(not(unix))]
                return Err(fail(
                    "unavailable",
                    "Recovered codec metadata requires typed file services on this platform",
                ));
            }
            document.persistent_id = persistent;
            let response = document.state(id);
            documents.open.insert(id, document);
            return Ok(response);
        }
        let id = number(r, "id")?;
        let document = documents
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
        #[cfg(unix)]
        if matches!(
            command,
            "document.bind" | "document.rebind" | "document.reload"
        ) && (document.closing || document.pending_save.is_some())
        {
            return Err(fail(
                "save_pending",
                "Resolve the pending save or recovery cleanup before changing bindings",
            ));
        }
        match command {
            "document.get" => Ok(document.state(id)),
            #[cfg(unix)]
            "document.reconnect" => {
                if document.saving || document.closing {
                    return Err(fail(
                        "save_pending",
                        "Wait for the active save or cleanup before reconnecting",
                    ));
                }
                if document.remote.as_deref() != Some(string(r, "expected_remote")?) {
                    return Err(fail(
                        "stale_revision",
                        "Document view changed while reconnecting",
                    ));
                }
                let remote = string(r, "remote")?;
                let token = document
                    .version
                    .as_deref()
                    .filter(|v| v.starts_with("bt2:"))
                    .or(document.encoding_source.as_deref())
                    .ok_or_else(|| {
                        fail(
                            "wrong_target",
                            "Reopen legacy documents through the verified environment",
                        )
                    })?;
                let original =
                    Services::file_binding(token).map_err(|e| fail("wrong_target", e))?;
                if original.file.target != self.runtime.verified_target(remote)? {
                    return Err(fail(
                        "wrong_target",
                        "Reconnected target differs from the original file",
                    ));
                }
                document.recovery_owner = document
                    .recovery_owner
                    .checked_add(1)
                    .ok_or_else(|| fail("exhausted", "Recovery ownership exhausted"))?;
                document.remote = Some(remote.to_owned());
                Ok(document.state(id))
            }
            "document.rebind" => {
                if document.saving {
                    return Err(fail("save_pending", "Wait for the pending save"));
                }
                let remote = r
                    .get("remote")
                    .filter(|v| !v.is_null())
                    .map(|_| string(r, "remote"))
                    .transpose()?;
                if document.path.as_deref() != Some(string(r, "expected_path")?)
                    || document.remote.as_deref() != remote
                {
                    return Err(fail(
                        "wrong_file",
                        "The file or environment no longer matches",
                    ));
                }
                #[cfg(unix)]
                let observed = if let Some(observed) = r.get("version").and_then(Value::as_str) {
                    let previous = document
                        .version
                        .as_deref()
                        .ok_or_else(|| fail("wrong_file", "File binding is missing"))?;
                    if string(r, "expected_version")? != previous {
                        return Err(fail("stale_version", "File binding changed while moving"));
                    }
                    Some(
                        Services::rebound_version(previous, observed, string(r, "path")?)
                            .map_err(|message| fail("wrong_file", message))?,
                    )
                } else {
                    None
                };
                #[cfg(unix)]
                self.rotate_recovery(document)?;
                #[cfg(unix)]
                if let Some(observed) = observed {
                    document.version = Some(observed);
                }
                document.path = Some(string(r, "path")?.to_owned());
                Ok(document.state(id))
            }
            "document.status" => Ok(document.metadata(id)),
            "document.reload" => {
                if number(r, "revision")? != document.buffer.revision() {
                    return Err(fail(
                        "stale_revision",
                        "Document changed while reading the file",
                    ));
                }
                if document.saving {
                    return Err(fail("save_pending", "Wait for the pending save"));
                }
                if document.buffer.checkpoint() != document.saved_state {
                    return Err(fail(
                        "dirty_document",
                        "External changes cannot replace unsaved work",
                    ));
                }
                let remote = r
                    .get("remote")
                    .filter(|v| !v.is_null())
                    .map(|_| string(r, "remote"))
                    .transpose()?;
                if document.path.as_deref() != Some(string(r, "path")?)
                    || document.remote.as_deref() != remote
                {
                    return Err(fail(
                        "wrong_file",
                        "The file or environment no longer matches",
                    ));
                }
                if document.version.as_deref() != Some(string(r, "expected_version")?) {
                    return Err(fail(
                        "stale_version",
                        "The file binding changed while reading",
                    ));
                }
                let version = string(r, "version")?.to_owned();
                let text = string(r, "text")?;
                document.edit(&json!({"revision":document.buffer.revision(),"start":0,"end":document.buffer.text().encode_utf16().count(),"text":text}), id)?;
                document.version = Some(version);
                document.saved_state = document.buffer.checkpoint();
                Ok(document.state(id))
            }
            "document.bind" => {
                if document.buffer.has_composition() {
                    return Err(fail("composition", "Resolve composition before binding"));
                }
                if number(r, "revision")? != document.buffer.revision() {
                    return Err(fail(
                        "stale_revision",
                        "Document changed after the file snapshot was created",
                    ));
                }
                if document.saving {
                    return Err(fail("save_pending", "Wait for the pending save"));
                }
                let path = string(r, "path")?.to_owned();
                let version = string(r, "version")?.to_owned();
                let remote = r
                    .get("remote")
                    .filter(|value| !value.is_null())
                    .map(|_| string(r, "remote").map(str::to_owned))
                    .transpose()?;
                #[cfg(unix)]
                self.rotate_recovery(document)?;
                document.buffer.end_typing();
                document.path = Some(path);
                document.version = Some(version);
                document.remote = remote;
                document.saved_state = document.buffer.checkpoint();
                Ok(document.state(id))
            }
            "document.edit" => document.edit(r, id),
            "document.undo" => document.history(id, true),
            "document.redo" => document.history(id, false),
            #[cfg(not(unix))]
            "document.close" => {
                if document.saving {
                    return Err(fail("save_pending", "Wait for the pending save"));
                }
                if document.buffer.checkpoint() != document.saved_state
                    && r.get("discard").and_then(Value::as_bool) != Some(true)
                {
                    return Err(fail(
                        "dirty_document",
                        "Save changes or explicitly discard before closing",
                    ));
                }
                documents.open.remove(&id);
                Ok(Value::Null)
            }
            _ => Err(fail(
                "unknown_command",
                format!("Unknown command: {command}"),
            )),
        }
    }
    fn capture_analysis(&self, id: u64, requested: Option<&str>) -> Result<AnalysisInput> {
        let (snapshot, association, path, override_id) = {
            let documents = self.documents()?;
            let document = documents
                .open
                .get(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            (
                document.buffer.snapshot(),
                document.language_generation,
                document.path.clone(),
                document.language.clone(),
            )
        };
        let language = if let Some(requested) = requested {
            requested.to_owned()
        } else {
            if snapshot.text.len() > 1024 * 1024 {
                return Err(fail(
                    "analysis_limit",
                    "Structural analysis is disabled above 1 MiB; text remains editable",
                ));
            }
            let first_line = snapshot
                .text
                .lines()
                .next()
                .map(|line| line.to_string())
                .unwrap_or_default();
            buntrail_language::registry::select(
                path.as_deref().unwrap_or(""),
                &first_line,
                override_id.as_deref(),
            )
            .into()
        };
        Ok(AnalysisInput {
            id,
            snapshot,
            association,
            path,
            language,
        })
    }
    fn validate_analysis(&self, input: &AnalysisInput) -> Result<()> {
        let documents = self.documents()?;
        let current = documents
            .open
            .get(&input.id)
            .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
        if current.buffer.revision() != input.snapshot.revision
            || current.language_generation != input.association
            || current.path != input.path
        {
            return Err(fail(
                "stale_revision",
                "Document or language changed during analysis",
            ));
        }
        Ok(())
    }
    pub fn set_language(&self, id: u64, language: Option<String>) -> Result<()> {
        if let Some(name) = &language {
            language::canonical(name)?;
        }
        let mut documents = self.documents()?;
        let document = documents
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
        document.language = language;
        document.language_generation = document
            .language_generation
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Language generation exhausted"))?;
        Ok(())
    }
    pub fn snapshot(&self, id: u64) -> Result<Snapshot> {
        self.documents()?
            .open
            .get(&id)
            .map(|d| d.buffer.snapshot())
            .ok_or_else(|| fail("not_found", "Document is no longer open"))
    }
    pub fn transaction(
        &self,
        id: u64,
        transaction: EditTransaction,
        selection: Option<BufferRange>,
        typing: u32,
    ) -> Result<EditResult> {
        self.documents()?
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document is no longer open"))?
            .transaction(transaction, selection, typing)
    }
    #[cfg(not(unix))]
    fn save(&self, id: u64) -> Result<Value> {
        self.save_with(id, |write, remote| {
            match remote {
                Some(remote) => self
                    .services
                    .call(&json!({"command":"remote.call","id":remote,"request":write})),
                None => self.services.call(&write),
            }
            .map_err(|message| fail("save_failed", message))
        })
    }
    #[cfg(not(unix))]
    fn save_with(
        &self,
        id: u64,
        writer: impl FnOnce(Value, Option<String>) -> Result<Value>,
    ) -> Result<Value> {
        let (path, version, remote, text, state) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if document.saving {
                return Err(fail("save_pending", "A save is already in progress"));
            }
            let path = document
                .path
                .clone()
                .ok_or_else(|| fail("unbound_document", "Choose a file before saving"))?;
            let version = document
                .version
                .clone()
                .ok_or_else(|| fail("invalid_request", "Observed file version is missing"))?;
            if document.buffer.has_composition() {
                return Err(fail("composition", "Resolve composition before saving"));
            }
            document.buffer.end_typing();
            document.saving = true;
            (
                path,
                version,
                document.remote.clone(),
                document
                    .buffer
                    .confirmed_snapshot()
                    .map_err(document::core_error)?
                    .text,
                document.buffer.checkpoint(),
            )
        };
        // The document mutex is released before filesystem work so typing never waits on disk.
        let write =
            json!({"command":"fs.write","path":path,"version":version,"text":text.to_string()});
        let response = writer(write, remote);
        let mut documents = self.documents()?;
        let document = documents
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document closed during save"))?;
        document.saving = false;
        let result = response?;
        document.version = Some(string(&result, "version")?.to_owned());
        document.saved_state = state;
        Ok(document.state(id))
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[test]
    fn captured_analysis_rejects_changed_revision_language_and_closed_document() {
        let engine = Engine::new();
        let opened = engine.call(json!({"command":"document.open","text":"function hello() {}"}));
        let id = opened["value"]["id"].as_u64().unwrap();
        let input = engine.capture_analysis(id, Some("js")).unwrap();
        engine.set_language(id, Some("typescript".into())).unwrap();
        assert_eq!(
            engine.validate_analysis(&input).unwrap_err().code,
            "stale_revision"
        );
        let input = engine.capture_analysis(id, Some("js")).unwrap();
        engine.call(
            json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":0,"text":"//\n"}),
        );
        assert_eq!(
            engine.validate_analysis(&input).unwrap_err().code,
            "stale_revision"
        );
        let input = engine.capture_analysis(id, Some("js")).unwrap();
        engine.call(json!({"command":"document.close","id":id,"discard":true}));
        assert_eq!(
            engine.validate_analysis(&input).unwrap_err().code,
            "not_found"
        );
    }
}

#[cfg(test)]
mod concurrent_save_tests {
    use super::*;
    #[test]
    fn edit_between_capture_and_real_write_saves_only_submitted_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("concurrent.txt");
        std::fs::write(&path, "old").unwrap();
        let engine = Engine::new();
        let read = engine.call(json!({"command":"fs.read","path":path}));
        let open=engine.call(json!({"command":"document.open","text":"old","path":path,"version":read["value"]["version"]}));
        let id = open["value"]["id"].as_u64().unwrap();
        engine.call(json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":3,"text":"submitted"}));
        let result=engine.save_with(id,|write,route|{
    assert!(route.is_none());assert_eq!(write["text"],"submitted");
    let edit=engine.call(json!({"command":"document.edit","id":id,"revision":1,"start":9,"end":9,"text":" later"}));assert_eq!(edit["ok"],true);
    engine.services.call(&write).map_err(|message|fail("save_failed",message))
  }).unwrap();
        assert_eq!(result["text"], "submitted later");
        assert_eq!(result["dirty"], true);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "submitted");
        let undo = engine.call(json!({"command":"document.undo","id":id}));
        assert_eq!(undo["value"]["text"], "submitted");
        assert_eq!(undo["value"]["dirty"], false);
    }
}

#[cfg(all(test, unix))]
mod recovery_save_tests {
    use super::*;
    use buntrail_core::contracts::{
        MutationResult, ObservedState, OperationReceipt, OperationState, ReconciliationEvidence,
    };
    use std::os::unix::fs::MetadataExt;
    fn value(engine: &Engine, request: Value) -> Value {
        let result = engine.call(request);
        assert_eq!(result["ok"], true, "{result}");
        result["value"].clone()
    }
    fn bound(engine: &Engine, path: &std::path::Path) -> u64 {
        std::fs::write(path, "old").unwrap();
        let read = value(engine, json!({"command":"fs.read","path":path}));
        value(
            engine,
            json!({"command":"document.open","path":path,"version":read["version"],"text":"old"}),
        )["id"]
            .as_u64()
            .unwrap()
    }
    fn draft(engine: &Engine, path: &std::path::Path) -> Value {
        use std::os::unix::ffi::OsStrExt;
        let expected = serde_json::to_value(path.as_os_str().as_bytes()).unwrap();
        let entries = value(engine, json!({"command":"recovery.list"}));
        let entry = entries["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["file"]["path"]["PosixBytes"] == expected)
            .unwrap();
        value(
            engine,
            json!({"command":"recovery.read","document":entry["document"]}),
        )
    }
    #[test]
    fn pending_snapshot_is_durable_before_write_and_later_edits_stay_recoverable() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        let engine = Engine::new();
        let id = bound(&engine, &path);
        value(
            &engine,
            json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":3,"text":"submitted"}),
        );
        engine.save_with(id,|write,_| {
            let saved=draft(&engine,&path); assert_eq!(saved["text"],"submitted"); assert!(saved["pending_operation"].is_array());
            assert_eq!(std::fs::read(&path).unwrap(),b"old");
            value(&engine,json!({"command":"document.edit","id":id,"revision":1,"start":0,"end":9,"text":"later"}));
            engine.services.save_document(&write).map_err(|e|fail("save_failed",e))
        }).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"submitted");
        assert_eq!(
            value(&engine, json!({"command":"document.get","id":id}))["dirty"],
            true
        );
        let saved = draft(&engine, &path);
        assert_eq!(saved["text"], "later");
        assert!(saved["pending_operation"].is_null());
    }
    #[test]
    fn lost_acknowledgement_is_inspected_without_replaying_save() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        let engine = Engine::new();
        let id = bound(&engine, &path);
        value(
            &engine,
            json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":3,"text":"submitted"}),
        );
        let error = engine
            .save_with(id, |write, _| {
                let mut response = engine
                    .services
                    .save_document(&write)
                    .map_err(|e| fail("save_failed", e))?;
                let mut receipt: OperationReceipt =
                    serde_json::from_value(response["receipt"].clone()).unwrap();
                let OperationState::Confirmed(MutationResult::Saved(saved)) = receipt.state else {
                    panic!("real save must complete")
                };
                receipt.state = OperationState::Unknown(ReconciliationEvidence {
                    target: saved.version.target,
                    observed: ObservedState::Unobserved,
                    explanation: "dropped acknowledgement".into(),
                });
                response["receipt"] = serde_json::to_value(receipt).unwrap();
                Ok(response)
            })
            .unwrap_err();
        assert_eq!(error.code, "save_uncertain");
        let inode = std::fs::metadata(&path).unwrap().ino();
        assert_eq!(
            engine.call(json!({"command":"document.save","id":id}))["error"]["code"],
            "save_uncertain"
        );
        assert_eq!(
            engine.call(json!({"command":"document.close","id":id,"discard":true}))["error"]["code"],
            "save_uncertain"
        );
        let inspected = value(&engine, json!({"command":"document.inspect_save","id":id}));
        assert_eq!(inspected["dirty"], false);
        assert!(inspected["pending_operation"].is_null());
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(std::fs::read(&path).unwrap(), b"submitted");
    }
    #[test]
    fn mismatched_saved_binding_remains_uncertain() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        let other = root.path().join("other");
        let engine = Engine::new();
        let id = bound(&engine, &path);
        std::fs::write(&other, "other").unwrap();
        let foreign = value(&engine, json!({"command":"fs.read","path":other}))["version"].clone();
        value(
            &engine,
            json!({"command":"document.edit","id":id,"revision":0,"start":0,"end":3,"text":"submitted"}),
        );
        let result = engine.save_with(id, |write, _| {
            let mut response = engine
                .services
                .save_document(&write)
                .map_err(|e| fail("save_failed", e))?;
            response["version"] = foreign;
            Ok(response)
        });
        assert!(
            result.is_err(),
            "a confirmed receipt cannot adopt another file's token"
        );
        assert_eq!(
            value(&engine, json!({"command":"document.get","id":id}))["dirty"],
            true
        );
        assert_eq!(std::fs::read(&other).unwrap(), b"other");
    }
}
