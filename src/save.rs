use super::*;
use crate::document::PendingSave;
use buntrail_core::contracts::*;

fn completion(pending: PendingSave, path: &str, response: Result<Value>) -> Result<String> {
    let value = response?;
    if let Some(receipt) = value.get("receipt") {
        let receipt: OperationReceipt = serde_json::from_value(receipt.clone()).map_err(|_| {
            fail(
                "save_uncertain",
                "Invalid save receipt; inspect the operation",
            )
        })?;
        if receipt.id != pending.operation {
            return Err(fail("save_uncertain", "Save receipt operation changed"));
        }
        match receipt.state {
            OperationState::Confirmed(MutationResult::Saved(saved))
                if saved.saved_revision == pending.revision =>
            {
                use std::os::unix::ffi::OsStrExt;
                let token = string(&value, "version")
                    .map_err(|_| fail("save_uncertain", "Saved binding is missing"))?;
                let binding =
                    Services::file_binding(token).map_err(|e| fail("save_uncertain", e))?;
                let expected =
                    std::path::absolute(path).map_err(|e| fail("save_uncertain", e.to_string()))?;
                let expected = TargetPath::posix(expected.as_os_str().as_bytes().to_vec())
                    .map_err(|e| fail("save_uncertain", e.message))?;
                if binding.version != saved.version || binding.file.path != expected {
                    return Err(fail(
                        "save_uncertain",
                        "Saved binding does not match the submitted file",
                    ));
                }
            }
            OperationState::Failed(failure) if failure.effect == FailureEffect::ProvenNoEffect => {
                return Err(fail("save_failed", failure.error.message));
            }
            _ => {
                return Err(fail(
                    "save_uncertain",
                    "Save outcome is uncertain; retain edits and inspect the save",
                ));
            }
        }
    }
    string(&value, "version").map(str::to_owned).map_err(|_| {
        fail(
            "save_uncertain",
            "Saved file version is missing; inspect the save",
        )
    })
}
impl Engine {
    pub(super) fn save(&self, id: u64) -> Result<Value> {
        self.save_with(id, |write, remote| match remote {
            Some(remote) => self
                .runtime
                .call(&json!({"command":"remote.call","id":remote,"request":write})),
            None => self
                .services
                .save_document(&write)
                .map_err(|e| fail("save_failed", e)),
        })
    }

    pub(super) fn save_with(
        &self,
        id: u64,
        writer: impl FnOnce(Value, Option<String>) -> Result<Value>,
    ) -> Result<Value> {
        let operation = OperationID(
            buntrail_services::connections::local::new_id()
                .map_err(|e| fail("save_failed", e.message))?,
        );
        let (write, remote, journal, prepared, pending, path) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if document.saving || document.closing {
                return Err(fail("save_pending", "Wait for pending save or close"));
            }
            if document.pending_save.is_some() {
                return Err(fail(
                    "save_uncertain",
                    "Inspect the uncertain save before retrying",
                ));
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
            let snapshot = document
                .buffer
                .confirmed_snapshot()
                .map_err(crate::document::core_error)?;
            let pending = PendingSave {
                operation,
                revision: BufferRevision(snapshot.revision),
                checkpoint: document.buffer.checkpoint(),
                uncertain: false,
            };
            document.saving = true;
            document.pending_save = Some(pending);
            let prepared = match document.recovery_snapshot(false) {
                Ok(v) => v,
                Err(e) => {
                    document.saving = false;
                    document.pending_save = None;
                    return Err(e);
                }
            };
            let write = json!({"command":"fs.write","path":path,"version":version,"text":snapshot.text.to_string(),"operation":operation,"revision":snapshot.revision});
            (
                write,
                document.remote.clone(),
                document.journal.clone(),
                prepared,
                pending,
                path,
            )
        };
        if let Some(prepared) = prepared
            && let Err(message) = self.services.write_recovery(&journal, prepared)
        {
            if let Some(document) = self.documents()?.open.get_mut(&id) {
                document.saving = false;
                document.pending_save = None;
            }
            return Err(fail("recovery_failed", message));
        }
        let result = completion(pending, &path, writer(write, remote));
        self.finish_save(id, pending, result)
    }
    pub(super) fn inspect_save(&self, id: u64) -> Result<Value> {
        let (pending, version, path, remote) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if document.saving || document.closing {
                return Err(fail("save_pending", "Wait for the active operation"));
            }
            let pending = document
                .pending_save
                .ok_or_else(|| fail("invalid_request", "There is no uncertain save"))?;
            let version = document
                .version
                .clone()
                .ok_or_else(|| fail("wrong_file", "File binding is missing"))?;
            document.saving = true;
            let path = document
                .path
                .clone()
                .ok_or_else(|| fail("wrong_file", "File path is missing"))?;
            (pending, version, path, document.remote.clone())
        };
        let response=match remote {
            Some(remote)=>self.runtime.call(&json!({"command":"remote.call","id":remote,"request":{"command":"fs.inspect","path":path,"version":version,"operation":pending.operation}})).map_err(|e|fail("save_uncertain",e.message)),
            None=>self.services.inspect_save(&version,pending.operation).map_err(|e|fail("save_uncertain",e)),
        };
        let result = completion(pending, &path, response);
        self.finish_save(id, pending, result)
    }
    fn finish_save(&self, id: u64, pending: PendingSave, result: Result<String>) -> Result<Value> {
        let (journal, snapshot) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document closed during save"))?;
            match &result {
                Ok(version) => {
                    document.version = Some(version.clone());
                    document.saved_state = pending.checkpoint;
                    document.pending_save = None;
                }
                Err(error) if error.code == "save_uncertain" => {
                    document.pending_save = Some(PendingSave {
                        uncertain: true,
                        ..pending
                    });
                }
                Err(_) => document.pending_save = None,
            }
            let snapshot = match document.recovery_snapshot(false) {
                Ok(v) => v,
                Err(e) => {
                    document.saving = false;
                    return Err(e);
                }
            };
            (document.journal.clone(), snapshot)
        };
        let journal_result = if let Some(snapshot) = snapshot {
            self.services
                .write_recovery(&journal, snapshot)
                .map_err(|e| fail("recovery_failed", e))
        } else {
            Ok(())
        };
        let mut documents = self.documents()?;
        let document = documents
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document closed during save"))?;
        document.saving = false;
        result?;
        journal_result?;
        Ok(document.state(id))
    }
}
