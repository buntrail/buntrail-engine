use super::*;
impl Engine {
    pub(super) fn checkpoint_recovery(&self, id: u64, expected: Option<u64>) -> Result<Value> {
        let (journal, snapshot) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if expected.is_some_and(|owner| owner != document.recovery_owner) {
                return Err(fail("stale_revision", "Recovery ownership changed"));
            }
            if document.closing {
                return Err(fail("close_pending", "Recovery cleanup is pending"));
            }
            (document.journal.clone(), document.recovery_snapshot(false)?)
        };
        if let Some(snapshot) = snapshot {
            self.services
                .write_recovery(&journal, snapshot)
                .map_err(|e| fail("recovery_failed", e))?;
        }
        Ok(json!({"written":true}))
    }
    pub(super) fn discard_document_recovery(&self, id: u64) -> Result<Value> {
        let (journal, snapshot) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if document.saving || document.closing {
                return Err(fail("save_pending", "Wait for the pending operation"));
            }
            if document.pending_save.is_some() {
                return Err(fail(
                    "save_uncertain",
                    "Inspect the uncertain save before discarding recovery",
                ));
            }
            let snapshot = document.recovery_snapshot(true)?;
            document.recovery_owner = document
                .recovery_owner
                .checked_add(1)
                .ok_or_else(|| fail("exhausted", "Recovery ownership exhausted"))?;
            document.closing = true;
            let used = document.journal.retire();
            (document.journal.clone(), if used { snapshot } else { None })
        };
        let result = if let Some(snapshot) = snapshot {
            self.services.write_recovery(&journal, snapshot)
        } else {
            Ok(())
        };
        let mut documents = self.documents()?;
        let document = documents
            .open
            .get_mut(&id)
            .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
        document.closing = false;
        if let Err(error) = result {
            journal.activate();
            return Err(fail("recovery_failed", error));
        }
        document.journal = Arc::new(buntrail_services::recovery::journal::Journal::new());
        Ok(Value::Null)
    }
    pub(super) fn rotate_recovery(&self, document: &mut crate::document::Document) -> Result<()> {
        let snapshot = document.recovery_snapshot(true)?;
        let owner = document
            .recovery_owner
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Recovery ownership exhausted"))?;
        let journal = document.journal.clone();
        if journal.retire()
            && let Some(snapshot) = snapshot
        {
            let services = self.services.clone();
            let retired = journal.clone();
            if let Err(error) = std::thread::Builder::new()
                .name("buntrail-recovery-cleanup".into())
                .spawn(move || {
                    if let Err(error) = services.write_recovery(&retired, snapshot) {
                        eprintln!(
                            "Buntrail retained a recovery draft after cleanup failed: {error}"
                        );
                    }
                })
            {
                journal.activate();
                return Err(fail("recovery_failed", error.to_string()));
            }
        }
        document.recovery_owner = owner;
        document.journal = Arc::new(buntrail_services::recovery::journal::Journal::new());
        Ok(())
    }
    pub(super) fn close_document(&self, id: u64, discard: bool) -> Result<Value> {
        let (journal, snapshot) = {
            let mut documents = self.documents()?;
            let document = documents
                .open
                .get_mut(&id)
                .ok_or_else(|| fail("not_found", "Document is no longer open"))?;
            if document.saving || document.closing {
                return Err(fail(
                    "save_pending",
                    "Wait for pending save or recovery cleanup",
                ));
            }
            if document.pending_save.is_some_and(|p| p.uncertain) {
                return Err(fail(
                    "save_uncertain",
                    "Inspect the uncertain save before closing",
                ));
            }
            if (document.buffer.checkpoint() != document.saved_state
                || document.buffer.has_composition())
                && !discard
            {
                return Err(fail(
                    "dirty_document",
                    "Save changes or explicitly discard before closing",
                ));
            }
            let snapshot = document.recovery_snapshot(true)?;
            document.closing = true;
            let used = document.journal.retire();
            (document.journal.clone(), if used { snapshot } else { None })
        };
        if let Some(snapshot) = snapshot
            && let Err(message) = self.services.write_recovery(&journal, snapshot)
        {
            journal.activate();
            if let Some(document) = self.documents()?.open.get_mut(&id) {
                document.closing = false;
            }
            return Err(fail("recovery_failed", message));
        }
        self.documents()?.open.remove(&id);
        Ok(Value::Null)
    }
}
