//! Buffer and selection coordination inside the file-binding registry.
use super::{Result, fail};
use buntrail_core::{Document as Buffer, contracts::*};
use serde_json::{Value, json};
use std::collections::HashMap;

pub(super) fn core_error(error: buntrail_core::Error) -> super::Failure {
    match error {
        buntrail_core::Error::Stale => fail(
            "stale_revision",
            "Document changed; resynchronize the native mirror",
        ),
        buntrail_core::Error::Composition => {
            fail("composition", "Resolve composition in its owning view")
        }
        _ => fail("invalid_range", "Invalid UTF-16 range"),
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
pub(super) struct PendingSave {
    pub operation: OperationID,
    pub revision: BufferRevision,
    pub checkpoint: u64,
    pub uncertain: bool,
}
pub(super) struct Document {
    pub buffer: Buffer,
    pub saved_state: u64,
    pub path: Option<String>,
    pub version: Option<String>,
    pub encoding_source: Option<String>,
    pub persistent_id: Option<DocumentID>,
    pub remote: Option<String>,
    pub saving: bool,
    #[cfg(unix)]
    pub closing: bool,
    #[cfg(unix)]
    pub journal: std::sync::Arc<buntrail_services::recovery::journal::Journal>,
    #[cfg(unix)]
    pub recovery_generation: u64,
    #[cfg(unix)]
    pub recovery_owner: u64,
    #[cfg(unix)]
    pub pending_save: Option<PendingSave>,
    pub language: Option<String>,
    pub language_generation: u64,
    selections: HashMap<ViewID, BufferRange>,
    undo_selections: Vec<BufferRange>,
    redo_selections: Vec<BufferRange>,
}
impl Document {
    pub fn new(
        text: &str,
        path: Option<String>,
        version: Option<String>,
        remote: Option<String>,
    ) -> Self {
        Self {
            buffer: Buffer::new(text.into()),
            saved_state: 0,
            path,
            version,
            encoding_source: None,
            persistent_id: None,
            remote,
            saving: false,
            #[cfg(unix)]
            closing: false,
            #[cfg(unix)]
            journal: std::sync::Arc::new(buntrail_services::recovery::journal::Journal::new()),
            #[cfg(unix)]
            recovery_generation: 0,
            #[cfg(unix)]
            recovery_owner: 0,
            #[cfg(unix)]
            pending_save: None,
            language: None,
            language_generation: 0,
            selections: HashMap::new(),
            undo_selections: vec![],
            redo_selections: vec![],
        }
    }
    #[cfg(unix)]
    pub fn recovery_snapshot(
        &mut self,
        discard: bool,
    ) -> Result<Option<buntrail_core::documents::RecoverySnapshot>> {
        let Some(token) = self.version.as_deref().filter(|v| v.starts_with("bt2:")) else {
            return Ok(None);
        };
        let binding = buntrail_services::Services::file_binding(token)
            .map_err(|e| fail("recovery_failed", e))?;
        self.recovery_generation = self
            .recovery_generation
            .checked_add(1)
            .ok_or_else(|| fail("exhausted", "Recovery generation exhausted"))?;
        let composition = self.buffer.has_composition();
        let text = if discard {
            std::sync::Arc::new(buntrail_core::Text::from(""))
        } else {
            self.buffer.snapshot().text
        };
        Ok(Some(buntrail_core::documents::RecoverySnapshot {
            snapshot: DocumentSnapshot {
                document: DocumentID([0; 16]),
                revision: BufferRevision(self.buffer.revision()),
                text,
                encoding: binding.encoding,
                line_endings: LineEndings::Mixed,
                language: self.language.clone(),
            },
            file: binding.file,
            version: binding.version,
            pending_operation: if discard {
                None
            } else {
                self.pending_save.map(|p| p.operation)
            },
            pending_revision: if discard {
                None
            } else {
                self.pending_save.map(|p| p.revision)
            },
            provisional_composition: !discard && composition,
            generation: self.recovery_generation,
            epoch: 0,
            lease: None,
            clean: discard
                || (self.buffer.checkpoint() == self.saved_state
                    && self.pending_save.is_none()
                    && !composition),
        }))
    }
    pub fn metadata(&self, id: u64) -> Value {
        let state = json!({"id":id,"revision":self.buffer.revision(),"dirty":self.buffer.checkpoint()!=self.saved_state,"path":self.path,"version":self.version,"remote":self.remote,"language_generation":self.language_generation,"persistent_id":self.persistent_id,"encoding_source":self.version.as_ref().filter(|v|v.starts_with("bt2:")).or(self.encoding_source.as_ref())});
        #[cfg(unix)]
        let state = {
            let mut state = state;
            state["recovery_owner"] = self.recovery_owner.into();
            state["pending_operation"] = self
                .pending_save
                .map(|p| json!(p.operation))
                .unwrap_or(serde_json::Value::Null);
            if self.pending_save.is_some_and(|p| p.uncertain) {
                state["dirty"] = true.into();
            }
            state
        };
        state
    }
    pub fn state(&self, id: u64) -> Value {
        let mut state = self.metadata(id);
        state["text"] = self.buffer.text().into();
        state
    }
    pub fn edit(&mut self, r: &Value, id: u64) -> Result<Value> {
        let revision = super::number(r, "revision")?;
        let start = super::number(r, "start")?;
        let end = super::number(r, "end")?;
        let text = super::string(r, "text")?;
        self.transaction(
            EditTransaction {
                expected_revision: BufferRevision(revision),
                source_view: ViewID(0),
                intent: EditIntent::Replace,
                replacements: vec![Replacement {
                    range: BufferRange {
                        revision: BufferRevision(revision),
                        start,
                        end,
                    },
                    text: text.into(),
                }],
            },
            None,
            2,
        )?;
        Ok(
            json!({"id":id,"revision":self.buffer.revision(),"dirty":self.buffer.checkpoint()!=self.saved_state}),
        )
    }
    pub fn history(&mut self, id: u64, undo: bool) -> Result<Value> {
        self.transaction(
            EditTransaction {
                expected_revision: BufferRevision(self.buffer.revision()),
                source_view: ViewID(0),
                intent: if undo {
                    EditIntent::Undo
                } else {
                    EditIntent::Redo
                },
                replacements: vec![],
            },
            None,
            2,
        )?;
        Ok(self.state(id))
    }
    fn range(&self, r: &BufferRange) -> Result<()> {
        if r.revision.0 != self.buffer.revision() {
            return Err(core_error(buntrail_core::Error::Stale));
        }
        if r.end < r.start {
            return Err(fail("invalid_range", "Range start exceeds end"));
        }
        let text = self.buffer.snapshot().text;
        for offset in [r.start, r.end] {
            let offset =
                usize::try_from(offset).map_err(|_| fail("invalid_range", "Offset too large"))?;
            text.utf16_to_char(offset).map_err(core_error)?;
        }
        Ok(())
    }
    pub fn transaction(
        &mut self,
        mut transaction: EditTransaction,
        selection: Option<BufferRange>,
        typing: u32,
    ) -> Result<EditResult> {
        #[cfg(unix)]
        if self.closing {
            return Err(fail(
                "close_pending",
                "Wait for recovery cleanup before editing",
            ));
        }
        if transaction.expected_revision.0 != self.buffer.revision() {
            return Err(core_error(buntrail_core::Error::Stale));
        }
        if typing > 2 {
            return Err(fail("invalid_request", "Invalid transaction options"));
        }
        if let Some(s) = &selection {
            self.range(s)?;
        }
        for r in &transaction.replacements {
            self.range(&r.range)?;
            if r.text.len() > 64 * 1024 * 1024 {
                return Err(fail("too_large", "Edit exceeds 64 MiB"));
            }
        }
        if transaction.replacements.len() > 1 {
            let mut ranges = transaction
                .replacements
                .iter()
                .map(|r| (&r.range.start, &r.range.end))
                .collect::<Vec<_>>();
            ranges.sort_unstable();
            if ranges
                .windows(2)
                .any(|r| r[0].1 > r[1].0 || r[0].0 == r[1].0)
            {
                return Err(fail("invalid_range", "Overlapping replacements"));
            }
        }
        // Legacy no-ops must not create editor revisions or undo groups.
        if matches!(
            transaction.intent,
            EditIntent::Replace | EditIntent::UpdateComposition
        ) {
            let snapshot = self.buffer.snapshot();
            transaction.replacements.retain(|r| {
                let start = snapshot.text.utf16_to_char(r.range.start as usize).unwrap();
                let end = snapshot.text.utf16_to_char(r.range.end as usize).unwrap();
                snapshot.text.slice(start, end) != r.text
            });
        }
        let view = transaction.source_view;
        let intent = transaction.intent.clone();
        let before = selection
            .clone()
            .or_else(|| self.selections.get(&view).cloned())
            .or_else(|| transaction.replacements.first().map(|r| r.range.clone()));
        let composition = self.buffer.has_composition();
        let undo_depth = self.buffer.undo_depth();
        if typing == 1 {
            self.buffer.begin_typing(view.0);
        } else if typing == 2 {
            self.buffer.end_typing();
        }
        let mut result = match self.buffer.apply_transaction(transaction) {
            Ok(result) => result,
            Err(buntrail_core::Error::EmptyHistory) => {
                return Ok(EditResult {
                    revision: BufferRevision(self.buffer.revision()),
                    deltas: vec![],
                    selection_anchors: selection.into_iter().collect(),
                });
            }
            Err(error) => return Err(core_error(error)),
        };
        // Selection history follows buffer undo groups.
        match intent {
            EditIntent::Undo => {
                if let Some(s) = self.undo_selections.pop() {
                    self.redo_selections.push(s.clone());
                    result.selection_anchors = vec![BufferRange {
                        revision: result.revision,
                        ..s
                    }];
                }
            }
            EditIntent::Redo => {
                if let Some(s) = self.redo_selections.pop() {
                    self.undo_selections.push(s);
                }
            }
            EditIntent::Replace
                if !composition
                    && self.buffer.undo_depth() > undo_depth
                    && !result.deltas.is_empty() =>
            {
                if let Some(s) = before {
                    self.undo_selections.push(s);
                    self.redo_selections.clear();
                }
            }
            EditIntent::BeginComposition => {
                self.undo_selections.push(before.unwrap_or(BufferRange {
                    revision: result.revision,
                    start: 0,
                    end: 0,
                }));
            }
            EditIntent::CommitComposition if self.buffer.undo_depth() == undo_depth => {
                self.undo_selections.pop();
            }
            EditIntent::CancelComposition => {
                if let Some(selection) = self.undo_selections.pop() {
                    result.selection_anchors = vec![BufferRange {
                        revision: result.revision,
                        ..selection
                    }];
                }
            }
            _ => {}
        }
        if let Some(s) = result.selection_anchors.first() {
            self.selections.insert(view, s.clone());
        }
        Ok(result)
    }
}
