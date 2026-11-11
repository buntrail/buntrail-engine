use super::*;
use buntrail_core::workspaces::{GitRecoveryKind, GitRecoveryReference};
use buntrail_services::{
    connections::local::LocalConnection, git::worktree_proof::WorktreePreparation,
};
use sha2::{Digest, Sha256};
use std::sync::{RwLockReadGuard, atomic::AtomicBool};

impl Runtime {
    pub fn document_admission(&self) -> Result<RwLockReadGuard<'_, ()>> {
        self.admission.try_read().map_err(|_| {
            fail(
                "busy",
                "A Git worktree change is pending; retry after it finishes",
            )
        })
    }
    pub(super) fn git_call(
        &self,
        route: Option<&str>,
        r: &Value,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Value> {
        let cancel = cancel.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let local = self
            .services
            .local_connection()
            .map_err(|e| fail("unavailable", e))?;
        let remote = route
            .map(|route| self.route(route))
            .transpose()?
            .map(|s| Arc::new(s.with_cancellation(cancel.clone())));
        if let Some(remote) = &remote {
            remote.validate_environment_ready().map_err(error)?;
        }
        let target = remote
            .as_ref()
            .map_or_else(|| local.identity.clone(), |s| s.target().clone());
        let backend = GitBackend {
            local: &local,
            remote: remote.as_deref(),
            cancel: &cancel,
        };
        let command = string(r, "command")?;
        if command == "git.pending" {
            let entries = local
                .store
                .load()
                .map_err(error)?
                .records
                .into_iter()
                .flat_map(|w| w.git_recoveries)
                .filter(|p| p.worktree.target == target)
                .collect::<Vec<_>>();
            return Ok(json!({"entries":entries}));
        }
        if command == "git.inspect" {
            let operation: OperationID = serde_json::from_value(r["operation"].clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?;
            let reference = local
                .store
                .load()
                .map_err(error)?
                .records
                .into_iter()
                .flat_map(|w| w.git_recoveries)
                .find(|p| p.operation == operation && p.worktree.target == target)
                .ok_or_else(|| {
                    fail(
                        "not_found",
                        "Retained Git operation not found for this target",
                    )
                })?;
            let receipt = match &remote {
                Some(s) => run(s.inspect(operation, target.clone())),
                None => run(local.files.inspect(operation, target.clone())),
            }
            .map_err(error)?;
            Self::validate_git_receipt(&reference, &receipt)?;
            if resolved(&receipt.state) {
                local
                    .store
                    .resolve_git_recovery(reference.owner.workspace, &reference)
                    .map_err(error)?;
            }
            return Ok(
                json!({"receipt":receipt,"resolved":resolved(&receipt.state),"repository_action":reference.repository_action}),
            );
        }
        let root = FileRef {
            target: target.clone(),
            path: TargetPath::posix(string(r, "root")?.as_bytes().to_vec()).map_err(error)?,
            resolved_identity: None,
        };
        let worktree = backend
            .discover(root)?
            .ok_or_else(|| fail("not_git", "This folder is not a Git worktree"))?;
        if command == "git.repository" || command == "git.merge_state" {
            let snapshot = match &remote {
                Some(s) => run(s.repository(worktree)),
                None => local.git.repository_sync(worktree, &cancel),
            }
            .map_err(error)?;
            if command == "git.merge_state" {
                return Ok(json!(snapshot.merge));
            }
            let pending = local.store.load().map(|catalog| {
                catalog
                    .records
                    .into_iter()
                    .flat_map(|w| w.git_recoveries)
                    .filter(|p| p.worktree == snapshot.worktree)
                    .map(|p| p.operation)
                    .collect::<Vec<_>>()
            });
            let mut value = json!(snapshot);
            match pending {
                Ok(entries) => value["pending_operations"] = json!(entries),
                Err(error) => {
                    value["pending_operations"] = Value::Null;
                    value["pending_error"] = json!(error.message);
                }
            }
            return Ok(value);
        }
        if command == "git.commit" {
            let query = CommitQuery {
                worktree,
                oid: string(r, "oid")?.into(),
                cursor: r
                    .get("cursor")
                    .filter(|v| !v.is_null())
                    .map(|v| {
                        v.as_u64()
                            .ok_or_else(|| fail("invalid_request", "Invalid file cursor"))
                    })
                    .transpose()?,
            };
            return Ok(json!(
                match &remote {
                    Some(s) => run(s.commit(query)),
                    None => local.git.commit_sync(query, &cancel),
                }
                .map_err(error)?
            ));
        }
        if command == "git.commit_diff" {
            let path: TargetPath = serde_json::from_value(r["path"].clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?;
            let query = DiffRequest {
                worktree,
                path,
                source: DiffSource::Commit(string(r, "oid")?.into()),
            };
            return Ok(json!(
                match &remote {
                    Some(s) => run(s.diff(query)),
                    None => local.git.diff_sync(query, &cancel),
                }
                .map_err(error)?
            ));
        }
        if command == "git.history" {
            let refs = r
                .get("refs")
                .map(|v| {
                    serde_json::from_value(v.clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))
                })
                .transpose()?
                .unwrap_or_default();
            let cursor = r
                .get("cursor")
                .filter(|v| !v.is_null())
                .map(|v| {
                    serde_json::from_value(v.clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))
                })
                .transpose()?;
            let query = HistoryQuery {
                filter: r
                    .get("filter")
                    .filter(|v| !v.is_null())
                    .map(|v| {
                        serde_json::from_value(v.clone())
                            .map_err(|e| fail("invalid_request", e.to_string()))
                    })
                    .transpose()?,
                worktree,
                ref_filter: refs,
                cursor,
            };
            return Ok(json!(
                match &remote {
                    Some(s) => run(s.history(query)),
                    None => local.git.history_sync(query, &cancel),
                }
                .map_err(error)?
            ));
        }
        let args = if command != "git.mutate" {
            let args = r["args"]
                .as_array()
                .ok_or_else(|| fail("invalid_request", "Git args must be an array"))?
                .iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| fail("invalid_request", "Git args must be strings"))
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(query) = GitRead::from_args(&args) {
                let output = backend.read(worktree, query)?;
                return Ok(
                    json!({"stdout":String::from_utf8_lossy(&output.stdout),"stderr":String::from_utf8_lossy(&output.stderr),"status":if output.success{0}else{1},"truncated":false}),
                );
            }
            Some(args)
        } else {
            None
        };
        let initial = backend.status(worktree.clone())?;
        let mutation = if let Some(args) = args {
            mutation(&backend, &worktree, &initial.state, &args)?
        } else {
            let ExpectedState::Git {
                worktree: observed,
                state: expected,
            } = serde_json::from_value(r["expected"].clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?
            else {
                return Err(fail(
                    "invalid_request",
                    "Expected a Git repository observation",
                ));
            };
            if observed != worktree {
                return Err(fail(
                    "target_changed",
                    "Repository identity changed; refresh and review this repository before applying an action",
                ));
            }
            if initial.state != expected {
                return Err(fail(
                    "stale_revision",
                    "Repository changed; refresh before applying this action",
                ));
            }
            GitMutation::Repository(
                serde_json::from_value(r["action"].clone())
                    .map_err(|e| fail("invalid_request", e.to_string()))?,
            )
        };
        if matches!(mutation, GitMutation::Repository(_))
            && let Some(remote) = &remote
        {
            remote.require_repository().map_err(error)?;
        }
        if local
            .store
            .load()
            .map_err(error)?
            .records
            .iter()
            .flat_map(|w| &w.git_recoveries)
            .any(|p| p.worktree == worktree)
        {
            return Err(fail(
                "mutation_uncertain",
                "Inspect pending Git operations for this repository before another mutation",
            ));
        }
        let changing = mutation.changes_worktree();
        let staging = matches!(mutation, GitMutation::Stage(_));
        let _exclusive = if changing || staging {
            Some(self.admission.try_write().map_err(|_| {
                fail(
                    "busy",
                    "Documents or filesystem operations are still active",
                )
            })?)
        } else {
            None
        };
        if changing || staging {
            let docs = self
                .documents
                .lock()
                .map_err(|_| fail("engine_failed", "Document registry poisoned"))?;
            if docs.open.values().any(|d| {
                d.path.is_some()
                    && (changing
                        || d.saving
                        || d.pending_save.is_some()
                        || d.buffer.checkpoint() != d.saved_state)
            }) {
                return Err(fail(
                    "busy",
                    if changing {
                        "Close bound documents before changing the worktree"
                    } else {
                        "Save bound documents and inspect pending saves before staging"
                    },
                ));
            }
        }
        let _worktree_lease = if changing {
            Some(self.git_gate.acquire(worktree.clone()).map_err(error)?)
        } else {
            None
        };
        let workspace = self.terminal_workspace(route, r)?;
        let owner = workspace.lock().unwrap().owner.clone();
        let mut local_observation = None;
        let mut remote_observation = None;
        let expected = if changing {
            if let Some(remote) = &remote {
                let observed = remote
                    .observe_owned(worktree.clone(), std::iter::empty(), |_| Ok(()))
                    .map_err(error)?;
                let state = observed.state.clone();
                remote_observation = Some(observed);
                state
            } else {
                let authority = local.observation_owner(owner.lease).map_err(error)?;
                let observed = local
                    .git
                    .observe_owned_sync(&authority, worktree.clone(), std::iter::empty(), &cancel)
                    .map_err(error)?;
                let state = observed.snapshot.state.clone();
                local_observation = Some((authority, observed));
                state
            }
        } else {
            initial.state.clone()
        };
        if let Err(error) = validate_repository_observation(&mutation, &initial.state, &expected) {
            if let Some((authority, observed)) = &local_observation {
                local.git.release_observation(authority, observed.id);
            }
            if let Some(observed) = &remote_observation {
                let _ = remote.as_ref().unwrap().release_observation(observed.id);
            }
            return Err(error);
        }
        if matches!(mutation, GitMutation::PullFastForward { .. })
            && expected.head != initial.state.head
        {
            return Err(fail(
                "stale_revision",
                "Branch changed while preparing pull",
            ));
        }
        let operation = OperationID(new_id().map_err(error)?);
        let request = GitMutationRequest {
            operation,
            worktree: worktree.clone(),
            expected,
            command: mutation,
        };
        let kind = match request.command {
            GitMutation::Repository(_) => GitRecoveryKind::Repository,
            GitMutation::Stage(_) => GitRecoveryKind::Stage,
            GitMutation::Unstage(_) => GitRecoveryKind::Unstage,
            GitMutation::Commit { .. } => GitRecoveryKind::Commit,
            GitMutation::Checkout { .. } => GitRecoveryKind::Checkout,
            GitMutation::Fetch { .. } => GitRecoveryKind::Fetch,
            GitMutation::Push { .. } => GitRecoveryKind::Push,
            GitMutation::PullFastForward { .. } => GitRecoveryKind::Pull,
        };
        let reference = GitRecoveryReference {
            repository_action: match &request.command {
                GitMutation::Repository(action) => Some(action.clone()),
                _ => None,
            },
            operation,
            worktree,
            kind,
            request_digest: Sha256::digest(
                serde_json::to_vec(&request).map_err(|e| fail("invalid_request", e.to_string()))?,
            )
            .into(),
            owner,
        };
        let mut retained = false;
        let mut submitted = false;
        let result = (|| {
            local
                .store
                .retain_git_recovery(reference.owner.workspace, reference.clone())
                .map_err(error)?;
            retained = true;
            if let Some((authority, observed)) = &local_observation {
                let digest = local
                    .git
                    .observation_alias_digest(authority, observed.id)
                    .map_err(error)?;
                let prepare = local
                    .git
                    .prepare_worktree_sync(
                        authority,
                        &request,
                        observed.id,
                        digest,
                        local
                            .git
                            .save_approval_input(authority, [])
                            .map_err(error)?,
                        &cancel,
                    )
                    .map_err(error)?;
                match prepare {
                    WorktreePreparation::Existing(receipt) => Ok(*receipt),
                    WorktreePreparation::Prepared(plan) => {
                        let approvals = local
                            .git
                            .save_approval_input(authority, [])
                            .map_err(error)?;
                        submitted = true;
                        let receipt = local
                            .git
                            .execute_worktree_sync(
                                authority, request, plan, digest, approvals, &cancel,
                            )
                            .map_err(error);
                        local.git.cancel_worktree_plan(authority, plan);
                        receipt
                    }
                }
            } else if let Some(observed) = &remote_observation {
                let remote = remote.as_ref().unwrap();
                match remote
                    .prepare_worktree(
                        &request,
                        observed.id,
                        observed.alias_digest,
                        std::iter::empty(),
                    )
                    .map_err(error)?
                {
                    WorktreePreparation::Existing(receipt) => Ok(*receipt),
                    WorktreePreparation::Prepared(plan) => {
                        submitted = true;
                        let receipt = remote
                            .execute_worktree(
                                request,
                                plan,
                                observed.alias_digest,
                                std::iter::empty(),
                            )
                            .map_err(error);
                        let _ = remote.cancel_worktree_plan(plan);
                        receipt
                    }
                }
            } else {
                submitted = true;
                Ok(backend.mutate(request))
            }
        })();
        if let Some((authority, observed)) = &local_observation {
            local.git.release_observation(authority, observed.id);
        }
        if let Some(observed) = &remote_observation {
            let _ = remote.as_ref().unwrap().release_observation(observed.id);
        }
        if result.is_err() && !submitted {
            if retained {
                local
                    .store
                    .resolve_git_recovery(reference.owner.workspace, &reference)
                    .map_err(error)?;
            }
            return result.map(|_| Value::Null);
        }
        let receipt = result.map_err(|e| {
            fail(
                "mutation_uncertain",
                format!(
                    "{}; inspect Git operation {} before retrying",
                    e.message,
                    buntrail_protocol::services::uuid(operation.0)
                ),
            )
        })?;
        Self::validate_git_receipt(&reference, &receipt)?;
        let resolved = resolved(&receipt.state);
        let success = matches!(receipt.state, OperationState::Confirmed(_));
        if matches!(
            &receipt.state,
            OperationState::Failed(OperationFailure {
                effect: FailureEffect::ProvenNoEffect,
                ..
            })
        ) {
            local
                .store
                .resolve_git_recovery(reference.owner.workspace, &reference)
                .map_err(error)?;
        }
        let diagnostic = match &receipt.state {
            OperationState::Failed(failure) => failure.error.message.clone(),
            OperationState::Unknown(evidence) => evidence.explanation.clone(),
            _ => "Inspect this operation before continuing".into(),
        };
        Ok(
            json!({"stdout":if success {"Git operation confirmed"}else{""},"stderr":if success {String::new()}else{diagnostic},"status":if success{0}else{1},"truncated":false,"operation":operation,"receipt":receipt,"pending":!resolved}),
        )
    }
    fn validate_git_receipt(
        reference: &GitRecoveryReference,
        receipt: &OperationReceipt,
    ) -> Result<()> {
        if receipt.id != reference.operation
            || matches!(&receipt.state,OperationState::Failed(f) if f.target!=reference.worktree.target)
            || matches!(&receipt.state,OperationState::Unknown(e) if e.target!=reference.worktree.target)
            || matches!(&receipt.state,OperationState::Confirmed(result) if !matches!(result,MutationResult::GitChanged {worktree,..} if worktree==&reference.worktree))
        {
            return Err(fail(
                "mutation_uncertain",
                "Git receipt identity differs; preserve pending inspection",
            ));
        }
        Ok(())
    }
}
fn resolved(state: &OperationState) -> bool {
    matches!(
        state,
        OperationState::Confirmed(_)
            | OperationState::Failed(OperationFailure {
                effect: FailureEffect::ProvenNoEffect,
                ..
            })
    )
}
struct GitBackend<'a> {
    local: &'a LocalConnection,
    remote: Option<&'a RemoteServices>,
    cancel: &'a AtomicBool,
}
impl GitBackend<'_> {
    fn discover(&self, root: FileRef) -> Result<Option<WorktreeIdentity>> {
        match self.remote {
            Some(s) => run(s.discover(root)),
            None => self.local.git.discover_sync(root, self.cancel),
        }
        .map_err(error)
    }
    fn status(&self, worktree: WorktreeIdentity) -> Result<GitSnapshot> {
        match self.remote {
            Some(s) => run(s.status(worktree)),
            None => self.local.git.status_sync(worktree, self.cancel),
        }
        .map_err(error)
    }
    fn read(&self, worktree: WorktreeIdentity, query: GitRead) -> Result<GitReadOutput> {
        match self.remote {
            Some(s) => run(s.read_query(worktree, query)),
            None => self.local.git.read_sync(worktree, query, self.cancel),
        }
        .map_err(error)
    }
    fn mutate(&self, request: GitMutationRequest) -> OperationReceipt {
        match self.remote {
            Some(s) => run(GitService::mutate(s, request)),
            None => self.local.git.mutate_sync(request, self.cancel),
        }
    }
}

fn mutation(
    backend: &GitBackend<'_>,
    worktree: &WorktreeIdentity,
    state: &GitExpectedState,
    args: &[&str],
) -> Result<GitMutation> {
    let path = |relative: &str| -> Result<TargetPath> {
        if relative.is_empty()
            || std::path::Path::new(relative)
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(fail(
                "invalid_request",
                "Git file paths must be relative children",
            ));
        }
        TargetPath::posix(
            format!(
                "{}/{}",
                files_adapter::text_path(&worktree.canonical_path)?.trim_end_matches('/'),
                relative
            )
            .into_bytes(),
        )
        .map_err(error)
    };
    Ok(match args {
        ["add", "--", file] => GitMutation::Stage(vec![path(file)?]),
        ["reset", "HEAD", "--", file] => GitMutation::Unstage(vec![path(file)?]),
        ["commit", "-m", message] if !message.trim().is_empty() && message.len() <= 1024 * 1024 => {
            GitMutation::Commit {
                message: (*message).into(),
            }
        }
        ["checkout", "--no-guess", reference]
            if !reference.is_empty() && !reference.starts_with('-') =>
        {
            GitMutation::Checkout {
                reference: reference.as_bytes().to_vec(),
            }
        }
        ["fetch"] | ["push"] | ["pull", "--ff-only"] => {
            let branch = match &state.head {
                GitHead::Branch { name, .. } => name,
                _ => {
                    return Err(fail(
                        "invalid_request",
                        "Select a checked-out branch for network operations",
                    ));
                }
            };
            let full = [b"refs/heads/".as_slice(), branch].concat();
            let output = backend.read(worktree.clone(), GitRead::Upstream)?;
            if !output.success {
                return Err(fail("unavailable", "Cannot read branch upstream"));
            }
            let upstream = output.stdout.split(|b| *b == b'\n').find_map(|line| {
                let fields = line.split(|b| *b == 0).collect::<Vec<_>>();
                (fields.len() == 3
                    && fields[0] == full
                    && !fields[1].is_empty()
                    && !fields[2].is_empty())
                .then(|| (fields[1].to_vec(), fields[2].to_vec()))
            });
            let (remote, reference) = match upstream {
                Some(pair) => pair,
                None if args == ["fetch"] => (b"origin".to_vec(), full),
                None => {
                    return Err(fail(
                        "invalid_request",
                        "Configure a branch upstream before pushing or pulling",
                    ));
                }
            };
            match args[0] {
                "fetch" => GitMutation::Fetch { remote },
                "push" => GitMutation::Push { remote, reference },
                _ => GitMutation::PullFastForward { remote, reference },
            }
        }
        _ => {
            return Err(fail(
                "invalid_request",
                "Unsupported Git command; arbitrary mutation flags are not accepted",
            ));
        }
    })
}

fn validate_repository_observation(
    mutation: &GitMutation,
    approved: &GitExpectedState,
    captured: &GitExpectedState,
) -> Result<()> {
    if matches!(mutation, GitMutation::Repository(_)) && approved != captured {
        return Err(fail(
            "stale_revision",
            "Repository changed after confirmation; refresh and review the changes before applying this action",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod repository_observation_tests {
    use super::*;
    #[test]
    fn abort_cannot_adopt_saved_edits_after_confirmation() {
        let approved = GitExpectedState {
            head: GitHead::Branch {
                name: b"refs/heads/main".to_vec(),
                oid: "a".repeat(40),
            },
            index_fingerprint: [1; 32],
            worktree_fingerprint: [2; 32],
        };
        let mut saved = approved.clone();
        saved.worktree_fingerprint = [3; 32];
        let abort = GitMutation::Repository(RepositoryAction::AbortMerge {
            session: OperationID([4; 16]),
        });
        assert!(
            validate_repository_observation(&abort, &approved, &saved).is_err(),
            "newly saved tracked edits were never listed in the abort confirmation"
        );
        assert!(validate_repository_observation(&abort, &approved, &approved).is_ok());
    }
}
