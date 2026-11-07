use super::*;
impl GitService for RemoteServices {
    fn read_query(
        &self,
        worktree: WorktreeIdentity,
        query: GitRead,
    ) -> ServiceFuture<'_, Result<GitReadOutput, CoreError>> {
        Box::pin(async move {
            self.check(&worktree.target)?;
            if !self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == "git.read")
            {
                return Err(CoreError::new(
                    ErrorCode::Unavailable,
                    "Active helper lacks typed Git inspection; preserve its retained sessions and update after they end",
                ));
            }
            match self.call(
                w::Call::GitRead {
                    worktree: w::Worktree::from_core(&worktree)?,
                    query,
                },
                None,
            )? {
                w::Reply::GitRead {
                    stdout_base64,
                    stderr_base64,
                    success,
                } => Ok(GitReadOutput {
                    stdout: B64.decode(stdout_base64).map_err(|_| w::invalid())?,
                    stderr: B64.decode(stderr_base64).map_err(|_| w::invalid())?,
                    success,
                }),
                _ => Err(w::invalid()),
            }
        })
    }

    fn existing_receipt(
        &self,
        r: GitMutationRequest,
    ) -> ServiceFuture<'_, Result<Option<OperationReceipt>, CoreError>> {
        Box::pin(async move {
            if self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == "git.original_lookup")
            {
                return self.existing_original(&r);
            }
            self.check(&r.worktree.target)?;
            if !self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == "git.existing")
            {
                return Err(CoreError::new(
                    ErrorCode::Unavailable,
                    "active helper cannot verify the original Git request; preserve the existing supervisor",
                ));
            }
            match self.call(
                w::Call::GitExisting {
                    worktree: w::Worktree::from_core(&r.worktree)?,
                    expected: w::GitState::from_core(&r.expected),
                    command: w::GitCommand::from_core(&r.command)?,
                },
                Some(r.operation),
            )? {
                w::Reply::GitExisting { receipt } => receipt
                    .map(|receipt| {
                        let receipt = receipt.core(&r.worktree.target)?;
                        if receipt.id != r.operation {
                            return Err(w::invalid());
                        }
                        Ok(receipt)
                    })
                    .transpose(),
                _ => Err(w::invalid()),
            }
        })
    }
    fn discover(
        &self,
        file: FileRef,
    ) -> ServiceFuture<'_, Result<Option<WorktreeIdentity>, CoreError>> {
        Box::pin(async move {
            self.check(&file.target)?;
            match self.call(
                w::Call::Discover {
                    file: w::File::from_core(&file)?,
                },
                None,
            )? {
                w::Reply::Discovered { worktree } => {
                    worktree.map(|w| w.core(&self.target)).transpose()
                }
                _ => Err(w::invalid()),
            }
        })
    }
    fn status(
        &self,
        worktree: WorktreeIdentity,
    ) -> ServiceFuture<'_, Result<GitSnapshot, CoreError>> {
        Box::pin(async move {
            self.check(&worktree.target)?;
            match self.call(
                w::Call::Status {
                    worktree: w::Worktree::from_core(&worktree)?,
                },
                None,
            )? {
                w::Reply::Status {
                    worktree,
                    state,
                    staged,
                    unstaged,
                    untracked,
                    conflicts,
                } => {
                    let entries = |v: Vec<w::GitEntry>| {
                        v.iter().map(w::GitEntry::core).collect::<Result<_, _>>()
                    };
                    Ok(GitSnapshot {
                        worktree: worktree.core(&self.target)?,
                        state: state.core()?,
                        staged: entries(staged)?,
                        unstaged: entries(unstaged)?,
                        untracked: entries(untracked)?,
                        conflicts: entries(conflicts)?,
                    })
                }
                _ => Err(w::invalid()),
            }
        })
    }
    fn diff(&self, r: DiffRequest) -> ServiceFuture<'_, Result<DiffSnapshot, CoreError>> {
        Box::pin(async move {
            self.check(&r.worktree.target)?;
            match self.call(
                w::Call::Diff {
                    worktree: w::Worktree::from_core(&r.worktree)?,
                    path_base64: w::path(&r.path)?,
                    source: w::DiffSource::from_core(&r.source),
                },
                None,
            )? {
                w::Reply::Diff {
                    source,
                    content,
                    unavailable,
                    change,
                    previous_path,
                    expected,
                    bytes_base64,
                } => Ok(DiffSnapshot {
                    source: source.core(),
                    content: match content.as_str() {
                        "Text" => DiffContent::Text,
                        "Binary" => DiffContent::Binary,
                        "Unavailable" => {
                            DiffContent::Unavailable(unavailable.ok_or_else(w::invalid)?)
                        }
                        _ => return Err(w::invalid()),
                    },
                    change: match change.as_str() {
                        "Modified" => DiffChange::Modified,
                        "Added" => DiffChange::Added,
                        "Deleted" => DiffChange::Deleted,
                        "Renamed" => DiffChange::Renamed,
                        "Conflicted" => DiffChange::Conflicted,
                        _ => return Err(w::invalid()),
                    },
                    previous_path: previous_path.as_deref().map(w::target_path).transpose()?,
                    expected: expected.core()?,
                    bytes: B64.decode(bytes_base64).map_err(|_| w::invalid())?,
                }),
                _ => Err(w::invalid()),
            }
        })
    }
    fn history(&self, r: HistoryQuery) -> ServiceFuture<'_, Result<GraphPage, CoreError>> {
        Box::pin(async move {
            self.check(&r.worktree.target)?;
            match self.call(
                w::Call::History {
                    worktree: w::Worktree::from_core(&r.worktree)?,
                    refs: r.ref_filter.iter().map(|r| B64.encode(r)).collect(),
                    cursor: r.cursor.map(|c| B64.encode(c)),
                },
                None,
            )? {
                w::Reply::History {
                    commits,
                    continuation_edges,
                    next_cursor,
                } => Ok(GraphPage {
                    commits: commits
                        .iter()
                        .map(w::Commit::core)
                        .collect::<Result<_, _>>()?,
                    continuation_edges,
                    next_cursor: next_cursor
                        .map(|c| B64.decode(c).map_err(|_| w::invalid()))
                        .transpose()?,
                }),
                _ => Err(w::invalid()),
            }
        })
    }
    fn mutate(&self, r: GitMutationRequest) -> ServiceFuture<'_, OperationReceipt> {
        Box::pin(async move {
            let id = r.operation;
            let target = r.worktree.target.clone();
            let result = (|| -> Result<OperationReceipt, CoreError> {
                self.check(&target)?;
                Ok(self.mutation_call(
                    w::Call::GitMutate {
                        worktree: w::Worktree::from_core(&r.worktree)?,
                        expected: w::GitState::from_core(&r.expected),
                        command: w::GitCommand::from_core(&r.command)?,
                    },
                    id,
                    &target,
                ))
            })();
            result.unwrap_or_else(|e| buntrail_services::files::failure(id, target, e))
        })
    }
}
