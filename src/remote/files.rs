use super::*;
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::ffi::OsStrExt,
};
impl RemoteServices {
    pub(crate) fn source_bytes(
        &self,
        source: &ByteSource,
        maximum: usize,
        verify: bool,
    ) -> Result<Vec<u8>, CoreError> {
        buntrail_services::files::document_io::source_bytes(&self.spools, source, maximum, verify)
    }
    pub fn load_snapshot(
        &self,
        snapshot: FileSnapshot,
        document: DocumentID,
        recovery: Option<&buntrail_services::recovery::store::RecoveryStore>,
    ) -> Result<
        (
            buntrail_core::documents::LoadedDocument,
            buntrail_services::files::LoadBudget,
        ),
        CoreError,
    > {
        self.check(&snapshot.file.target)?;
        buntrail_services::files::document_io::load_snapshot(
            snapshot,
            document,
            recovery,
            &self.spools,
        )
    }
    pub fn save_prepared(
        &self,
        recovery: &buntrail_services::recovery::store::RecoveryStore,
        prepared: buntrail_core::documents::PreparedSave,
    ) -> OperationReceipt {
        let operation = prepared.operation;
        let target = prepared.recovery.file.target.clone();
        if let Err(error) = self.check(&target) {
            return buntrail_services::files::failure(operation, target, error);
        }
        match buntrail_services::files::document_io::prepare_save(recovery, prepared) {
            Ok((request, _budget)) => self.upload(request, false),
            Err(error) => buntrail_services::files::failure(operation, target, error),
        }
    }
    fn read_sync(&self, mut file: FileRef) -> Result<FileSnapshot, CoreError> {
        self.check(&file.target)?;
        let mut request = self.transport.start(
            Message::Read {
                path_base64: w::path(&file.path)?,
            },
            None,
            None,
        )?;
        let begin = request.next()?;
        let Message::ReadBegin {
            transfer_id,
            version,
            total_length,
            path_base64,
        } = begin.header.message
        else {
            return Err(response_error(begin.header.message));
        };
        if path_base64 != w::path(&file.path)?
            || begin.header.stream_id.as_deref() != Some(&transfer_id)
        {
            return Err(w::invalid());
        }
        if total_length > buntrail_services::files::MAX_ENCODED_BYTES as u64 {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "Remote file exceeds bounded spool capacity",
            ));
        }
        let version: buntrail_protocol::types::WireVersion =
            serde_json::from_str(&version).map_err(io_error)?;
        let version = version.core(&self.target)?;
        let (mut spool, path, lease) = self.spools.create_spool()?;
        let mut verifier =
            buntrail_protocol::frame::TransferVerifier::new(transfer_id.clone(), total_length)
                .map_err(|_| w::invalid())?;
        let mut prefix = Vec::new();
        loop {
            let f = request.next()?;
            if f.header.stream_id.as_deref() != Some(&transfer_id) {
                return Err(w::invalid());
            }
            match f.header.message {
                Message::ReadChunk {
                    transfer_id: ref tid,
                    offset,
                } => {
                    verifier
                        .chunk(tid, offset, &f.payload)
                        .map_err(|_| w::invalid())?;
                    prefix.extend_from_slice(&f.payload[..f.payload.len().min(4 - prefix.len())]);
                    spool.write_all(&f.payload).map_err(io_error)?;
                    request.credit(&transfer_id, f.payload.len())?;
                }
                Message::ReadEnd {
                    transfer_id: tid,
                    sha256,
                } => {
                    let digest = w::digest(&sha256)?;
                    verifier.end(&tid, digest).map_err(|_| w::invalid())?;
                    if digest != version.content_digest {
                        return Err(w::invalid());
                    }
                    request.finish();
                    break;
                }
                m => return Err(response_error(m)),
            }
        }
        spool.sync_all().map_err(io_error)?;
        file.resolved_identity = Some(version.file_identity.clone());
        let encoding = if prefix.starts_with(&[0xff, 0xfe]) {
            Encoding::Other("UTF-16LE".into())
        } else if prefix.starts_with(&[0xfe, 0xff]) {
            Encoding::Other("UTF-16BE".into())
        } else {
            Encoding::Utf8 {
                bom: prefix.starts_with(&[0xef, 0xbb, 0xbf]),
            }
        };
        Ok(FileSnapshot {
            file,
            version: version.clone(),
            size: total_length,
            encoding,
            source: ByteSource::PrivateSpool {
                local_path: TargetPath::posix(path.as_os_str().as_bytes().to_vec())?,
                length: total_length,
                digest: version.content_digest,
                lease: Some(lease),
            },
        })
    }
    fn upload(&self, r: SaveRequest, create: bool) -> OperationReceipt {
        let id = r.operation;
        let target = r.file.target.clone();
        let mut commit_submitted = false;
        let outcome = (|| -> Result<OperationReceipt, CoreError> {
            self.check(&r.file.target)?;
            if !create
                && !self
                    .negotiated_capabilities()
                    .iter()
                    .any(|cap| cap == "file.save_proof")
            {
                return Err(CoreError::new(
                    ErrorCode::Unavailable,
                    "active helper lacks authenticated save provenance; preserve its existing sessions",
                ));
            }
            if let Some(v) = &r.expected_version {
                self.check(&v.target)?;
            }
            let (mut input, length): (Box<dyn Read>, u64) = match &r.source {
                ByteSource::Memory(b) => {
                    (Box::new(std::io::Cursor::new(b.clone())), b.len() as u64)
                }
                ByteSource::PrivateSpool {
                    local_path,
                    length,
                    lease,
                    ..
                } => {
                    let TargetPath::PosixBytes(bytes) = local_path else {
                        return Err(w::invalid());
                    };
                    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(bytes));
                    if lease.is_none() || self.spools.retain(path).is_none() {
                        return Err(CoreError::new(
                            ErrorCode::Unavailable,
                            "Upload source is not retained by this connection",
                        ));
                    }
                    (Box::new(File::open(path).map_err(io_error)?), *length)
                }
            };
            let transfer = w::uuid(buntrail_services::connections::local::new_id()?);
            let mut request = self.transport.start(
                Message::UploadBegin {
                    transfer_id: transfer.clone(),
                    path_base64: w::path(&r.file.path)?,
                    total_length: length,
                },
                Some(id),
                Some(transfer.clone()),
            )?;
            match request.next()?.header.message {
                Message::Ack {} => {}
                message => return Err(response_error(message)),
            }
            let mut offset = 0;
            let mut hash = sha2::Sha256::new();
            let mut chunk = [0; 65536];
            while offset < length {
                let n = (length - offset).min(65536) as usize;
                input.read_exact(&mut chunk[..n]).map_err(io_error)?;
                hash.update(&chunk[..n]);
                request.send(
                    Message::UploadChunk {
                        transfer_id: transfer.clone(),
                        offset,
                    },
                    chunk[..n].to_vec(),
                )?;
                match request.next()?.header.message {
                    Message::Credit { bytes } if bytes as usize == n => {}
                    m => return Err(response_error(m)),
                }
                offset += n as u64;
            }
            if input.read(&mut [0; 1]).map_err(io_error)? != 0 {
                return Err(w::invalid());
            }
            let digest = w::hex(&hash.finalize());
            if let ByteSource::PrivateSpool {
                digest: expected, ..
            } = &r.source
                && digest != w::hex(expected)
            {
                return Err(w::invalid());
            }
            request.send(
                Message::UploadEnd {
                    transfer_id: transfer.clone(),
                    sha256: digest,
                },
                vec![],
            )?;
            match request.next()?.header.message {
                Message::Ack {} => {}
                message => return Err(response_error(message)),
            }
            request.send(
                if create {
                    Message::CreateCommit {
                        transfer_id: transfer,
                    }
                } else {
                    Message::SaveCommitOwned {
                        transfer_id: transfer,
                        lease_id: w::uuid(self.lease.lease.0),
                        submitted_revision: r.submitted_revision.0,
                        expected_version: r
                            .expected_version
                            .as_ref()
                            .map(|v| serde_json::to_string(&w::version(v)).map_err(io_error))
                            .transpose()?,
                    }
                },
                vec![],
            )?;
            commit_submitted = true;
            if create {
                let mut replies = Replies {
                    request,
                    stream_id: None,
                    bytes: Vec::new(),
                    length: None,
                    hash: None,
                    sequence: 0,
                    done: false,
                };
                loop {
                    let f = replies.request.next()?;
                    if matches!(f.header.message, Message::Receipt { .. }) {
                        let receipt = legacy_receipt(id, &target, f.header.message)?;
                        replies.request.finish();
                        return Ok(receipt);
                    }
                    if let Some(result) = replies.accept(f)? {
                        return match result? {
                            w::Reply::Receipt { receipt } => receipt.core(&target),
                            _ => Err(w::invalid()),
                        };
                    }
                }
            }
            let message = request.next()?.header.message;
            request.finish();
            legacy_receipt(id, &target, message)
        })();
        outcome.unwrap_or_else(|error| {
            if commit_submitted {
                super::operations::uncertain(id, target, error)
            } else {
                buntrail_services::files::failure(id, target, error)
            }
        })
    }
}
fn response_error(m: Message) -> CoreError {
    match m {
        Message::Error { code, safe_message } => w::core_error(&code, &safe_message),
        _ => w::invalid(),
    }
}
pub(super) fn legacy_receipt(
    id: OperationID,
    t: &TargetIdentity,
    m: Message,
) -> Result<OperationReceipt, CoreError> {
    use buntrail_protocol::types::{ReceiptState as S, WireEffect, WireObservation as O};
    let Message::Receipt {
        state,
        saved_version,
        saved_revision,
        safe_message,
        failure,
        observed,
    } = m
    else {
        return Err(response_error(m));
    };
    let observed = match observed {
        O::Unobserved => ObservedState::Unobserved,
        O::File {
            path_base64,
            version,
            digest,
        } => ObservedState::File {
            file: FileRef {
                target: t.clone(),
                path: w::target_path(&path_base64)?,
                resolved_identity: None,
            },
            version: version.as_ref().map(|v| v.core(t)).transpose()?,
            digest: digest.as_deref().map(w::digest).transpose()?,
        },
    };
    Ok(OperationReceipt {
        id,
        state: match state {
            S::Accepted => OperationState::Accepted,
            S::Running => OperationState::Running,
            S::Confirmed => OperationState::Confirmed(MutationResult::Saved(SaveResult {
                saved_revision: BufferRevision(saved_revision.ok_or_else(w::invalid)?),
                version: saved_version.ok_or_else(w::invalid)?.core(t)?,
            })),
            S::Failed => {
                let f = failure.ok_or_else(w::invalid)?;
                OperationState::Failed(OperationFailure {
                    error: w::core_error(&f.code, &safe_message),
                    target: t.clone(),
                    observed,
                    effect: if f.effect == WireEffect::ProvenNoEffect {
                        FailureEffect::ProvenNoEffect
                    } else {
                        FailureEffect::PossiblePartialEffects
                    },
                })
            }
            S::Unknown => OperationState::Unknown(ReconciliationEvidence {
                target: t.clone(),
                observed,
                explanation: safe_message,
            }),
        },
    })
}
impl FileService for RemoteServices {
    fn resolve(
        &self,
        root: FileRef,
        purpose: FileResolvePurpose,
    ) -> ServiceFuture<'_, Result<FileResolution, CoreError>> {
        Box::pin(async move {
            self.check(&root.target)?;
            if !self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == "file.resolve")
            {
                return Err(CoreError::new(
                    ErrorCode::Unavailable,
                    "Target path resolution is unavailable",
                ));
            }
            match self.call(
                w::Call::FileResolve {
                    root: w::File::from_core(&root)?,
                    purpose: w::ResolvePurpose::from_core(&purpose)?,
                },
                None,
            )? {
                w::Reply::FileResolved {
                    root,
                    file,
                    resolved_path,
                    node_kind,
                    version,
                } => Ok(FileResolution {
                    root: root.core(&self.target)?,
                    file: file.core(&self.target)?,
                    resolved_path: w::target_path(&resolved_path)?,
                    kind: node_kind
                        .map(|k| match k.as_str() {
                            "File" => Ok(FileKind::File),
                            "Directory" => Ok(FileKind::Directory),
                            "Symlink" => Ok(FileKind::Symlink),
                            "Other" => Ok(FileKind::Other),
                            _ => Err(w::invalid()),
                        })
                        .transpose()?,
                    version: version.map(|v| v.core(&self.target)).transpose()?,
                }),
                _ => Err(w::invalid()),
            }
        })
    }
    fn list(&self, file: FileRef) -> ServiceFuture<'_, Result<Vec<FileEntry>, CoreError>> {
        Box::pin(async move {
            self.check(&file.target)?;
            let w::Reply::List { entries } = self.call(
                w::Call::List {
                    file: w::File::from_core(&file)?,
                },
                None,
            )?
            else {
                return Err(w::invalid());
            };
            entries
                .into_iter()
                .map(|e| {
                    Ok(FileEntry {
                        file: e.file.core(&self.target)?,
                        display_name: e.display_name,
                        kind: match e.kind.as_str() {
                            "File" => FileKind::File,
                            "Directory" => FileKind::Directory,
                            "Symlink" => FileKind::Symlink,
                            "Other" => FileKind::Other,
                            _ => return Err(w::invalid()),
                        },
                    })
                })
                .collect()
        })
    }
    fn read(&self, file: FileRef) -> ServiceFuture<'_, Result<FileSnapshot, CoreError>> {
        Box::pin(async move { self.read_sync(file) })
    }
    fn save(&self, r: SaveRequest) -> ServiceFuture<'_, OperationReceipt> {
        Box::pin(async move { self.upload(r, false) })
    }
    fn mutate(&self, r: FileMutationRequest) -> ServiceFuture<'_, OperationReceipt> {
        Box::pin(async move {
            let id = r.operation;
            let target = r.file.target.clone();
            if let FileMutation::Create { source } = r.command {
                if r.expected != ExpectedPathState::Absent {
                    return buntrail_services::files::failure(id, target, w::invalid());
                }
                return self.upload(
                    SaveRequest {
                        operation: id,
                        file: r.file,
                        expected_version: None,
                        submitted_revision: BufferRevision(0),
                        source,
                    },
                    true,
                );
            }
            let result = (|| -> Result<OperationReceipt, CoreError> {
                self.check(&target)?;
                let file = w::File::from_core(&r.file)?;
                let command = match r.command {
                    FileMutation::CreateDirectory => w::FileCommand::CreateDirectory,
                    FileMutation::Rename { destination } => w::FileCommand::Rename {
                        destination: w::path(&destination)?,
                    },
                    FileMutation::Move { destination } => w::FileCommand::Move {
                        destination: w::path(&destination)?,
                    },
                    FileMutation::Remove => {
                        if !self
                            .negotiated_capabilities()
                            .iter()
                            .any(|c| c == "file.remove")
                        {
                            return Err(CoreError::new(
                                ErrorCode::Unavailable,
                                "Active helper lacks nonrecursive removal; preserve its sessions and update after they end",
                            ));
                        }
                        w::FileCommand::Remove {
                            confirmed_target: self.transport.context.target_binding.clone(),
                            confirmed_path: file.path_base64.clone(),
                        }
                    }
                    FileMutation::Delete => w::FileCommand::Delete {
                        confirmed_target: self.transport.context.target_binding.clone(),
                        confirmed_path: file.path_base64.clone(),
                    },
                    FileMutation::Trash => {
                        if !self
                            .negotiated_capabilities()
                            .iter()
                            .any(|c| c == "file.trash")
                        {
                            return Err(CoreError::new(
                                ErrorCode::Unavailable,
                                "Active helper lacks Trash; update after retained sessions end",
                            ));
                        }
                        w::FileCommand::Trash {
                            confirmed_target: self.transport.context.target_binding.clone(),
                            confirmed_path: file.path_base64.clone(),
                        }
                    }
                    FileMutation::Create { .. } => unreachable!(),
                };
                let expected = match r.expected {
                    ExpectedPathState::Absent => None,
                    ExpectedPathState::Present(v) => {
                        self.check(&v.target)?;
                        Some(w::version(&v))
                    }
                };
                Ok(self.mutation_call(
                    w::Call::FileMutate {
                        file,
                        expected,
                        command,
                    },
                    id,
                    &target,
                ))
            })();
            result.unwrap_or_else(|e| buntrail_services::files::failure(id, target, e))
        })
    }
    fn watch(&self, file: FileRef) -> ServiceFuture<'_, Result<EventStream<FileEvent>, CoreError>> {
        Box::pin(async move {
            self.check(&file.target)?;
            Ok(Box::pin(Watch {
                source: self.stream(
                    w::Call::Watch {
                        file: w::File::from_core(&file)?,
                    },
                    None,
                )?,
                target: self.target.clone(),
            }) as EventStream<FileEvent>)
        })
    }
}
struct Watch {
    source: Replies,
    target: TargetIdentity,
}
impl Stream for Watch {
    type Item = Result<FileEvent, CoreError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.source).poll_next(cx);
        result.map(|v| {
            v.map(|r| {
                r.and_then(|r| {
                    let w::Reply::Watch { file, event } = r else {
                        return Err(w::invalid());
                    };
                    let file = file.core(&self.target)?;
                    Ok(match event.as_str() {
                        "changed" => FileEvent::Changed(file),
                        "removed" => FileEvent::Removed(file),
                        "rescan_required" => FileEvent::RescanRequired(file),
                        _ => return Err(w::invalid()),
                    })
                })
            })
        })
    }
}

/// An immutable effective remote Save request, frozen before its approval is published.
/// Existing remote Save wire semantics omit FileRef hints; registered aliases retain theirs.
pub struct RemoteOwnedSave {
    remote: RemoteServices,
    request: SaveRequest,
    approval: GitSaveApproval,
    _budget: buntrail_services::files::LoadBudget,
}
impl RemoteOwnedSave {
    pub fn approval(&self) -> &GitSaveApproval {
        &self.approval
    }
    pub fn execute(self) -> OperationReceipt {
        if let Err(error) = self.remote.owned_save_ready() {
            return buntrail_services::files::failure(
                self.request.operation,
                self.request.file.target,
                error,
            );
        }
        self.remote.upload(self.request, false)
    }
}
impl RemoteServices {
    fn owned_save_ready(&self) -> Result<(), CoreError> {
        if !self.transport.owns_registration(&self.lease) {
            return Err(CoreError::new(
                ErrorCode::Closed,
                "exact remote save registration is no longer active",
            ));
        }
        if !self
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "file.save_proof")
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "active helper lacks authenticated save provenance; preserve its existing sessions",
            ));
        }
        Ok(())
    }
    pub fn prepare_owned_save(&self, request: SaveRequest) -> Result<RemoteOwnedSave, CoreError> {
        self.owned_save_ready()?;
        let length = match &request.source {
            ByteSource::Memory(b) => b.len() as u64,
            ByteSource::PrivateSpool { length, .. } => *length,
        };
        if length > (buntrail_services::files::MAX_DOCUMENT_BYTES * 2 + 4) as u64 {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "save source exceeds staging bound",
            ));
        }
        let budget =
            buntrail_services::files::LoadBudget::acquire(length as usize * 2 + 1024 * 1024)?;
        self.freeze_owned_save(request, budget)
    }
    pub fn prepare_owned_document(
        &self,
        recovery: &buntrail_services::recovery::store::RecoveryStore,
        prepared: buntrail_core::documents::PreparedSave,
    ) -> Result<RemoteOwnedSave, CoreError> {
        self.owned_save_ready()?;
        self.check(&prepared.recovery.file.target)?;
        let (request, budget) =
            buntrail_services::files::document_io::prepare_save(recovery, prepared)?;
        self.freeze_owned_save(request, budget)
    }
    fn freeze_owned_save(
        &self,
        mut request: SaveRequest,
        budget: buntrail_services::files::LoadBudget,
    ) -> Result<RemoteOwnedSave, CoreError> {
        self.owned_save_ready()?;
        self.check(&request.file.target)?;
        if let Some(expected) = &request.expected_version {
            self.check(&expected.target)?;
        }
        let bytes = match &request.source {
            ByteSource::Memory(bytes) => bytes.clone(),
            source => Arc::from(self.source_bytes(
                source,
                buntrail_services::files::MAX_DOCUMENT_BYTES * 2 + 4,
                true,
            )?),
        };
        request.file.resolved_identity = None;
        let approval = GitSaveApproval {
            operation: request.operation,
            actor: self.lease.clone(),
            request_digest: buntrail_services::files::save_request_digest(
                &request.file,
                &request.expected_version,
                request.submitted_revision,
                sha2::Sha256::digest(&bytes).into(),
            )?,
        };
        request.source = ByteSource::Memory(bytes);
        Ok(RemoteOwnedSave {
            remote: self.clone(),
            request,
            approval,
            _budget: budget,
        })
    }
}
