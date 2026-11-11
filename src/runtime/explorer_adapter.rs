use super::*;
use buntrail_core::workspaces::{FileRecoveryKind, FileRecoveryReference};
use sha2::{Digest, Sha256};

impl Runtime {
    pub(super) fn explorer_call(&self, route: Option<&str>, r: &Value) -> Result<Value> {
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
        let service: &dyn FileService = remote
            .as_ref()
            .map_or(local.files.as_ref() as &dyn FileService, |s| s.as_ref());
        let command = string(r, "command")?;
        if command == "fs.explorer_capabilities" {
            let mut capabilities = json!({"trash":remote.as_ref().is_none_or(|s| s.negotiated_capabilities().iter().any(|c| c=="file.trash")),"import":true});
            if let Some(root) = r.get("root").and_then(Value::as_str) {
                let resolved = run(service.resolve(
                    FileRef {
                        target,
                        path: TargetPath::posix(root.as_bytes().to_vec()).map_err(error)?,
                        resolved_identity: None,
                    },
                    FileResolvePurpose::Root,
                ))
                .map_err(error)?;
                capabilities["root_path"] =
                    json!(files_adapter::text_path(&resolved.resolved_path)?);
            }
            return Ok(capabilities);
        }
        if command == "fs.pending" || command == "fs.inspect_operation" {
            let entries = local
                .store
                .load()
                .map_err(error)?
                .records
                .into_iter()
                .flat_map(|w| w.file_recoveries)
                .filter(|p| p.file.target == target)
                .collect::<Vec<_>>();
            if command == "fs.pending" {
                return Ok(json!({"entries":entries}));
            }
            let operation: OperationID = serde_json::from_value(r["operation"].clone())
                .map_err(|e| fail("invalid_request", e.to_string()))?;
            let reference = entries
                .into_iter()
                .find(|p| p.operation == operation)
                .ok_or_else(|| {
                    fail(
                        "not_found",
                        "No retained file operation for this verified target",
                    )
                })?;
            let receipt = match &remote {
                Some(s) => run(s.inspect(operation, target)),
                None => run(local.files.inspect(operation, target)),
            }
            .map_err(error)?;
            let resolved = matches!(
                receipt.state,
                OperationState::Confirmed(_)
                    | OperationState::Failed(OperationFailure {
                        effect: FailureEffect::ProvenNoEffect,
                        ..
                    })
            );
            if resolved {
                local
                    .store
                    .resolve_file_recovery(reference.owner.workspace, &reference)
                    .map_err(error)?;
            }
            return Ok(json!({"receipt":receipt,"resolved":resolved}));
        }
        if command == "fs.import" && route.is_some() {
            return Err(fail(
                "invalid_request",
                "Import must originate on the local engine",
            ));
        }
        // Import's destination route is explicit; the source always belongs to this Mac.
        if command == "fs.import"
            && let Some(id) = r.get("remote").and_then(Value::as_str)
        {
            let mut request = r.clone();
            request.as_object_mut().unwrap().remove("remote");
            request["command"] = json!("fs.import_destination");
            return self.explorer_call(Some(id), &request);
        }
        let workspace = self.terminal_workspace(route, r)?;
        let owner = workspace.lock().unwrap().owner.clone();
        let path = string(r, "path")?;
        let root = std::path::Path::new(string(r, "root")?);
        let requested = std::path::Path::new(path);
        if !requested.is_absolute()
            || !requested.starts_with(root)
            || requested == root
            || requested
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(fail(
                "invalid_request",
                "File action must target a child of this workspace",
            ));
        }
        let file = FileRef {
            target: target.clone(),
            path: TargetPath::posix(path.as_bytes().to_vec()).map_err(error)?,
            resolved_identity: None,
        };
        let operation = r
            .get("operation")
            .map(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|e| fail("invalid_request", e.to_string()))
            })
            .transpose()?
            .unwrap_or(OperationID(new_id().map_err(error)?));
        let creating = matches!(
            command,
            "fs.create" | "fs.mkdir" | "fs.import" | "fs.import_destination"
        );
        let expected = if creating {
            ExpectedPathState::Absent
        } else {
            let observed = Services::file_binding(string(r, "observed")?)
                .map_err(|e| fail("wrong_file", e))?;
            if observed.file.target != target || observed.file.path != file.path {
                return Err(fail(
                    "wrong_file",
                    "Observed file belongs to another target/path",
                ));
            }
            ExpectedPathState::Present(observed.version)
        };
        let mut destination = None;
        let mut _preparation_budget = None;
        let (kind, mutation) = match command {
            "fs.import" | "fs.import_destination" => {
                let binding = Services::file_binding(string(r, "source_observed")?)
                    .map_err(|e| fail("wrong_file", e))?;
                let source_path = string(r, "source_path")?;
                if binding.file.target != local.identity
                    || files_adapter::text_path(&binding.file.path)? != source_path
                {
                    return Err(fail("wrong_file", "Import source identity differs"));
                }
                if !std::fs::symlink_metadata(source_path)
                    .map_err(|e| fail("service_error", e.to_string()))?
                    .is_file()
                {
                    return Err(fail(
                        "invalid_request",
                        "Only regular files can be imported; symlinks and folders are refused",
                    ));
                }
                let snapshot = local.files.read_sync(binding.file.clone()).map_err(error)?;
                if snapshot.version != binding.version {
                    return Err(fail("stale_revision", "Import source changed"));
                }
                _preparation_budget = Some(
                    files::LoadBudget::acquire(
                        (snapshot.size as usize)
                            .saturating_mul(2)
                            .saturating_add(1024 * 1024),
                    )
                    .map_err(error)?,
                );
                let bytes = local
                    .files
                    .source_bytes(&snapshot.source, files::MAX_ENCODED_BYTES, true)
                    .map_err(error)?;
                (
                    FileRecoveryKind::Import,
                    FileMutation::Create {
                        source: ByteSource::Memory(bytes.into()),
                    },
                )
            }
            "fs.create" => {
                let text = string(r, "text")?;
                if text.len() > files::MAX_DOCUMENT_BYTES || text.contains('\0') {
                    return Err(fail(
                        "invalid_request",
                        "Text exceeds editing limits or contains NUL",
                    ));
                }
                _preparation_budget = Some(
                    files::LoadBudget::acquire(
                        text.len().saturating_mul(2).saturating_add(1024 * 1024),
                    )
                    .map_err(error)?,
                );
                (
                    FileRecoveryKind::Create,
                    FileMutation::Create {
                        source: ByteSource::Memory(text.as_bytes().to_vec().into()),
                    },
                )
            }
            "fs.mkdir" => (
                FileRecoveryKind::CreateDirectory,
                FileMutation::CreateDirectory,
            ),
            "fs.rename" => {
                let to = std::path::Path::new(string(r, "to")?);
                if !to.is_absolute()
                    || to == root
                    || to
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    return Err(fail(
                        "invalid_request",
                        "Move destination must be an absolute path on the verified target",
                    ));
                }
                let to = TargetPath::posix(string(r, "to")?.as_bytes().to_vec()).map_err(error)?;
                destination = Some(to.clone());
                (
                    FileRecoveryKind::Move,
                    FileMutation::Move { destination: to },
                )
            }
            "fs.remove" => (FileRecoveryKind::Remove, FileMutation::Remove),
            "fs.trash" => (FileRecoveryKind::Trash, FileMutation::Trash),
            _ => return Err(fail("invalid_request", "Unsupported sidebar mutation")),
        };
        let request = FileMutationRequest {
            operation,
            file: file.clone(),
            expected,
            command: mutation,
        };
        // Fingerprint bounded metadata plus the content digest, never JSON-expand imported bytes.
        let content_digest = match &request.command {
            FileMutation::Create {
                source: ByteSource::Memory(bytes),
            } => Some(<[u8; 32]>::from(Sha256::digest(bytes))),
            _ => None,
        };
        let request_digest = Sha256::digest(
            serde_json::to_vec(&(
                &request.file,
                &request.expected,
                &kind,
                &destination,
                content_digest,
            ))
            .map_err(|e| fail("invalid_request", e.to_string()))?,
        )
        .into();
        let reference = FileRecoveryReference {
            operation,
            file,
            destination,
            kind,
            request_digest,
            owner,
        };
        let (_, fresh) = local
            .store
            .retain_file_recovery(reference.owner.workspace, reference.clone())
            .map_err(error)?;
        if !fresh {
            return Err(fail(
                "operation_uncertain",
                "Operation already submitted; inspect its original outcome",
            ));
        }
        let receipt = run(service.mutate(request));
        let resolved = matches!(
            receipt.state,
            OperationState::Failed(OperationFailure {
                effect: FailureEffect::ProvenNoEffect,
                ..
            })
        );
        // Confirmed effects remain discoverable until the frontend has applied them and acknowledges.
        if resolved {
            local
                .store
                .resolve_file_recovery(reference.owner.workspace, &reference)
                .map_err(error)?;
        }
        Ok(json!({"receipt":receipt,"resolved":resolved}))
    }
}
