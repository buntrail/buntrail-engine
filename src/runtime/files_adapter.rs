use super::*;
pub(super) fn text_path(path: &TargetPath) -> Result<String> {
    match path {
        TargetPath::PosixBytes(bytes) => String::from_utf8(bytes.clone()).map_err(|_| {
            fail(
                "unavailable",
                "This native consumer cannot represent a non-UTF-8 path",
            )
        }),
        _ => Err(fail("unavailable", "Remote target requires a POSIX path")),
    }
}
fn snapshot(request: &Value, encoding: Encoding) -> Result<DocumentSnapshot> {
    let text = string(request, "text")?;
    if text.len() > files::MAX_DOCUMENT_BYTES || text.contains('\0') {
        return Err(fail(
            "too_large",
            "Text exceeds editing limits or contains NUL",
        ));
    }
    Ok(DocumentSnapshot {
        document: DocumentID([0; 16]),
        revision: BufferRevision(
            request
                .get("revision")
                .map(|_| number(request, "revision"))
                .transpose()?
                .unwrap_or(0),
        ),
        text: Arc::new(Text::from(text)),
        encoding,
        line_endings: LineEndings::Mixed,
        language: None,
    })
}
fn operation(request: &Value) -> Result<OperationID> {
    match request.get("operation") {
        Some(id) => {
            serde_json::from_value(id.clone()).map_err(|e| fail("invalid_request", e.to_string()))
        }
        None => Ok(OperationID(new_id().map_err(error)?)),
    }
}
fn completion(binding: FileBinding, receipt: OperationReceipt) -> Result<Value> {
    let version = match &receipt.state {
        OperationState::Confirmed(MutationResult::Saved(saved)) => {
            if saved.version.target != binding.file.target {
                return Err(fail("save_uncertain", "Save receipt target changed"));
            }
            Some(
                FileBinding {
                    version: saved.version.clone(),
                    ..binding
                }
                .token()
                .map_err(|e| fail("save_uncertain", e))?,
            )
        }
        _ => None,
    };
    let mut value = json!({"receipt":receipt});
    if let Some(version) = version {
        value["version"] = version.into();
    }
    Ok(value)
}
impl Runtime {
    fn file(remote: &RemoteServices, path: &str) -> Result<FileRef> {
        Ok(FileRef {
            target: remote.target().clone(),
            path: TargetPath::posix(path.as_bytes().to_vec()).map_err(error)?,
            resolved_identity: None,
        })
    }
    fn binding(remote: &RemoteServices, request: &Value) -> Result<FileBinding> {
        let binding = Services::file_binding(string(request, "version")?)
            .map_err(|e| fail("wrong_file", e))?;
        if binding.file.target != *remote.target()
            || binding.file.path != Self::file(remote, string(request, "path")?)?.path
        {
            return Err(fail(
                "wrong_file",
                "File binding does not belong to this verified environment/path",
            ));
        }
        Ok(binding)
    }
    pub(super) fn files_call(&self, remote: &RemoteServices, request: &Value) -> Result<Value> {
        match string(request, "command")? {
            "fs.list" => {
                let entries = run(remote.list(Self::file(remote, string(request, "path")?)?))
                    .map_err(error)?;
                let mut values=entries.into_iter().map(|entry|Ok(json!({"name":entry.display_name,"path":text_path(&entry.file.path)?,"directory":entry.kind==FileKind::Directory}))).collect::<Result<Vec<_>>>()?;
                values.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
                Ok(json!({"entries":values}))
            }
            "fs.observe" => {
                let file = Self::file(remote, string(request, "path")?)?;
                let version = remote.path_version(file.clone()).map_err(error)?;
                Ok(
                    json!({"observed":FileBinding {file,version,encoding:Encoding::Utf8 {bom:false}}.token().map_err(|e|fail("service_error",e))?}),
                )
            }
            "fs.read" => {
                let snapshot = run(remote.read(Self::file(remote, string(request, "path")?)?))
                    .map_err(error)?;
                let _budget = files::LoadBudget::acquire(
                    (snapshot.size as usize).saturating_mul(7) / 2 + 1024 * 1024,
                )
                .map_err(error)?;
                let bytes = remote
                    .source_bytes(&snapshot.source, files::MAX_DOCUMENT_BYTES * 2 + 4, true)
                    .map_err(error)?;
                let (text, encoding) = files::decode(&bytes).map_err(error)?;
                if text.len() > files::MAX_DOCUMENT_BYTES {
                    return Err(fail("too_large", "Decoded text exceeds 64 MiB"));
                }
                let binding = FileBinding {
                    file: snapshot.file,
                    version: snapshot.version,
                    encoding,
                };
                Ok(
                    json!({"text":text,"version":binding.token().map_err(|e|fail("service_error",e))?}),
                )
            }
            "fs.write" => {
                let binding = Self::binding(remote, request)?;
                let snapshot = snapshot(request, binding.encoding.clone())?;
                let _budget = files::reserve_encoding(&snapshot).map_err(error)?;
                let bytes = files::encode(&snapshot).map_err(error)?;
                let receipt = run(remote.save(SaveRequest {
                    operation: operation(request)?,
                    file: binding.file.clone(),
                    expected_version: Some(binding.version.clone()),
                    submitted_revision: snapshot.revision,
                    source: ByteSource::Memory(bytes.into()),
                }));
                completion(binding, receipt)
            }
            "fs.inspect" => {
                let binding = Self::binding(remote, request)?;
                let receipt = run(remote.inspect(
                    serde_json::from_value(request["operation"].clone())
                        .map_err(|e| fail("invalid_request", e.to_string()))?,
                    binding.file.target.clone(),
                ))
                .map_err(|e| fail("save_uncertain", e.message))?;
                completion(binding, receipt)
            }
            "fs.create" | "fs.mkdir" | "fs.rename" | "fs.remove" => {
                let command = string(request, "command")?;
                let file = Self::file(remote, string(request, "path")?)?;
                let encoding =
                    if let Some(value) = request.get("source_version").filter(|v| !v.is_null()) {
                        let version = value.as_str().ok_or_else(|| {
                            fail("invalid_request", "source_version must be a string or null")
                        })?;
                        let binding =
                            Services::file_binding(version).map_err(|e| fail("wrong_file", e))?;
                        if binding.file.target != *remote.target() {
                            return Err(fail(
                                "wrong_file",
                                "Source encoding belongs to another environment",
                            ));
                        }
                        binding.encoding
                    } else {
                        Encoding::Utf8 { bom: false }
                    };
                let _budget;
                let mutation = match command {
                    "fs.create" => {
                        let snapshot = snapshot(request, encoding.clone())?;
                        _budget = files::reserve_encoding(&snapshot).map_err(error)?;
                        FileMutation::Create {
                            source: ByteSource::Memory(
                                files::encode(&snapshot).map_err(error)?.into(),
                            ),
                        }
                    }
                    "fs.mkdir" => FileMutation::CreateDirectory,
                    "fs.rename" => FileMutation::Move {
                        destination: Self::file(remote, string(request, "to")?)?.path,
                    },
                    _ => FileMutation::Remove,
                };
                let expected = if matches!(command, "fs.create" | "fs.mkdir") {
                    ExpectedPathState::Absent
                } else {
                    ExpectedPathState::Present(if let Some(observed) = request.get("observed") {
                        let binding =
                            Services::file_binding(observed.as_str().ok_or_else(|| {
                                fail("invalid_request", "observed must be a token")
                            })?)
                            .map_err(|e| fail("invalid_request", e))?;
                        if binding.file.target != file.target || binding.file.path != file.path {
                            return Err(fail(
                                "wrong_file",
                                "Observed node belongs to another target/path",
                            ));
                        }
                        binding.version
                    } else {
                        remote.path_version(file.clone()).map_err(error)?
                    })
                };
                let receipt = run(FileService::mutate(
                    remote,
                    FileMutationRequest {
                        operation: operation(request)?,
                        file,
                        expected,
                        command: mutation,
                    },
                ));
                match &receipt.state {
                    OperationState::Confirmed(MutationResult::FileChanged { file, version }) => {
                        let version = version
                            .as_ref()
                            .map(|version| {
                                FileBinding {
                                    file: file.clone(),
                                    version: version.clone(),
                                    encoding,
                                }
                                .token()
                            })
                            .transpose()
                            .map_err(|e| fail("service_error", e))?;
                        Ok(json!({"version":version,"receipt":receipt}))
                    }
                    OperationState::Failed(failed)
                        if failed.effect == FailureEffect::ProvenNoEffect =>
                    {
                        Err(error(failed.error.clone()))
                    }
                    _ => Err(fail(
                        "operation_uncertain",
                        format!(
                            "File operation {} is uncertain; inspect its receipt before retrying",
                            buntrail_protocol::services::uuid(receipt.id.0)
                        ),
                    )),
                }
            }
            _ => Err(fail(
                "unavailable",
                "This operation is unavailable for the verified remote service",
            )),
        }
    }
}
