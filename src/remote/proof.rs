//! Worker-only proof transport. Events are provisional until observe_owned returns success.
use super::*;
use buntrail_protocol::{
    frame::{Frame, TransferVerifier},
    types::ProofAction,
};
use buntrail_services::git::proof_codec::{self, output::OutputReader};
pub use buntrail_services::git::{
    proof_codec::output::{EntryGroup, ObservationEvent, OwnedObservation},
    worktree_proof::WorktreePreparation,
};
use std::{
    io::{self, Read, Write},
    time::{Duration, Instant},
};

fn unavailable(message: &str) -> CoreError {
    CoreError::new(ErrorCode::Unavailable, message)
}
fn response(message: Message) -> CoreError {
    match message {
        Message::Error { code, safe_message } => w::core_error(&code, &safe_message),
        _ => w::invalid(),
    }
}
struct Count(u64);
impl Write for Count {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(b.len() as u64)
            .ok_or_else(|| io::Error::other("proof length overflow"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum Part {
    Lookup,
    Original,
    Approvals,
    Aliases,
}
impl Part {
    fn chunk(self, id: String, offset: u64) -> Message {
        match self {
            Self::Lookup => Message::ProofLookupChunk {
                transfer_id: id,
                offset,
            },
            Self::Original => Message::ProofOriginalChunk {
                transfer_id: id,
                offset,
            },
            Self::Approvals => Message::ProofApprovalsChunk {
                transfer_id: id,
                offset,
            },
            Self::Aliases => Message::ProofAliasesChunk {
                transfer_id: id,
                offset,
            },
        }
    }
    fn end(self, id: String, sha256: String) -> Message {
        match self {
            Self::Lookup => Message::ProofLookupEnd {
                transfer_id: id,
                sha256,
            },
            Self::Original => Message::ProofOriginalEnd {
                transfer_id: id,
                sha256,
            },
            Self::Approvals => Message::ProofApprovalsEnd {
                transfer_id: id,
                sha256,
            },
            Self::Aliases => Message::ProofAliasesEnd {
                transfer_id: id,
                sha256,
            },
        }
    }
}
fn next(
    remote: &RemoteServices,
    request: &mut transport::Request,
    deadline: Instant,
    cancellable: bool,
) -> Result<Frame, CoreError> {
    let mut cx = Context::from_waker(std::task::Waker::noop());
    loop {
        if let Poll::Ready(frame) = request.poll(&mut cx) {
            let frame = frame?;
            if !matches!(frame.header.message, Message::ProofOutputBegin { .. })
                && frame.header.stream_id != request.header.stream_id
            {
                return Err(w::invalid());
            }
            return Ok(frame);
        }
        if cancellable {
            remote.check_cancellation()?;
        }
        if Instant::now() >= deadline {
            return Err(CoreError::new(
                ErrorCode::Unknown,
                "proof transfer absolute deadline exceeded; inspect original operation without replay",
            ));
        }
        std::thread::park_timeout(Duration::from_millis(2));
    }
}
struct Upload<'a> {
    remote: &'a RemoteServices,
    request: &'a mut transport::Request,
    part: Part,
    deadline: Instant,
    bytes: Vec<u8>,
    offset: u64,
    sequence: u64,
    hash: sha2::Sha256,
}
impl<'a> Upload<'a> {
    fn new(
        remote: &'a RemoteServices,
        request: &'a mut transport::Request,
        part: Part,
        deadline: Instant,
    ) -> Self {
        Self {
            remote,
            request,
            part,
            deadline,
            bytes: Vec::with_capacity(65536),
            offset: 0,
            sequence: 0,
            hash: sha2::Sha256::new(),
        }
    }
    fn send(&self, message: Message, payload: Vec<u8>) -> Result<(), CoreError> {
        if matches!(self.part, Part::Lookup) {
            self.request.send_control(message, payload)
        } else {
            self.request.send(message, payload)
        }
    }
    fn drain(&mut self) -> Result<(), CoreError> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        if !matches!(self.part, Part::Lookup) {
            self.remote.check_cancellation()?;
        }
        let id = self
            .request
            .header
            .stream_id
            .clone()
            .ok_or_else(w::invalid)?;
        self.request.header.sequence = Some(self.sequence);
        let payload = std::mem::replace(&mut self.bytes, Vec::with_capacity(65536));
        let length = payload.len();
        self.hash.update(&payload);
        self.send(self.part.chunk(id.clone(), self.offset), payload)?;
        let frame = next(
            self.remote,
            self.request,
            self.deadline,
            !matches!(self.part, Part::Lookup),
        )?;
        match frame.header.message {
            Message::Credit { bytes }
                if bytes as usize == length && frame.header.stream_id.as_ref() == Some(&id) => {}
            m => return Err(response(m)),
        }
        self.offset += length as u64;
        self.sequence = self.sequence.checked_add(1).ok_or_else(w::invalid)?;
        Ok(())
    }
    fn finish(mut self, length: u64) -> Result<(), CoreError> {
        self.drain()?;
        if self.offset != length {
            return Err(w::invalid());
        }
        self.request.header.sequence = Some(self.sequence);
        self.send(
            self.part.end(
                self.request
                    .header
                    .stream_id
                    .clone()
                    .ok_or_else(w::invalid)?,
                w::hex(&self.hash.clone().finalize()),
            ),
            Vec::new(),
        )?;
        self.request.header.sequence = None;
        Ok(())
    }
}
impl Write for Upload<'_> {
    fn write(&mut self, mut b: &[u8]) -> io::Result<usize> {
        let total = b.len();
        while !b.is_empty() {
            let n = b.len().min(65536 - self.bytes.len());
            self.bytes.extend_from_slice(&b[..n]);
            b = &b[n..];
            if self.bytes.len() == 65536 {
                self.drain().map_err(io::Error::other)?;
            }
        }
        Ok(total)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.drain().map_err(io::Error::other)
    }
}
fn approvals<'a>(
    out: &mut impl Write,
    remote: &RemoteServices,
    values: impl Iterator<Item = &'a GitSaveApproval>,
) -> Result<(), CoreError> {
    for a in values {
        if a.actor.client != remote.lease.client || a.actor.runtime != remote.lease.runtime {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "save approval client or runtime changed",
            ));
        }
        for b in [
            a.operation.0.as_slice(),
            a.request_digest.as_slice(),
            a.actor.client.0.as_slice(),
            a.actor.workspace.0.as_slice(),
            a.actor.lease.0.as_slice(),
        ] {
            out.write_all(b).map_err(io_error)?;
        }
    }
    Ok(())
}
struct Download<'a> {
    remote: &'a RemoteServices,
    request: &'a mut transport::Request,
    id: String,
    verifier: TransferVerifier,
    sequence: u64,
    buffer: std::io::Cursor<Vec<u8>>,
    seal: Option<[u8; 32]>,
    deadline: Instant,
}
impl Read for Download<'_> {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        if b.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.buffer.read(b)?;
            if n != 0 {
                return Ok(n);
            }
            if !self.buffer.get_ref().is_empty() {
                self.request
                    .credit(&self.id, self.buffer.get_ref().len())
                    .map_err(io::Error::other)?;
                self.buffer = std::io::Cursor::new(Vec::new());
            }
            if self.seal.is_some() {
                return Ok(0);
            }
            let frame =
                next(self.remote, self.request, self.deadline, true).map_err(io::Error::other)?;
            if frame.header.stream_id.as_ref() != Some(&self.id)
                || frame.header.sequence != Some(self.sequence)
            {
                return Err(io::Error::other("output owner or sequence mismatch"));
            }
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("output sequence overflow"))?;
            match frame.header.message {
                Message::ProofOutputChunk {
                    transfer_id,
                    offset,
                } => {
                    self.verifier
                        .chunk(&transfer_id, offset, &frame.payload)
                        .map_err(io::Error::other)?;
                    self.buffer = std::io::Cursor::new(frame.payload);
                }
                Message::ProofOutputEnd {
                    transfer_id,
                    sha256,
                } => {
                    let digest = w::digest(&sha256).map_err(io::Error::other)?;
                    self.verifier
                        .end(&transfer_id, digest)
                        .map_err(io::Error::other)?;
                    self.seal = Some(digest);
                }
                m => return Err(io::Error::other(response(m))),
            }
        }
    }
}
impl RemoteServices {
    fn proof_capability(&self) -> Result<(), CoreError> {
        if !self
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "git.worktree_proof")
        {
            return Err(unavailable(
                "active helper lacks git.worktree_proof; preserve its existing sessions",
            ));
        }
        Ok(())
    }
    /// Freeze the complete aliases under the caller's registration barrier. Events remain
    /// provisional until this method validates the complete codec and authenticated end seal.
    pub fn observe_owned<'a>(
        &self,
        worktree: WorktreeIdentity,
        aliases: impl Iterator<Item = &'a FileRef> + Clone,
        mut on_event: impl FnMut(ObservationEvent) -> Result<(), CoreError>,
    ) -> Result<OwnedObservation, CoreError> {
        self.proof_capability()?;
        self.check(&worktree.target)?;
        if !self.transport.owns_registration(&self.lease) {
            return Err(unavailable("exact observation registration changed"));
        }
        self.check_cancellation()?;
        let mut count = Count(0);
        proof_codec::write_aliases(&mut count, &self.target, aliases.clone())?;
        let id = w::uuid(buntrail_services::connections::local::new_id()?);
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut request = self.transport.start(
            Message::ProofObserveBegin {
                transfer_id: id.clone(),
                total_length: count.0,
                lease_id: w::uuid(self.lease.lease.0),
                worktree: w::Worktree::from_core(&worktree)?,
            },
            None,
            Some(id),
        )?;
        match next(self, &mut request, deadline, true)?.header.message {
            Message::Ack {} => {}
            m => return Err(response(m)),
        }
        let mut upload = Upload::new(self, &mut request, Part::Aliases, deadline);
        proof_codec::write_aliases(&mut upload, &self.target, aliases)?;
        upload.finish(count.0)?;
        let begin = next(self, &mut request, deadline, true)?;
        let Message::ProofOutputBegin {
            transfer_id,
            total_length,
        } = begin.header.message
        else {
            return Err(response(begin.header.message));
        };
        if begin.header.stream_id.as_ref() != Some(&transfer_id) {
            return Err(w::invalid());
        }
        request.header.stream_id = Some(transfer_id.clone());
        let mut download = Download {
            remote: self,
            request: &mut request,
            id: transfer_id.clone(),
            verifier: TransferVerifier::new(transfer_id, total_length).map_err(|_| w::invalid())?,
            sequence: 0,
            buffer: std::io::Cursor::new(Vec::new()),
            seal: None,
            deadline,
        };
        let mut output = OutputReader::new(&mut download, total_length, &self.target)?;
        let observation = output.observation_id();
        let result = (|| {
            while let Some(event) = output.next_event()? {
                on_event(event)?;
            }
            let digest = output.wire_digest();
            let summary = output.finish()?;
            Ok((summary, digest))
        })();
        let result = result.and_then(|(summary, digest)| {
            if digest.is_none() || digest != download.seal {
                Err(w::invalid())
            } else {
                Ok(summary)
            }
        });
        drop(download);
        if result.is_ok() {
            request.finish();
        }
        drop(request);
        if result.is_err() {
            let _ = self.release_observation(observation);
        }
        result
    }
    fn lookup_prefix(
        &self,
        original: &GitMutationRequest,
        action: ProofAction,
    ) -> Result<(transport::Request, Message, u64), CoreError> {
        self.check(&original.worktree.target)?;
        let mut count = Count(0);
        proof_codec::write_original(&mut count, original, &self.target)?;
        let id = w::uuid(buntrail_services::connections::local::new_id()?);
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut request = self.transport.start(
            Message::ProofLookupBegin {
                transfer_id: id.clone(),
                total_length: count.0,
                action,
                lease_id: w::uuid(self.lease.lease.0),
            },
            Some(original.operation),
            Some(id),
        )?;
        match next(self, &mut request, deadline, false)?.header.message {
            Message::ProofLookupReady { credit: 65536 } => {}
            m => return Err(response(m)),
        }
        let mut upload = Upload::new(self, &mut request, Part::Lookup, deadline);
        proof_codec::write_original(&mut upload, original, &self.target)?;
        upload.finish(count.0)?;
        let ready = next(self, &mut request, deadline, false)?;
        Ok((request, ready.header.message, count.0))
    }

    /// Worker-only existing lookup. This cannot promote a missing original into new work.
    pub fn existing_original(
        &self,
        original: &GitMutationRequest,
    ) -> Result<Option<OperationReceipt>, CoreError> {
        if !self
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "git.original_lookup")
        {
            return Err(unavailable(
                "Active helper cannot stream original lookup; preserve the existing supervisor",
            ));
        }
        let (mut request, message, _) = self.lookup_prefix(original, ProofAction::Lookup)?;
        request.finish();
        match message {
            Message::ProofMissing {} => Ok(None),
            Message::ProofExisting { receipt } => {
                let receipt = receipt.core(&self.target)?;
                if receipt.id != original.operation {
                    return Err(w::invalid());
                }
                Ok(Some(receipt))
            }
            other => Err(response(other)),
        }
    }
    fn proof_request<'a>(
        &self,
        original: &GitMutationRequest,
        action: ProofAction,
        proof_id: [u8; 16],
        alias_digest: [u8; 32],
        values: impl Iterator<Item = &'a GitSaveApproval> + Clone,
    ) -> Result<Message, CoreError> {
        self.proof_capability()?;
        let (mut request, ready, original_length) = self.lookup_prefix(original, action)?;
        let (original_transfer_id, approvals_transfer_id) = match ready {
            message @ Message::ProofExisting { .. } => {
                request.finish();
                return Ok(message);
            }
            Message::ProofReady {
                original_transfer_id,
                approvals_transfer_id,
            } => (original_transfer_id, approvals_transfer_id),
            m => return Err(response(m)),
        };
        self.check_cancellation()?;
        if !self.transport.owns_registration(&self.lease) {
            return Err(unavailable("exact proof registration changed"));
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        request.header.stream_id = Some(original_transfer_id);
        let mut upload = Upload::new(self, &mut request, Part::Original, deadline);
        proof_codec::write_original(&mut upload, original, &self.target)?;
        upload.finish(original_length)?;
        match next(self, &mut request, deadline, true)?.header.message {
            Message::Ack {} => {}
            m => return Err(response(m)),
        }
        let mut count = Count(0);
        approvals(&mut count, self, values.clone())?;
        request.header.stream_id = Some(approvals_transfer_id.clone());
        request.send(
            Message::ProofApprovalsBegin {
                transfer_id: approvals_transfer_id,
                total_length: count.0,
                proof_id: w::uuid(proof_id),
                alias_digest: w::hex(&alias_digest),
            },
            Vec::new(),
        )?;
        match next(self, &mut request, deadline, true)?.header.message {
            Message::Ack {} => {}
            m => return Err(response(m)),
        }
        let mut upload = Upload::new(self, &mut request, Part::Approvals, deadline);
        approvals(&mut upload, self, values)?;
        upload.finish(count.0)?;
        let result = next(self, &mut request, deadline, false)?.header.message;
        request.finish();
        Ok(result)
    }
    /// Existing receipt wins before approval iteration. Preserve the original ID on every error.
    pub fn prepare_worktree<'a>(
        &self,
        original: &GitMutationRequest,
        observation: [u8; 16],
        alias_digest: [u8; 32],
        approvals: impl Iterator<Item = &'a GitSaveApproval> + Clone,
    ) -> Result<WorktreePreparation, CoreError> {
        match self.proof_request(
            original,
            ProofAction::Prepare,
            observation,
            alias_digest,
            approvals,
        )? {
            Message::ProofPrepared { plan_id } => {
                Ok(WorktreePreparation::Prepared(w::id(&plan_id)?))
            }
            Message::ProofExisting { receipt } => {
                let r = receipt.core(&self.target)?;
                if r.id != original.operation {
                    return Err(w::invalid());
                }
                Ok(WorktreePreparation::Existing(Box::new(r)))
            }
            m => Err(response(m)),
        }
    }
    pub fn execute_worktree<'a>(
        &self,
        original: GitMutationRequest,
        plan: [u8; 16],
        alias_digest: [u8; 32],
        approvals: impl Iterator<Item = &'a GitSaveApproval> + Clone,
    ) -> Result<OperationReceipt, CoreError> {
        match self.proof_request(
            &original,
            ProofAction::Execute,
            plan,
            alias_digest,
            approvals,
        )? {
            Message::ProofExisting { receipt } => {
                let r = receipt.core(&self.target)?;
                if r.id != original.operation {
                    return Err(w::invalid());
                }
                Ok(r)
            }
            m => Err(response(m)),
        }
    }
    fn proof_cleanup(&self, message: Message) -> Result<(), CoreError> {
        self.proof_capability()?;
        let mut request = self.transport.start(message, None, None)?;
        let result = request.next()?.header.message;
        request.finish();
        match result {
            Message::Ack {} => Ok(()),
            m => Err(response(m)),
        }
    }
    pub fn cancel_worktree_plan(&self, plan: [u8; 16]) -> Result<(), CoreError> {
        self.proof_cleanup(Message::ProofCancel {
            lease_id: w::uuid(self.lease.lease.0),
            plan_id: w::uuid(plan),
        })
    }
    pub fn release_observation(&self, observation: [u8; 16]) -> Result<(), CoreError> {
        self.proof_cleanup(Message::ProofRelease {
            lease_id: w::uuid(self.lease.lease.0),
            observation_id: w::uuid(observation),
        })
    }
}
