//! Service proxies only. T16 connects these to the engine's document/mutation coordinator.
mod files;
pub use files::RemoteOwnedSave;
mod git;
mod proof;
pub use proof::{EntryGroup, ObservationEvent, OwnedObservation, WorktreePreparation};
mod operations;
mod search;
mod terminal;
mod transport;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use buntrail_core::contracts::{
    async_types::{EventStream, ServiceFuture},
    *,
};
use buntrail_protocol::{services as w, types::Message};
use buntrail_services::contracts::*;
use futures_core::Stream;
use sha2::Digest;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
fn io_error(e: impl std::fmt::Display) -> CoreError {
    CoreError::new(ErrorCode::Io, &e.to_string())
}
#[derive(Clone)]
pub struct RemoteServices {
    transport: transport::Transport,
    target: TargetIdentity,
    lease: WorkspaceLease,
    spools: Arc<buntrail_services::leases::LeasePool>,
    cancellation: Option<Arc<AtomicBool>>,
    // A registered view keeps the reusable physical anchor alive, never the reverse.
    anchor: Option<Arc<RemoteServices>>,
}
pub(crate) use transport::Lifetime;
pub use transport::Shutdown as RemoteShutdown;
impl RemoteServices {
    /// The route owner supplies an authenticated byte transport and verified target, never a display hostname.
    pub fn connect(
        socket: std::os::unix::net::UnixStream,
        target: TargetIdentity,
        lease: WorkspaceLease,
        spool_directory: std::path::PathBuf,
    ) -> Result<Self, CoreError> {
        let spools = buntrail_services::leases::LeasePool::open(spool_directory)?;
        let transport = transport::Transport::connect(socket, &target, &lease)?;
        let lease = WorkspaceLease {
            runtime: transport.runtime.clone(),
            ..lease
        };
        Ok(Self {
            lease,
            transport,
            target,
            spools,
            cancellation: None,
            anchor: None,
        })
    }
    #[cfg(test)]
    pub(crate) fn shares_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.transport.connected, &other.transport.connected)
    }
    /// Worker-only registration; all views share the existing wire worker and spool pool.
    pub fn register_workspace(&self, lease: WorkspaceLease) -> Result<Self, CoreError> {
        let transport = self.transport.register(lease.clone())?;
        Ok(Self {
            transport,
            lease,
            ..self.clone()
        })
    }
    /// Worker-only, read-only observation of an exact prior owner.
    pub fn observe_owner(&self, owner: WorkspaceLease) -> Result<LeaseObservation, CoreError> {
        if !self
            .negotiated_capabilities()
            .iter()
            .any(|c| c == "leases.observe")
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "active helper cannot observe owner retirement; preserve the existing supervisor",
            ));
        }
        if owner.client != self.lease.client {
            return Err(w::invalid());
        }
        let expected = w::LeaseOwner::from_core(&owner, &self.target)?;
        match self.call(
            w::Call::LeaseObserve {
                owner: expected.clone(),
            },
            None,
        )? {
            w::Reply::LeaseObserved {
                owner: actual,
                runtime_generation,
                presence,
                workers_retired,
            } if actual == expected
                && w::id(&runtime_generation)? == self.runtime().supervisor_boot =>
            {
                let presence = match presence {
                    w::OwnerPresence::Active {} => LeasePresence::Active,
                    w::OwnerPresence::Detached { remaining_millis }
                        if remaining_millis <= 1_800_000 =>
                    {
                        LeasePresence::Detached { remaining_millis }
                    }
                    w::OwnerPresence::Absent {} => LeasePresence::Absent,
                    w::OwnerPresence::RuntimeLost {}
                        if owner.runtime != self.runtime() && !workers_retired =>
                    {
                        LeasePresence::RuntimeLost
                    }
                    _ => return Err(w::invalid()),
                };
                if owner.runtime != self.runtime() && presence != LeasePresence::RuntimeLost {
                    return Err(w::invalid());
                }
                Ok(LeaseObservation {
                    owner,
                    target: self.target.clone(),
                    observed_runtime: self.runtime(),
                    presence,
                    workers_retired,
                })
            }
            _ => Err(w::invalid()),
        }
    }
    pub fn lease(&self) -> &WorkspaceLease {
        &self.lease
    }
    pub fn target(&self) -> &TargetIdentity {
        &self.target
    }
    pub fn validate_environment_ready(&self) -> Result<(), CoreError> {
        self.validate_connection_ready()?;
        if !self.transport.registered() {
            return Err(CoreError::new(
                ErrorCode::Closed,
                "environment registration is released or disconnected; explicit owner resume is required",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_connection_ready(&self) -> Result<(), CoreError> {
        if self.connection_state() != ConnectionState::Ready
            || !self.transport.authentication_ready.load(Ordering::SeqCst)
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "current connection authentication has not been acknowledged",
            ));
        }
        if self.target.container.is_some()
            && !self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == "container.binding.v1")
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "Active helper cannot verify container identity/runtime; preserve it and reconnect after a compatible supervisor is available",
            ));
        }
        for capability in [
            "services",
            "file.resolve",
            "file.read",
            "file.save",
            "file.save_proof",
            "operations",
            "leases",
            "terminal",
            "leases.retirement",
            "terminal.ordered_controls",
            "leases.observe",
            "git.existing",
        ] {
            if !self
                .negotiated_capabilities()
                .iter()
                .any(|c| c == capability)
            {
                return Err(CoreError::new(
                    ErrorCode::Unavailable,
                    &format!(
                        "active helper lacks {capability}; preserve its existing sessions and reconnect after a compatible supervisor is available"
                    ),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn anchored_workspace(
        self: &Arc<Self>,
        lease: WorkspaceLease,
    ) -> Result<Self, CoreError> {
        let mut view = self.register_workspace(lease)?;
        view.anchor = Some(self.clone());
        Ok(view)
    }
    pub(crate) fn bind_connection_permit(
        &mut self,
        permit: Arc<crate::environment::ConnectionPermit>,
    ) {
        self.transport.connection_permit = Some(permit);
    }
    pub(crate) fn physical(&self) -> Option<Arc<crate::environment::PhysicalOwner>> {
        self.transport.physical.clone()
    }
    pub(crate) fn bind_physical(
        &mut self,
        channel: buntrail_services::connections::ssh::SshChannel,
    ) -> Result<(), CoreError> {
        self.bind_retirement(Box::new(move || Box::pin(channel.close())))
    }
    pub(crate) fn bind_retirement(
        &mut self,
        retire: Box<dyn FnOnce() -> ServiceFuture<'static, ()> + Send>,
    ) -> Result<(), CoreError> {
        self.transport.physical = Some(crate::environment::PhysicalOwner::start(
            self.transport.lifetime(),
            retire,
        )?);
        Ok(())
    }
    pub fn negotiated_capabilities(&self) -> &[String] {
        &self.transport.capabilities
    }
    pub(crate) fn validate_workspace_release(
        &self,
        intent: LeaseReleaseIntent,
    ) -> Result<(), CoreError> {
        if intent == LeaseReleaseIntent::ResolvedWorkspaceClosure
            && !self
                .negotiated_capabilities()
                .iter()
                .any(|capability| capability == "leases.retirement")
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "helper cannot confirm terminal retirement; keep this workspace until a compatible connection is available",
            ));
        }
        Ok(())
    }
    /// Revokes this view's new-work authority; other registered views remain connected.
    pub fn release_workspace(&self, intent: LeaseReleaseIntent) -> Result<(), CoreError> {
        self.validate_workspace_release(intent)?;
        self.transport.release(match intent {
            LeaseReleaseIntent::ViewDetachment => {
                buntrail_protocol::types::ReleaseIntent::ViewDetachment
            }
            LeaseReleaseIntent::ResolvedWorkspaceClosure => {
                buntrail_protocol::types::ReleaseIntent::ResolvedWorkspaceClosure
            }
        })
    }
    pub(crate) fn detach_workspace(&self) -> Result<(), CoreError> {
        self.transport.detach()
    }
    /// Physical-route owner only. Drop the SSH channel separately and await both shutdowns off the owner thread.
    pub fn close_connection(&self) -> RemoteShutdown {
        self.transport.close()
    }
    pub fn set_authentication(&self, ssh_auth_sock: Option<TargetPath>) -> Result<(), CoreError> {
        let mut request = self.transport.start(
            Message::Authentication {
                ssh_auth_sock: ssh_auth_sock.as_ref().map(w::path).transpose()?,
            },
            None,
            None,
        )?;
        if !matches!(request.next()?.header.message, Message::Ack {}) {
            return Err(w::invalid());
        }
        request.finish();
        self.transport
            .authentication_ready
            .store(true, Ordering::SeqCst);
        Ok(())
    }
    pub fn runtime(&self) -> RuntimeGeneration {
        self.transport.runtime.clone()
    }
    pub fn connection_state(&self) -> ConnectionState {
        if self.transport.connected.load(Ordering::SeqCst) {
            ConnectionState::Ready
        } else {
            ConnectionState::Disconnected
        }
    }
    /// A worker-scoped view; cancellation never changes sibling views or physical ownership.
    pub(crate) fn with_cancellation(&self, cancellation: Arc<AtomicBool>) -> Self {
        Self {
            cancellation: Some(cancellation),
            ..self.clone()
        }
    }
    fn check_cancellation(&self) -> Result<(), CoreError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            Err(CoreError::new(
                ErrorCode::Cancelled,
                "remote request cancellation requested",
            ))
        } else {
            Ok(())
        }
    }
    fn next_reply(
        &self,
        request: &mut transport::Request,
    ) -> Result<buntrail_protocol::frame::Frame, CoreError> {
        if self.cancellation.is_none() {
            return request.next();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        loop {
            // Prefer an already-observed receipt over a concurrent cancellation request.
            if let Poll::Ready(result) = request.poll(&mut cx) {
                return result;
            }
            self.check_cancellation()?;
            if std::time::Instant::now() >= deadline {
                return Err(CoreError::new(
                    ErrorCode::Unknown,
                    "Remote request deadline exceeded; inspect original operation without replay",
                ));
            }
            std::thread::park_timeout(std::time::Duration::from_millis(10));
        }
    }
    fn check(&self, t: &TargetIdentity) -> Result<(), CoreError> {
        if t != &self.target {
            Err(CoreError::new(
                ErrorCode::TargetChanged,
                "Remote target changed",
            ))
        } else {
            Ok(())
        }
    }
    fn stream(&self, call: w::Call, operation: Option<OperationID>) -> Result<Replies, CoreError> {
        self.stream_payload(call, operation, Vec::new())
    }
    fn stream_payload(
        &self,
        call: w::Call,
        operation: Option<OperationID>,
        payload: Vec<u8>,
    ) -> Result<Replies, CoreError> {
        self.check_cancellation()?;
        Ok(Replies {
            request: self.transport.start_payload(
                Message::ServiceCall { call },
                operation,
                None,
                payload,
            )?,
            stream_id: None,
            bytes: Vec::new(),
            length: None,
            hash: None,
            sequence: 0,
            done: false,
        })
    }
    fn call(&self, call: w::Call, operation: Option<OperationID>) -> Result<w::Reply, CoreError> {
        self.call_payload(call, operation, Vec::new())
    }
    fn call_payload(
        &self,
        call: w::Call,
        operation: Option<OperationID>,
        payload: Vec<u8>,
    ) -> Result<w::Reply, CoreError> {
        let mut stream = self.stream_payload(call, operation, payload)?;
        loop {
            let frame = self.next_reply(&mut stream.request)?;
            if let Some(value) = stream.accept(frame)? {
                return value;
            }
        }
    }
    pub fn contains_document(
        &self,
        worktree: WorktreeIdentity,
        file: FileRef,
    ) -> Result<bool, CoreError> {
        self.check(&worktree.target)?;
        self.check(&file.target)?;
        match self.call(
            w::Call::Contains {
                worktree: w::Worktree::from_core(&worktree)?,
                file: w::File::from_core(&file)?,
            },
            None,
        )? {
            w::Reply::Contains { value } => Ok(value),
            _ => Err(w::invalid()),
        }
    }
    pub fn path_version(&self, file: FileRef) -> Result<FileVersion, CoreError> {
        self.check(&file.target)?;
        match self.call(
            w::Call::PathVersion {
                file: w::File::from_core(&file)?,
            },
            None,
        )? {
            w::Reply::Version { version } => version.core(&self.target),
            _ => Err(w::invalid()),
        }
    }
}
struct Replies {
    request: transport::Request,
    stream_id: Option<String>,
    bytes: Vec<u8>,
    length: Option<u64>,
    hash: Option<String>,
    sequence: u64,
    done: bool,
}
impl Replies {
    fn accept(
        &mut self,
        f: buntrail_protocol::frame::Frame,
    ) -> Result<Option<Result<w::Reply, CoreError>>, CoreError> {
        if let Message::Error { code, safe_message } = f.header.message {
            self.done = true;
            self.request.finish();
            return Ok(Some(Err(w::core_error(&code, &safe_message))));
        }
        let Message::ServiceEvent {
            offset,
            total_length,
            sha256,
            terminal,
        } = f.header.message
        else {
            return Err(w::invalid());
        };
        let stream_id = f.header.stream_id.clone().ok_or_else(w::invalid)?;
        if self.stream_id.as_ref().is_some_and(|id| id != &stream_id) {
            return Err(w::invalid());
        }
        self.stream_id = Some(stream_id);
        if f.header.sequence != Some(self.sequence)
            || offset != self.bytes.len() as u64
            || total_length > 32 * 1024 * 1024
            || self.length.is_some_and(|l| l != total_length)
            || self.hash.as_ref().is_some_and(|h| h != &sha256)
        {
            return Err(w::invalid());
        }
        self.length = Some(total_length);
        self.hash = Some(sha256.clone());
        self.bytes.extend_from_slice(&f.payload);
        let complete = self.bytes.len() as u64 == total_length;
        self.request.credit(
            f.header.stream_id.as_deref().ok_or_else(w::invalid)?,
            f.payload.len(),
        )?;
        if !complete {
            return Ok(None);
        }
        if w::hex(&sha2::Sha256::digest(&self.bytes)) != sha256 {
            return Err(w::invalid());
        }
        let value: w::Reply = serde_json::from_slice(&self.bytes).map_err(io_error)?;
        self.bytes.clear();
        self.length = None;
        self.hash = None;
        self.sequence += 1;
        if terminal {
            self.done = true;
            self.request.finish();
        }
        Ok(Some(match value {
            w::Reply::Error { code, message } => Err(w::core_error(&code, &message)),
            r => Ok(r),
        }))
    }
}
impl Stream for Replies {
    type Item = Result<w::Reply, CoreError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let frame = match self.request.poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(f)) => f,
            };
            match self.accept(frame) {
                Ok(Some(v)) => return Poll::Ready(Some(v)),
                Ok(None) => {}
                Err(e) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

impl WorkspaceLeaseService for RemoteServices {
    fn register(&self, lease: WorkspaceLease) -> ServiceFuture<'_, Result<(), CoreError>> {
        Box::pin(async move {
            if lease != self.lease {
                return Err(w::invalid());
            }
            self.validate_environment_ready()
        })
    }
    fn release(
        &self,
        id: WorkspaceLeaseID,
        intent: LeaseReleaseIntent,
    ) -> ServiceFuture<'_, Result<(), CoreError>> {
        Box::pin(async move {
            if id != self.lease.lease {
                return Err(w::invalid());
            }
            self.release_workspace(intent)
        })
    }
}
