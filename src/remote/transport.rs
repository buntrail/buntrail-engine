use super::*;
use buntrail_protocol::{
    frame::{Decoder, Frame},
    types::{Context, Header, Message},
};
use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    os::unix::net::UnixStream,
    sync::{
        Condvar, Mutex, Weak,
        mpsc::{self, SyncSender},
    },
    task::{Poll, Waker},
    time::Duration,
};
struct Inbox {
    frames: VecDeque<Frame>,
    bytes: usize,
    error: Option<CoreError>,
    waker: Option<Waker>,
}
struct Mailbox {
    cancelled: AtomicBool,
    finished: AtomicBool,
    unsent_refusal: AtomicBool,
    state: Mutex<Inbox>,
    ready: Condvar,
}
impl Mailbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            unsent_refusal: AtomicBool::new(false),
            state: Mutex::new(Inbox {
                frames: VecDeque::new(),
                bytes: 0,
                error: None,
                waker: None,
            }),
            ready: Condvar::new(),
        })
    }
    fn fail(&self, e: CoreError) {
        let mut s = self.state.lock().unwrap();
        s.error = Some(e);
        if let Some(w) = s.waker.take() {
            w.wake()
        }
        self.ready.notify_all();
    }
    fn refuse_unsent(&self, error: CoreError) {
        // Only the worker can establish that this owner was removed without
        // entering the socket output. A deadline alone cannot prove that.
        self.unsent_refusal.store(true, Ordering::SeqCst);
        self.fail(error);
    }
    fn push(&self, f: Frame) -> Result<(), CoreError> {
        let mut s = self.state.lock().unwrap();
        if s.bytes + f.payload.len() > 1024 * 1024 || s.frames.len() >= 10000 {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "Remote receive window exceeded",
            ));
        }
        s.bytes += f.payload.len();
        s.frames.push_back(f);
        if let Some(w) = s.waker.take() {
            w.wake()
        }
        self.ready.notify_all();
        Ok(())
    }
    fn take(s: &mut Inbox) -> Option<Result<Frame, CoreError>> {
        if let Some(f) = s.frames.pop_front() {
            s.bytes -= f.payload.len();
            Some(Ok(f))
        } else {
            s.error.clone().map(Err)
        }
    }
}
type Credits = Arc<Mutex<HashMap<u64, (Header, usize, Arc<Scope>)>>>;
struct SendFrame {
    frame: Frame,
    mailbox: Option<Arc<Mailbox>>,
    scope: Arc<Scope>,
}
#[derive(Clone)]
pub(super) struct Transport {
    sender: SyncSender<SendFrame>,
    credits: Credits,
    control: SyncSender<SendFrame>,
    pub context: Context,
    scope: Arc<Scope>,
    scopes: Arc<Mutex<Vec<Weak<Scope>>>>,
    registrations: Arc<Mutex<()>>,
    pub capabilities: Arc<Vec<String>>,
    retired: Arc<Retirement>,
    pub runtime: RuntimeGeneration,
    pub connected: Arc<AtomicBool>,
    ids: Arc<std::sync::atomic::AtomicU64>,
    pub(super) authentication_ready: Arc<AtomicBool>,
    pub(super) physical: Option<Arc<crate::environment::PhysicalOwner>>,
    pub(super) connection_permit: Option<Arc<crate::environment::ConnectionPermit>>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
}
struct Scope {
    context: Context,
    lease: WorkspaceLease,
    active: AtomicBool,
    release_confirmed: Mutex<Option<buntrail_protocol::types::ReleaseIntent>>,
}
#[derive(Default)]
struct Retirement {
    done: AtomicBool,
    wake: Mutex<Option<Waker>>,
}
struct Retire(Arc<Retirement>, Arc<AtomicBool>);
impl Drop for Retire {
    fn drop(&mut self) {
        self.1.store(false, Ordering::SeqCst);
        self.0.done.store(true, Ordering::SeqCst);
        if let Some(wake) = self.0.wake.lock().unwrap().take() {
            wake.wake();
        }
    }
}
pub struct Shutdown(Arc<Retirement>);
impl std::future::Future for Shutdown {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let mut wake = self.0.wake.lock().unwrap();
        if self.0.done.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            *wake = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
pub(super) struct Request {
    pub header: Header,
    mailbox: Arc<Mailbox>,
    transport: Transport,
    finished: bool,
}
impl Drop for Request {
    fn drop(&mut self) {
        self.transport.requests.fetch_sub(1, Ordering::SeqCst);
        if !self.finished {
            self.mailbox.cancelled.store(true, Ordering::SeqCst);
        }
    }
}
impl Request {
    pub fn send_control(&self, message: Message, payload: Vec<u8>) -> Result<(), CoreError> {
        let mut header = self.header.clone();
        header.message = message;
        self.transport
            .control
            .try_send(SendFrame {
                frame: Frame { header, payload },
                mailbox: None,
                scope: self.transport.scope.clone(),
            })
            .map_err(|_| {
                CoreError::new(
                    ErrorCode::Unknown,
                    "original transfer control queue unavailable; inspect without replay",
                )
            })
    }

    pub fn send(&self, message: Message, payload: Vec<u8>) -> Result<(), CoreError> {
        let mut header = self.header.clone();
        header.message = message;
        self.transport
            .sender
            .send(SendFrame {
                frame: Frame { header, payload },
                mailbox: None,
                scope: self.transport.scope.clone(),
            })
            .map_err(|_| closed())
    }
    pub fn credit(&self, stream: &str, bytes: usize) -> Result<(), CoreError> {
        let mut credits = self.transport.credits.lock().unwrap();
        if credits.len() >= 64 && !credits.contains_key(&self.header.request_id) {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "Remote credit owner capacity reached",
            ));
        }
        let mut header = self.header.clone();
        header.stream_id = Some(stream.into());
        let entry =
            credits
                .entry(header.request_id)
                .or_insert((header, 0, self.transport.scope.clone()));
        if entry.0.stream_id.as_deref() != Some(stream) || entry.1 + bytes > 524288 {
            return Err(w::invalid());
        }
        entry.1 += bytes;
        Ok(())
    }
    pub fn next(&mut self) -> Result<Frame, CoreError> {
        self.next_with_timeout(Duration::from_secs(120))
    }
    fn next_with_timeout(&mut self, timeout: Duration) -> Result<Frame, CoreError> {
        let mut s = self.mailbox.state.lock().unwrap();
        loop {
            if let Some(f) = Mailbox::take(&mut s) {
                return f;
            }
            let (state, timeout) = self.mailbox.ready.wait_timeout(s, timeout).unwrap();
            s = state;
            if timeout.timed_out() {
                return Err(CoreError::new(
                    ErrorCode::Unknown,
                    "Remote request deadline exceeded; inspect original operation without replay",
                ));
            }
        }
    }
    pub fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<Frame, CoreError>> {
        let mut s = self.mailbox.state.lock().unwrap();
        if let Some(f) = Mailbox::take(&mut s) {
            Poll::Ready(f)
        } else {
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    pub fn finish(&mut self) {
        self.finished = true;
        self.mailbox.finished.store(true, Ordering::SeqCst);
    }
    pub fn connection_lost(&self) -> bool {
        !self.transport.connected.load(Ordering::SeqCst)
    }
    pub fn refused_before_send(&self) -> bool {
        self.mailbox.unsent_refusal.load(Ordering::SeqCst)
    }
}
fn closed() -> CoreError {
    CoreError::new(
        ErrorCode::Closed,
        "Remote connection lost; inspect accepted operations, never replay",
    )
}
impl Transport {
    pub fn connect(
        mut socket: UnixStream,
        target: &TargetIdentity,
        lease: &WorkspaceLease,
    ) -> Result<Self, CoreError> {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(io_error)?;
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(io_error)?;
        let mut h = Header {
            major: 1,
            minor: 0,
            request_id: 1,
            context: None,
            operation_id: None,
            stream_id: None,
            sequence: None,
            message: Message::Hello {
                capabilities: buntrail_protocol::negotiation::CAPABILITIES
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                max_header: 65536,
                max_payload: 1048576,
            },
        };
        write_frame(
            &mut socket,
            &Frame {
                header: h.clone(),
                payload: vec![],
            },
        )?;
        let welcome = read_frame(&mut socket, None)?;
        if welcome.header.request_id != 1 {
            return Err(w::invalid());
        }
        let Message::Welcome {
            target_binding,
            runtime_generation,
            container_start_base64,
            capabilities,
            ..
        } = welcome.header.message
        else {
            return Err(w::invalid());
        };
        if !capabilities.iter().any(|s| s == "services") {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "Remote service capability unavailable",
            ));
        }
        let expected = w::hex(&sha2::Sha256::digest(
            serde_json::to_vec(target).map_err(io_error)?,
        ));
        if target_binding != expected {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "Helper target binding changed",
            ));
        }
        let runtime = RuntimeGeneration {
            supervisor_boot: w::id(&runtime_generation)?,
            container_start: buntrail_protocol::types::decode_container_start(
                container_start_base64.as_deref(),
            )
            .map_err(|_| w::invalid())?,
        };
        if target.container.is_some() && runtime.container_start.is_none() {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "Container helper Welcome is missing verified start evidence; no owner was registered",
            ));
        }
        if runtime.container_start != lease.runtime.container_start {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "Helper container start generation changed; preserve the original runtime and reconcile explicitly",
            ));
        }
        if lease.runtime.supervisor_boot != [0; 16] && lease.runtime != runtime {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "Helper runtime changed",
            ));
        }
        let context = Context {
            workspace_id: w::uuid(lease.workspace.0),
            client_id: w::uuid(lease.client.0),
            target_binding,
            runtime_generation,
        };
        h.request_id = 2;
        h.message = Message::Register {
            lease_id: w::uuid(lease.lease.0),
            client_id: context.client_id.clone(),
            workspace_id: context.workspace_id.clone(),
            target_binding: context.target_binding.clone(),
            runtime_generation: context.runtime_generation.clone(),
        };
        write_frame(
            &mut socket,
            &Frame {
                header: h,
                payload: vec![],
            },
        )?;
        let ack = read_frame(&mut socket, Some(&context))?;
        if ack.header.request_id != 2 || !matches!(ack.header.message, Message::Ack {}) {
            return Err(w::invalid());
        }
        socket.set_nonblocking(true).map_err(io_error)?;
        let (tx, rx) = mpsc::sync_channel::<SendFrame>(32);
        let (control, controls) = mpsc::sync_channel::<SendFrame>(32);
        let connected = Arc::new(AtomicBool::new(true));
        let live = connected.clone();
        let scope = Arc::new(Scope {
            context: context.clone(),
            lease: WorkspaceLease {
                runtime: runtime.clone(),
                ..lease.clone()
            },
            active: AtomicBool::new(true),
            release_confirmed: Mutex::new(None),
        });
        let scopes = Arc::new(Mutex::new(vec![Arc::downgrade(&scope)]));
        let retired = Arc::new(Retirement::default());
        let completion = Retire(retired.clone(), connected.clone());
        let credits: Credits = Arc::new(Mutex::new(HashMap::new()));
        let pending_credit = credits.clone();
        let ids = Arc::new(std::sync::atomic::AtomicU64::new(3));
        let worker_ids = ids.clone();
        std::thread::Builder::new()
            .name("buntrail-remote-wire".into())
            .spawn(move || {
                let _completion = completion;
                worker(socket, rx, controls, pending_credit, worker_ids, live)
            })
            .map_err(io_error)?;
        Ok(Self {
            sender: tx,
            credits,
            control,
            context,
            scope,
            scopes,
            registrations: Arc::new(Mutex::new(())),
            capabilities: Arc::new(capabilities),
            retired,
            runtime,
            connected,
            ids,
            authentication_ready: Arc::new(AtomicBool::new(false)),
            physical: None,
            connection_permit: None,
            requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }
    pub fn register(&self, lease: WorkspaceLease) -> Result<Self, CoreError> {
        let _registration = self.registrations.lock().map_err(io_error)?;
        if !self.connected.load(Ordering::SeqCst) {
            return Err(closed());
        }
        if lease.client != self.scope.lease.client || lease.runtime != self.runtime {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "workspace client or runtime changed",
            ));
        }
        let mut context = self.context.clone();
        context.workspace_id = w::uuid(lease.workspace.0);
        let scope = {
            let mut scopes = self.scopes.lock().map_err(io_error)?;
            scopes.retain(|s| s.strong_count() > 0);
            if scopes
                .iter()
                .filter_map(Weak::upgrade)
                .any(|s| s.lease.lease == lease.lease && s.lease != lease)
            {
                return Err(CoreError::new(
                    ErrorCode::InvalidArgument,
                    "lease already belongs to another workspace",
                ));
            }
            if let Some(existing) = scopes
                .iter()
                .filter_map(Weak::upgrade)
                .find(|s| s.lease == lease && s.active.load(Ordering::SeqCst))
            {
                existing
            } else {
                if scopes.len() >= 256 {
                    return Err(CoreError::new(
                        ErrorCode::Busy,
                        "workspace view capacity reached; retire prior views",
                    ));
                }
                let scope = Arc::new(Scope {
                    context: context.clone(),
                    lease: lease.clone(),
                    active: AtomicBool::new(false),
                    release_confirmed: Mutex::new(None),
                });
                scopes.push(Arc::downgrade(&scope));
                scope
            }
        };
        let view = Self {
            context,
            scope,
            ..self.clone()
        };
        if view.scope.active.load(Ordering::SeqCst) {
            return Ok(view);
        }
        let c = &view.context;
        let mut request = view.start(
            Message::Register {
                lease_id: w::uuid(lease.lease.0),
                client_id: c.client_id.clone(),
                workspace_id: c.workspace_id.clone(),
                target_binding: c.target_binding.clone(),
                runtime_generation: c.runtime_generation.clone(),
            },
            None,
            None,
        )?;
        if !matches!(request.next()?.header.message, Message::Ack {}) {
            return Err(w::invalid());
        }
        request.finish();
        view.scope.active.store(true, Ordering::SeqCst);
        Ok(view)
    }
    pub fn release(
        &self,
        intent: buntrail_protocol::types::ReleaseIntent,
    ) -> Result<(), CoreError> {
        self.release_wait(intent, Duration::from_secs(120), false)
    }
    pub(super) fn detach(&self) -> Result<(), CoreError> {
        self.release_wait(
            buntrail_protocol::types::ReleaseIntent::ViewDetachment,
            Duration::from_millis(500),
            true,
        )
    }
    fn release_wait(
        &self,
        intent: buntrail_protocol::types::ReleaseIntent,
        timeout: Duration,
        nonblocking: bool,
    ) -> Result<(), CoreError> {
        let _registration = if nonblocking {
            self.registrations.try_lock().map_err(|_| {
                self.scope.active.store(false, Ordering::SeqCst);
                CoreError::new(
                    ErrorCode::Unknown,
                    "Local view detached; remote lease acknowledgement is unconfirmed",
                )
            })?
        } else {
            self.registrations.lock().map_err(io_error)?
        };
        if !self.scope.active.swap(false, Ordering::SeqCst) {
            return if self
                .scope
                .release_confirmed
                .lock()
                .map_err(io_error)?
                .as_ref()
                == Some(&intent)
            {
                Ok(())
            } else {
                Err(CoreError::new(
                    ErrorCode::Closed,
                    "workspace release is unconfirmed; automatic replay is disabled",
                ))
            };
        }
        let mut request = self.start(
            Message::Release {
                lease_id: w::uuid(self.scope.lease.lease.0),
                intent: intent.clone(),
            },
            None,
            None,
        )?;
        if !matches!(
            request.next_with_timeout(timeout)?.header.message,
            Message::Ack {}
        ) {
            return Err(w::invalid());
        }
        request.finish();
        *self.scope.release_confirmed.lock().map_err(io_error)? = Some(intent);
        Ok(())
    }
    pub(super) fn owns_registration(&self, lease: &WorkspaceLease) -> bool {
        self.registered() && &self.scope.lease == lease && lease.runtime == self.runtime
    }
    pub(super) fn registered(&self) -> bool {
        self.scope.active.load(Ordering::SeqCst) && self.connected.load(Ordering::SeqCst)
    }
    pub(super) fn lifetime(&self) -> Lifetime {
        Lifetime {
            registrations: self.registrations.clone(),
            connected: self.connected.clone(),
            retired: self.retired.clone(),
            scopes: self.scopes.clone(),
            requests: self.requests.clone(),
        }
    }
    pub fn close(&self) -> Shutdown {
        self.connected.store(false, Ordering::SeqCst);
        Shutdown(self.retired.clone())
    }
    fn frame(
        &self,
        message: Message,
        operation: Option<OperationID>,
        stream: Option<String>,
    ) -> Frame {
        Frame {
            header: Header {
                major: 1,
                minor: 0,
                request_id: self.ids.fetch_add(1, Ordering::SeqCst),
                context: if matches!(message, Message::Register { .. }) {
                    None
                } else {
                    Some(self.context.clone())
                },
                operation_id: operation.map(|o| w::uuid(o.0)),
                stream_id: stream,
                sequence: None,
                message,
            },
            payload: vec![],
        }
    }
    pub fn start(
        &self,
        message: Message,
        operation: Option<OperationID>,
        stream: Option<String>,
    ) -> Result<Request, CoreError> {
        self.start_payload(message, operation, stream, Vec::new())
    }
    pub fn start_payload(
        &self,
        message: Message,
        operation: Option<OperationID>,
        stream: Option<String>,
        payload: Vec<u8>,
    ) -> Result<Request, CoreError> {
        if !self.connected.load(Ordering::SeqCst)
            || (!self.scope.active.load(Ordering::SeqCst)
                && !matches!(message, Message::Register { .. } | Message::Release { .. }))
        {
            return Err(closed());
        }
        let mut frame = self.frame(message, operation, stream);
        frame.payload = payload;
        // Legacy existing-only calls must fit the exact envelope before queueing any frame.
        if matches!(
            &frame.header.message,
            Message::ServiceCall {
                call: w::Call::GitExisting { .. }
            }
        ) {
            struct HeaderBound(usize);
            impl std::io::Write for HeaderBound {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    if bytes.len() > 65536 - self.0 {
                        return Err(std::io::Error::other(
                            "legacy original exceeds header limit",
                        ));
                    }
                    self.0 += bytes.len();
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            serde_json::to_writer(HeaderBound(0), &frame.header).map_err(|_| CoreError::new(ErrorCode::Unavailable, "Active helper cannot stream this complete original request; preserve its operation and supervisor"))?;
        }
        let header = frame.header.clone();
        let mailbox = Mailbox::new();
        let sender = if matches!(&frame.header.message, Message::ServiceCall { call } if call.unprepared_worktree_mutation())
            || matches!(
                frame.header.message,
                Message::ProofLookupBegin { .. }
                    | Message::ProofCancel { .. }
                    | Message::ProofRelease { .. }
                    | Message::ServiceCall {
                        call: w::Call::Inspect
                            | w::Call::GitExisting { .. }
                            | w::Call::LeaseObserve { .. }
                            | w::Call::TerminalEnd { .. }
                            | w::Call::TerminalInput { .. }
                            | w::Call::TerminalResize { .. }
                    }
            ) {
            &self.control
        } else {
            &self.sender
        };
        sender
            .try_send(SendFrame {
                frame,
                mailbox: Some(mailbox.clone()),
                scope: self.scope.clone(),
            })
            .map_err(|_| CoreError::new(ErrorCode::Busy, "Remote request queue full"))?;
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(Request {
            header,
            mailbox,
            transport: self.clone(),
            finished: false,
        })
    }
}
fn write_frame(s: &mut UnixStream, f: &Frame) -> Result<(), CoreError> {
    s.write_all(&f.encode().map_err(|_| w::invalid())?)
        .map_err(io_error)
}
fn read_frame(s: &mut UnixStream, c: Option<&Context>) -> Result<Frame, CoreError> {
    let mut prefix = [0; 8];
    s.read_exact(&mut prefix).map_err(io_error)?;
    let h = u32::from_be_bytes(prefix[..4].try_into().unwrap()) as usize;
    let p = u32::from_be_bytes(prefix[4..].try_into().unwrap()) as usize;
    if h > 65536 || p > 1048576 {
        return Err(w::invalid());
    }
    let mut bytes = vec![0; h + p + 8];
    bytes[..8].copy_from_slice(&prefix);
    s.read_exact(&mut bytes[8..]).map_err(io_error)?;
    Decoder::default()
        .push(&bytes, c)
        .map_err(|_| w::invalid())?
        .pop()
        .ok_or_else(w::invalid)
}

fn worker(
    mut socket: UnixStream,
    rx: mpsc::Receiver<SendFrame>,
    controls: mpsc::Receiver<SendFrame>,
    pending_credit: Credits,
    worker_ids: Arc<std::sync::atomic::AtomicU64>,
    live: Arc<AtomicBool>,
) {
    let mut routes: HashMap<u64, (Arc<Mailbox>, Header, Arc<Scope>)> = HashMap::new();
    let mut decoder = Decoder::default();
    let mut output = Vec::new();
    let mut written = 0;
    let mut _writing_scope: Option<Arc<Scope>> = None;
    let mut input = [0; 65536];
    let outcome = (|| -> Result<(), CoreError> {
        loop {
            if !live.load(Ordering::SeqCst) {
                return Ok(());
            }
            if output.is_empty() {
                // Wire completion does not retire the consumer's bounded owner. A finished
                // consumer has returned all credit; flush it before releasing its route.
                {
                    let credits = pending_credit.lock().unwrap();
                    routes.retain(|id, (mailbox, _, _)| {
                        !mailbox.finished.load(Ordering::SeqCst) || credits.contains_key(id)
                    });
                }
                let cancelled = routes
                    .iter()
                    .find(|(_, (m, _, _))| m.cancelled.load(Ordering::SeqCst))
                    .map(|(id, _)| *id);

                let next = if let Some(id) = cancelled {
                    let (_, mut header, scope) = routes.remove(&id).unwrap();

                    pending_credit.lock().unwrap().remove(&id);

                    header.request_id = worker_ids.fetch_add(1, Ordering::SeqCst);

                    header.message = Message::Cancel { request_id: id };

                    header.operation_id = None;
                    header.stream_id = None;
                    header.sequence = None;

                    Some(SendFrame {
                        frame: Frame {
                            header,
                            payload: vec![],
                        },
                        mailbox: None,
                        scope,
                    })
                } else if let Ok(control) = controls.try_recv() {
                    Some(control)
                } else {
                    let credit = {
                        let mut credits = pending_credit.lock().unwrap();

                        let id = credits.keys().next().copied();

                        id.and_then(|id| credits.remove(&id))
                    };

                    if let Some((mut header, bytes, scope)) = credit {
                        header.message = Message::Credit {
                            bytes: bytes as u32,
                        };

                        Some(SendFrame {
                            frame: Frame {
                                header,
                                payload: vec![],
                            },
                            mailbox: None,
                            scope,
                        })
                    } else {
                        match rx.try_recv() {
                            Ok(v) => Some(v),
                            Err(mpsc::TryRecvError::Empty) => None,
                            Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                        }
                    }
                };

                if let Some(v) = next {
                    if let Some(mailbox) = v.mailbox {
                        if mailbox.cancelled.load(Ordering::SeqCst) {
                            continue;
                        }
                        if routes.len() >= 32 {
                            mailbox.refuse_unsent(CoreError::new(
                                ErrorCode::Busy,
                                "Remote request capacity reached",
                            ));

                            continue;
                        }
                        routes.insert(
                            v.frame.header.request_id,
                            (mailbox, v.frame.header.clone(), v.scope.clone()),
                        );
                    } else if !matches!(
                        v.frame.header.message,
                        Message::Credit { .. } | Message::Cancel { .. }
                    ) && !routes.contains_key(&v.frame.header.request_id)
                    {
                        continue;
                    }
                    output = v.frame.encode().map_err(|_| w::invalid())?;
                    _writing_scope = Some(v.scope);
                    written = 0;
                }
            }
            if !output.is_empty() {
                match socket.write(&output[written..]) {
                    Ok(0) => return Err(closed()),
                    Ok(n) => {
                        written += n;
                        if written == output.len() {
                            output.clear();
                            _writing_scope = None;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(io_error(e)),
                }
            }
            match socket.read(&mut input) {
                Ok(0) => return Err(closed()),
                Ok(n) => {
                    for f in decoder
                        .push_replies(&input[..n], |id| {
                            routes.get(&id).map(|(_, header, scope)| {
                                (scope.context.clone(), header.operation_id.clone())
                            })
                        })
                        .map_err(|_| w::invalid())?
                    {
                        let id = f.header.request_id;

                        if let Some((route, request_header, expected)) = routes.get(&id) {
                            if f.header.operation_id != request_header.operation_id
                                || f.header.context.as_ref() != Some(&expected.context)
                            {
                                return Err(w::invalid());
                            }
                            route.push(f)?;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(io_error(e)),
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    })();

    live.store(false, Ordering::SeqCst);

    let error = outcome.err().unwrap_or_else(closed);

    for (route, _, _) in routes.values() {
        route.fail(error.clone());
    }
    for receiver in [rx, controls] {
        while let Ok(v) = receiver.try_recv() {
            if let Some(m) = v.mailbox {
                m.refuse_unsent(error.clone());
            }
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;

pub(crate) struct Lifetime {
    registrations: Arc<Mutex<()>>,
    pub connected: Arc<AtomicBool>,
    retired: Arc<Retirement>,
    scopes: Arc<Mutex<Vec<Weak<Scope>>>>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
}
impl Lifetime {
    pub fn has_active_views(&self) -> bool {
        self.scopes
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .any(|s| s.active.load(Ordering::SeqCst))
    }
    pub fn idle(&self) -> bool {
        // Registration/release can temporarily have neither an active view nor a queued request.
        let Ok(_transition) = self.registrations.try_lock() else {
            return false;
        };
        self.requests.load(Ordering::SeqCst) == 0 && !self.has_active_views()
    }
    pub fn shutdown(&self) -> Shutdown {
        self.connected.store(false, Ordering::SeqCst);
        Shutdown(self.retired.clone())
    }
}
