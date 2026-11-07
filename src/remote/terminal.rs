use super::*;
use buntrail_protocol::terminal as wire;
impl RemoteServices {
    fn terminal_owner(
        &self,
        owner: &WorkspaceLease,
        runtime: &RuntimeGeneration,
    ) -> Result<(), CoreError> {
        if runtime != &self.transport.runtime || owner != &self.lease {
            return Err(CoreError::new(
                ErrorCode::TargetChanged,
                "terminal owner or runtime changed",
            ));
        }
        Ok(())
    }
    fn ordered_controls(&self) -> Result<(), CoreError> {
        if !self
            .negotiated_capabilities()
            .iter()
            .any(|capability| capability == "terminal.ordered_controls")
        {
            return Err(CoreError::new(
                ErrorCode::Unavailable,
                "helper cannot guarantee ordered terminal renderer and resize admission",
            ));
        }
        Ok(())
    }
    // Admission is memory-only and precedes independent completion-worker scheduling.
    pub(crate) fn admit_terminal_input(
        &self,
        session: TerminalSessionID,
        bytes: Vec<u8>,
    ) -> Result<ServiceFuture<'static, Result<(), CoreError>>, CoreError> {
        if bytes.len() > 65536 {
            return Err(CoreError::new(
                ErrorCode::Busy,
                "terminal input chunk exceeds 64 KiB; input not sent",
            ));
        }
        self.ordered_controls()?;
        self.admit_terminal_control(
            w::Call::TerminalInput {
                lease_id: w::uuid(self.lease.lease.0),
                session_id: w::uuid(session.0),
            },
            bytes,
        )
    }
    pub(crate) fn admit_terminal_resize(
        &self,
        session: TerminalSessionID,
        size: TerminalSize,
    ) -> Result<ServiceFuture<'static, Result<(), CoreError>>, CoreError> {
        self.ordered_controls()?;
        self.admit_terminal_control(
            w::Call::TerminalResize {
                lease_id: w::uuid(self.lease.lease.0),
                session_id: w::uuid(session.0),
                columns: size.columns.get(),
                rows: size.rows.get(),
            },
            Vec::new(),
        )
    }
    fn admit_terminal_control(
        &self,
        call: w::Call,
        bytes: Vec<u8>,
    ) -> Result<ServiceFuture<'static, Result<(), CoreError>>, CoreError> {
        let mut stream = self.stream_payload(call, None, bytes)?;
        let service = self.clone();
        Ok(Box::pin(async move {
            loop {
                let frame = service.next_reply(&mut stream.request)?;
                if let Some(reply) = stream.accept(frame)? {
                    return match reply? {
                        w::Reply::TerminalAck => Ok(()),
                        _ => Err(w::invalid()),
                    };
                }
            }
        }))
    }
    fn terminal_ack(&self, call: w::Call) -> Result<(), CoreError> {
        match self.call(call, None)? {
            w::Reply::TerminalAck => Ok(()),
            _ => Err(w::invalid()),
        }
    }
}
impl TerminalService for RemoteServices {
    fn open(&self, r: TerminalOpenRequest) -> ServiceFuture<'_, OperationReceipt> {
        Box::pin(async move {
            let call = (|| {
                self.check(&r.target)?;
                self.terminal_owner(&r.owner, &r.runtime)?;
                Ok(w::Call::TerminalOpen {
                    lease_id: w::uuid(r.owner.lease.0),
                    session_id: w::uuid(r.session.0),
                    directory_base64: w::path(&r.directory)?,
                    shell_base64: w::path(&r.shell)?,
                    columns: r.size.columns.get(),
                    rows: r.size.rows.get(),
                })
            })();
            match call {
                Ok(call) => self.mutation_call(call, r.operation, &r.target),
                Err(e) => buntrail_services::files::failure(r.operation, r.target, e),
            }
        })
    }
    fn attach(
        &self,
        r: TerminalAttachRequest,
    ) -> ServiceFuture<'_, Result<EventStream<TerminalEvent>, CoreError>> {
        Box::pin(async move {
            self.ordered_controls()?;
            self.terminal_owner(&r.owner, &r.runtime)?;
            Ok(Box::pin(Terminal {
                source: self.stream(
                    w::Call::TerminalAttach {
                        lease_id: w::uuid(r.owner.lease.0),
                        session_id: w::uuid(r.session.0),
                        last_output_sequence: r.last_output_sequence,
                    },
                    None,
                )?,
                disconnected: false,
            }) as EventStream<TerminalEvent>)
        })
    }
    fn input(
        &self,
        session: TerminalSessionID,
        bytes: Vec<u8>,
    ) -> ServiceFuture<'_, Result<(), CoreError>> {
        match self.admit_terminal_input(session, bytes) {
            Ok(completion) => completion,
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
    fn resize(
        &self,
        session: TerminalSessionID,
        size: TerminalSize,
    ) -> ServiceFuture<'_, Result<(), CoreError>> {
        match self.admit_terminal_resize(session, size) {
            Ok(completion) => completion,
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
    fn end(
        &self,
        session: TerminalSessionID,
        intent: TerminalEndIntent,
    ) -> ServiceFuture<'_, Result<(), CoreError>> {
        Box::pin(async move {
            self.terminal_ack(w::Call::TerminalEnd {
                lease_id: w::uuid(self.lease.lease.0),
                session_id: w::uuid(session.0),
                intent: wire::EndIntent::from_core(intent),
            })
        })
    }
}
struct Terminal {
    source: Replies,
    disconnected: bool,
}
impl Stream for Terminal {
    type Item = Result<TerminalEvent, CoreError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.disconnected {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.source).poll_next(cx) {
            Poll::Ready(Some(Ok(w::Reply::Terminal { event }))) => Poll::Ready(Some(event.core())),
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Some(Err(w::invalid()))),
            Poll::Ready(Some(Err(_))) if self.source.request.connection_lost() => {
                self.disconnected = true;
                Poll::Ready(Some(Ok(TerminalEvent::Disconnected {
                    expires_at_unix_seconds: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        + RETENTION_SECONDS,
                })))
            }
            other => other.map(|v| v.map(|r| r.and_then(|_| Err(w::invalid())))),
        }
    }
}
