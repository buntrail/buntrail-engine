use super::*;
pub(super) fn uncertain(id: OperationID, target: TargetIdentity, e: CoreError) -> OperationReceipt {
    // Without explicit unsent/refusal evidence, transport failure cannot exclude execution.
    OperationReceipt {
        id,
        state: OperationState::Unknown(ReconciliationEvidence {
            target,
            observed: ObservedState::Unobserved,
            explanation: format!(
                "{}; inspect the original operation; automatic replay is disabled",
                e.message
            ),
        }),
    }
}
impl OperationService for RemoteServices {
    fn inspect(
        &self,
        id: OperationID,
        target: TargetIdentity,
    ) -> ServiceFuture<'_, Result<OperationReceipt, CoreError>> {
        Box::pin(async move {
            self.check(&target)?;
            match self.call(w::Call::Inspect, Some(id))? {
                w::Reply::Receipt { receipt } => {
                    let r = receipt.core(&target)?;
                    if r.id != id {
                        return Err(w::invalid());
                    }
                    Ok(r)
                }
                _ => Err(w::invalid()),
            }
        })
    }
}

impl RemoteServices {
    pub(super) fn mutation_call(
        &self,
        call: w::Call,
        id: OperationID,
        target: &TargetIdentity,
    ) -> OperationReceipt {
        let lookup_sensitive = call.unprepared_worktree_mutation();
        let mut replies = match self.stream(call, Some(id)) {
            Ok(stream) => stream,
            Err(error) if lookup_sensitive => return uncertain(id, target.clone(), error),
            Err(error) => return buntrail_services::files::failure(id, target.clone(), error),
        };
        let result = (|| {
            loop {
                let frame = self.next_reply(&mut replies.request)?;
                // Admission gates use the existing receipt with explicit no-effect evidence.
                if matches!(frame.header.message, Message::Receipt { .. }) {
                    let receipt = super::files::legacy_receipt(id, target, frame.header.message)?;
                    replies.request.finish();
                    return Ok(receipt);
                }
                if let Some(value) = replies.accept(frame)? {
                    let w::Reply::Receipt { receipt } = value? else {
                        return Err(w::invalid());
                    };
                    let receipt = receipt.core(target)?;
                    if receipt.id != id {
                        return Err(w::invalid());
                    }
                    return Ok(receipt);
                }
            }
        })();
        result.unwrap_or_else(|error| {
            if lookup_sensitive {
                uncertain(id, target.clone(), error)
            } else {
                request_failure(&replies.request, id, target, error)
            }
        })
    }
}

pub(super) fn request_failure(
    request: &transport::Request,
    id: OperationID,
    target: &TargetIdentity,
    error: CoreError,
) -> OperationReceipt {
    if request.refused_before_send() {
        buntrail_services::files::failure(id, target.clone(), error)
    } else {
        uncertain(id, target.clone(), error)
    }
}
