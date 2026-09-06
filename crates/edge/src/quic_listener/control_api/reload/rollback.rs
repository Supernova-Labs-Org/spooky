use std::sync::Arc;

use super::{request_body::ControlApiRuntimeRollbackPayload, *};

impl QUICListener {
    pub(in crate::quic_listener::control_api) async fn handle_control_api_runtime_rollback(
        req: Request<Incoming>,
        state: &crate::quic_listener::runtime_state::ControlApiServiceCtx,
    ) -> Response<Full<Bytes>> {
        let runtime_state = state.current_service_state();
        let authorization_generation = req
            .extensions()
            .get::<super::super::state::ControlApiAuthorizationGeneration>()
            .copied();
        let identity = req.extensions().get::<AdminIdentity>().cloned();
        let request_context = req.extensions().get::<ControlApiRequestContext>().cloned();
        let Some(runtime_bundle_handle) = runtime_state.runtime_bundle_handle().cloned() else {
            return Self::control_api_not_found_response();
        };
        let payload =
            match Self::control_api_json_body::<ControlApiRuntimeRollbackPayload>(req).await {
                Ok(payload) => payload,
                Err(response) => {
                    Self::emit_control_api_audit_event(
                        &runtime_state.security,
                        identity.as_ref(),
                        request_context.as_ref(),
                        AdminAuditEventType::RuntimeRollback,
                        AdminAuditAction::RuntimeRollbackResult,
                        Self::control_api_audit_target_for_route(
                            ControlApiRoute::RuntimeRollback,
                            None,
                        ),
                        AdminAuditGeneration::default(),
                        AdminAuditResult::Failed,
                        Some("invalid_request_body".to_string()),
                    );
                    return *response;
                }
            };
        let current_auth_generation = state.current_service_state().auth_policy_generation();
        let authorization_is_current = authorization_generation.is_some_and(|expected| {
            expected.runtime == current_auth_generation.0
                && expected.listener_tls == current_auth_generation.1
        });
        if !authorization_is_current {
            return Self::stale_control_api_connection_response();
        }
        let current_generation = runtime_bundle_handle.current_generation();
        Self::emit_control_api_audit_event(
            &runtime_state.security,
            identity.as_ref(),
            request_context.as_ref(),
            AdminAuditEventType::RuntimeRollback,
            AdminAuditAction::RuntimeRollbackAttempt,
            Self::control_api_audit_target_for_route(ControlApiRoute::RuntimeRollback, None),
            AdminAuditGeneration {
                active_generation: Some(current_generation),
                target_generation: Some(payload.target_generation),
                ..Default::default()
            },
            AdminAuditResult::Success,
            payload
                .reason
                .clone()
                .or_else(|| Some("runtime_rollback".to_string())),
        );
        let rollback = RuntimeActivationService::rollback_generation(
            &runtime_bundle_handle,
            RollbackRequest {
                target_generation: payload.target_generation,
                requested_by: payload
                    .requested_by
                    .or_else(|| Some("control_api".to_string())),
                trigger_source: Some("control_api".to_string()),
                reason: payload
                    .reason
                    .or_else(|| Some("runtime_rollback".to_string())),
                expected_active_generation: payload.expected_active_generation,
                requested_at_ms: crate::watchdog::time::now_millis(),
            },
        );
        Self::record_control_api_rollback_outcome(&runtime_bundle_handle, &rollback);
        Self::emit_control_api_audit_event(
            &runtime_state.security,
            identity.as_ref(),
            request_context.as_ref(),
            AdminAuditEventType::RuntimeRollback,
            AdminAuditAction::RuntimeRollbackResult,
            Self::control_api_audit_target_for_route(ControlApiRoute::RuntimeRollback, None),
            AdminAuditGeneration {
                active_generation: Some(rollback.active_generation),
                target_generation: Some(rollback.request.target_generation),
                candidate_generation: rollback.rolled_back_to,
            },
            if rollback.succeeded() {
                AdminAuditResult::Success
            } else {
                AdminAuditResult::Failed
            },
            Some(
                rollback
                    .primary_rejection_reason()
                    .map(|reason| reason.slug().to_string())
                    .unwrap_or_else(|| rollback.outcome_reason().slug().to_string()),
            ),
        );
        if !rollback.succeeded() {
            return Self::json_response(
                rollback_result_status(&rollback),
                rollback_error_payload(&rollback, rollback_error(&rollback)),
            );
        }
        Self::json_response(StatusCode::ACCEPTED, rollback)
    }

    pub(super) fn record_control_api_rollback_outcome(
        handle: &RuntimeBundleHandle,
        rollback: &RollbackResult,
    ) {
        let current = handle.current_view();
        let metrics = Arc::clone(&current.shared_services().metrics);
        let outcome = rollback.outcome_reason();
        metrics.record_runtime_rollback_outcome(outcome);
        if let Some(reason) = rollback.primary_rejection_reason() {
            metrics.inc_runtime_rejection_reason(reason);
            warn!(
                "runtime rollback rejected active_generation={} target_generation={} reason={} error={}",
                rollback.active_generation,
                rollback.request.target_generation,
                outcome.slug(),
                rollback_error(rollback)
            );
        } else if let Some(rolled_back_to) = rollback.rolled_back_to {
            info!(
                "runtime rollback succeeded active_generation={} rolled_back_to={} reason={}",
                rollback.active_generation,
                rolled_back_to,
                outcome.slug()
            );
        }
    }
}

pub(super) fn rollback_result_status(rollback: &RollbackResult) -> StatusCode {
    if rollback
        .rejected_changes
        .iter()
        .any(|rejection| matches!(rejection.kind, RejectedChangeKind::InvalidConfiguration))
    {
        StatusCode::BAD_REQUEST
    } else if rollback.rejected_changes.iter().any(|rejection| {
        matches!(
            rejection.kind,
            RejectedChangeKind::ResourcePreparationFailed
        )
    }) || matches!(
        rollback.status,
        crate::runtime::activation::GenerationStatus::Failed
    ) {
        StatusCode::INTERNAL_SERVER_ERROR
    } else if rollback.rejected_changes.iter().any(|rejection| {
        rejection.kind == RejectedChangeKind::RuntimeStateUnavailable
            && rejection.reason == RuntimeRejectionReason::UnknownGeneration
    }) {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::CONFLICT
    }
}

pub(super) fn rollback_error(rollback: &RollbackResult) -> &str {
    rollback
        .rejected_changes
        .first()
        .map(|rejection| rejection.message.as_str())
        .unwrap_or(rollback.history_entry.summary.as_str())
}

pub(super) fn rollback_error_payload(rollback: &RollbackResult, error: &str) -> serde_json::Value {
    json!({
        "error": error,
        "rejection_reason": rollback.primary_rejection_reason().map(RuntimeRejectionReason::slug),
        "active_generation": rollback.active_generation,
        "target_generation": rollback.request.target_generation,
        "rolled_back_to": rollback.rolled_back_to,
        "status": rollback.status,
        "rejected_changes": rollback.rejected_changes,
        "history_entry": rollback.history_entry,
    })
}
