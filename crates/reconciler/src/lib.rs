//! Cloudflare Workers connector with plan-time URL propagation and apply-time
//! identities.

use std::sync::Arc;

use connectrpc::ConnectError;
use connectrpc::ErrorCode;
use connectrpc::RequestContext;
use connectrpc::ServiceRequest;
use connectrpc::ServiceResult;

use crate::proto::ConnectorService;
use crate::proto::ReconcileSliceRequest;
use crate::proto::ReconcileSliceResponse;
use crate::proto::RetireSliceRequest;
use crate::proto::RetireSliceResponse;
use crate::reconciler::ReconcileError;
use crate::reconciler::Reconciler;

pub mod context;
pub mod proto;
pub mod reconciler;
pub mod slice;
pub mod target;

/// Connector registry key.
pub const CONNECTOR_NAME: &str = "cloudflare";

/// Generated service adapter.
pub struct ConnectorHandler {
    reconciler: Arc<Reconciler>,
}

impl ConnectorHandler {
    /// Wrap a configured reconciler.
    pub fn new(reconciler: Arc<Reconciler>) -> Self {
        Self { reconciler }
    }
}

impl ConnectorService for ConnectorHandler {
    async fn reconcile_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ReconcileSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<ReconcileSliceResponse> + Send + use<'a>> {
        let sequence = self
            .reconciler
            .accept(&request)
            .await
            .map_err(|error| connect_error(&error))?;
        Ok(ReconcileSliceResponse::default()
            .with_accepted_sequence(sequence)
            .into())
    }

    async fn retire_slice<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, RetireSliceRequest>,
    ) -> ServiceResult<impl connectrpc::Encodable<RetireSliceResponse> + Send + use<'a>> {
        let slice = request
            .slice
            .as_option()
            .ok_or_else(|| ConnectError::invalid_argument("slice is required"))?;
        if slice.connector != Some(CONNECTOR_NAME) {
            return Err(ConnectError::invalid_argument(
                "slice.connector must be cloudflare",
            ));
        }
        let graph_id = slice
            .graph_id
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                ConnectError::invalid_argument("slice.graph_id must contain 16 bytes")
            })?;
        let generation = slice
            .generation
            .filter(|value| *value > 0)
            .ok_or_else(|| ConnectError::invalid_argument("slice.generation is required"))?;
        let sequence = slice
            .sequence
            .ok_or_else(|| ConnectError::invalid_argument("slice.sequence is required"))?;
        let retired = self
            .reconciler
            .retire(graph_id, generation, sequence)
            .await
            .map_err(|error| connect_error(&error))?;
        Ok(RetireSliceResponse::default()
            .with_retired_generation(retired)
            .into())
    }
}

fn connect_error(error: &ReconcileError) -> ConnectError {
    let code = match error {
        ReconcileError::Invalid(_) => ErrorCode::InvalidArgument,
        ReconcileError::Retired | ReconcileError::SequenceConflict => ErrorCode::FailedPrecondition,
        ReconcileError::State(_) => ErrorCode::Internal,
    };
    ConnectError::new(code, error.to_string())
}
