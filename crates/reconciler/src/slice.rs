//! Parse-don't-validate boundary for complete Cloudflare desired levels.

use buffa::Message as _;
use buffa::MessageView as _;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::context::ComponentContext;
use crate::context::ContextError;
use crate::proto::GraphSlice;
use crate::proto::ReconcileSliceRequestView;
use crate::proto::RegisteredComponentSpecView;

/// Complete validated desired level.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DesiredSlice {
    /// Graph identity.
    pub graph_id: [u8; 16],
    /// Graph generation.
    pub generation: u64,
    /// Slice materialization sequence.
    pub sequence: u64,
    /// Cloudflare-owned components.
    pub components: IdOrdMap<ComponentPin>,
    /// Current-generation upstream values.
    pub upstream_outputs: IdOrdMap<UpstreamOutput>,
}

/// One immutable component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentPin {
    /// Core content hash.
    pub spec_hash: [u8; 32],
    /// Human-facing name.
    pub name: String,
    /// Connector deployment context.
    pub context: ComponentContext,
}

impl IdOrdItem for ComponentPin {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.spec_hash
    }
}

/// Canonical upstream values keyed by producer spec hash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpstreamOutput {
    /// Producer hash.
    pub component_spec_hash: [u8; 32],
    /// JSON object bytes.
    pub values_json: Vec<u8>,
}

impl IdOrdItem for UpstreamOutput {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

/// Shared-contract or connector-context violation.
#[derive(Debug, Error)]
pub enum SliceError {
    /// Required contract data is malformed.
    #[error("{0}")]
    Invalid(String),
    /// Component context is malformed.
    #[error("component {component}: {source}")]
    Context {
        component: String,
        source: ContextError,
    },
}

impl DesiredSlice {
    /// Parse an accepted request view.
    pub fn from_request(request: &ReconcileSliceRequestView<'_>) -> Result<Self, SliceError> {
        Self::from_view(
            request
                .slice
                .as_option()
                .ok_or_else(|| SliceError::Invalid("slice is required".into()))?,
        )
    }

    /// Parse an exact recovered owned message.
    pub fn from_recovered(slice: &GraphSlice) -> Result<Self, SliceError> {
        let bytes = slice.encode_to_vec();
        let view = crate::proto::GraphSliceView::decode_view(&bytes)
            .map_err(|error| SliceError::Invalid(error.to_string()))?;
        Self::from_view(&view)
    }

    fn from_view(slice: &crate::proto::GraphSliceView<'_>) -> Result<Self, SliceError> {
        let graph_id = exact(slice.graph_id, "slice.graph_id")?;
        let generation = slice.generation.filter(|value| *value > 0).ok_or_else(|| {
            SliceError::Invalid("slice.generation must be greater than zero".into())
        })?;
        let sequence = slice
            .sequence
            .ok_or_else(|| SliceError::Invalid("slice.sequence is required".into()))?;
        if slice.connector != Some(crate::CONNECTOR_NAME) {
            return Err(SliceError::Invalid(format!(
                "slice.connector must be {:?}",
                crate::CONNECTOR_NAME
            )));
        }
        let mut components = IdOrdMap::with_capacity(slice.components.len());
        for item in &slice.components {
            let component = parse_component(item)?;
            components
                .insert_unique(component)
                .map_err(|_| SliceError::Invalid("duplicate component hash".into()))?;
        }
        let mut upstream_outputs = IdOrdMap::with_capacity(slice.upstream_outputs.len());
        for item in &slice.upstream_outputs {
            let component_spec_hash =
                exact(item.component_spec_hash, "upstream component_spec_hash")?;
            let value: serde_json::Value =
                serde_json::from_slice(item.values_json.unwrap_or_default())
                    .map_err(|error| SliceError::Invalid(error.to_string()))?;
            let values_json = serde_json::to_vec(&value)
                .map_err(|error| SliceError::Invalid(error.to_string()))?;
            upstream_outputs
                .insert_unique(UpstreamOutput {
                    component_spec_hash,
                    values_json,
                })
                .map_err(|_| SliceError::Invalid("duplicate upstream output".into()))?;
        }
        Ok(Self {
            graph_id,
            generation,
            sequence,
            components,
            upstream_outputs,
        })
    }

    /// Digest the complete delivered level, including its sequence identity.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("desired slice serializes");
        *blake3::hash(&bytes).as_bytes()
    }

    /// Stable key for one component's spec and the upstream values it consumes.
    #[must_use]
    pub fn deployment_key(&self, component: &ComponentPin) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"henosis.dev/cloudflare-deployment/v1\0");
        hasher.update(&component.spec_hash);
        for slot in &component.context.slots {
            hasher.update(&slot.producer_spec_hash);
            hasher.update(slot.output.as_bytes());
            if let Some(output) = self
                .upstream_outputs
                .iter()
                .find(|output| output.component_spec_hash == slot.producer_spec_hash)
            {
                hasher.update(&output.values_json);
            }
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Digest only material that can change a Cloudflare deployment.
    #[must_use]
    pub fn target_digest(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(&(
            self.graph_id,
            self.generation,
            &self.components,
            &self.upstream_outputs,
        ))
        .expect("desired target material serializes");
        *blake3::hash(&bytes).as_bytes()
    }
}

fn parse_component(item: &RegisteredComponentSpecView<'_>) -> Result<ComponentPin, SliceError> {
    let spec_hash = exact(item.hash, "component.hash")?;
    let spec = item
        .spec
        .as_option()
        .ok_or_else(|| SliceError::Invalid("component spec is required".into()))?;
    let encoded = spec
        .to_owned_message()
        .map_err(|error| SliceError::Invalid(error.to_string()))?
        .encode_to_vec();
    if blake3::hash(&encoded).as_bytes() != &spec_hash {
        return Err(SliceError::Invalid(
            "component hash does not match canonical spec".into(),
        ));
    }
    let name = spec
        .name
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SliceError::Invalid("component name is required".into()))?
        .to_owned();
    if spec.connector != Some(crate::CONNECTOR_NAME) {
        return Err(SliceError::Invalid(format!(
            "component {name:?} is not Cloudflare-owned"
        )));
    }
    let context = ComponentContext::from_bytes(spec.connector_context.unwrap_or_default())
        .map_err(|source| SliceError::Context {
            component: name.clone(),
            source,
        })?;
    Ok(ComponentPin {
        spec_hash,
        name,
        context,
    })
}

fn exact<const N: usize>(value: Option<&[u8]>, field: &str) -> Result<[u8; N], SliceError> {
    value
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SliceError::Invalid(format!("{field} must contain exactly {N} bytes")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::API_VERSION;
    use crate::context::InputSlot;

    fn desired(upstream: &str) -> DesiredSlice {
        let component = ComponentPin {
            spec_hash: [2; 32],
            name: "service-e".into(),
            context: ComponentContext {
                api_version: API_VERSION.into(),
                worker_name: "service-e".into(),
                entry: "src/index.js".into(),
                assets_directory: None,
                environment: "preview_01kxcq3c85ey9rz1q3738e8114".into(),
                files: Vec::new(),
                slots: vec![InputSlot {
                    key: "SUPABASE_URL".into(),
                    producer: "service-d".into(),
                    output: "restUrl".into(),
                    producer_spec_hash: [1; 32],
                }],
                tunnel: None,
            },
        };
        let output = UpstreamOutput {
            component_spec_hash: [1; 32],
            values_json: serde_json::to_vec(&serde_json::json!({"restUrl": upstream})).unwrap(),
        };
        DesiredSlice {
            graph_id: [3; 16],
            generation: 1,
            sequence: 1,
            components: IdOrdMap::from_iter_unique([component]).unwrap(),
            upstream_outputs: IdOrdMap::from_iter_unique([output]).unwrap(),
        }
    }

    #[test]
    fn deployment_key_ignores_unrelated_components_but_tracks_consumed_outputs() {
        let first = desired("https://database.example/rest/v1");
        let second = desired("https://database.example/rest/v2");
        let component = first.components.iter().next().unwrap();

        assert_ne!(
            first.deployment_key(component),
            second.deployment_key(second.components.iter().next().unwrap())
        );
        let mut unrelated = first.clone();
        unrelated.generation = 2;
        unrelated.sequence = 9;
        assert_eq!(
            first.deployment_key(component),
            unrelated.deployment_key(unrelated.components.iter().next().unwrap())
        );
    }
}
