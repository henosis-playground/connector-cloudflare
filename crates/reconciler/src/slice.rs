//! Decode complete Cloudflare desired levels from the shared connector SDK.

use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;

use crate::context::ComponentContext;

/// Complete validated desired level plus transition-owned removals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DesiredSlice {
    pub graph_id: [u8; 16],
    pub generation: u64,
    pub sequence: u64,
    pub components: IdOrdMap<ComponentPin>,
    pub upstream_outputs: IdOrdMap<UpstreamOutput>,
    #[serde(default)]
    pub removed_components: Vec<ComponentPin>,
}

/// One immutable component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentPin {
    pub spec_hash: [u8; 32],
    pub name: String,
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
    pub component_spec_hash: [u8; 32],
    pub values_json: Vec<u8>,
}

impl IdOrdItem for UpstreamOutput {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

impl DesiredSlice {
    /// Decode target-specific context after the SDK validates the shared wire.
    pub fn decode(
        slice: &connector_sdk::TargetSlice,
    ) -> Result<Self, connector_sdk::ContractError> {
        let mut components = IdOrdMap::with_capacity(slice.components.len());
        for item in &slice.components {
            let context =
                ComponentContext::from_bytes(&item.connector_context).map_err(|error| {
                    connector_sdk::ContractError::target(format!(
                        "component {:?}: {error}",
                        item.name
                    ))
                })?;
            components
                .insert_unique(ComponentPin {
                    spec_hash: item.spec_hash,
                    name: item.name.clone(),
                    context,
                })
                .map_err(|_| connector_sdk::ContractError::target("duplicate component hash"))?;
        }
        let upstream_outputs =
            IdOrdMap::from_iter_unique(slice.upstream_outputs.iter().map(|output| {
                UpstreamOutput {
                    component_spec_hash: output.component_spec_hash,
                    values_json: output.values_json.clone(),
                }
            }))
            .map_err(|_| connector_sdk::ContractError::target("duplicate upstream output"))?;
        Ok(Self {
            graph_id: slice.graph_id,
            generation: slice.generation,
            sequence: slice.sequence,
            components,
            upstream_outputs,
            removed_components: Vec::new(),
        })
    }

    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("desired slice serializes");
        *blake3::hash(&bytes).as_bytes()
    }

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
            removed_components: Vec::new(),
        }
    }

    #[test]
    fn deployment_key_ignores_level_identity_but_tracks_consumed_outputs() {
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
