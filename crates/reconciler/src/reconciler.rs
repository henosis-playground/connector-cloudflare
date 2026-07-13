//! Cloudflare target lifecycle implemented against the shared connector SDK.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use connector_sdk::ApplyOutcome;
use connector_sdk::Approved;
use connector_sdk::BlockedInput;
use connector_sdk::ConcurrencyScope;
use connector_sdk::Connector;
use connector_sdk::ContractError;
use connector_sdk::Diagnostic;
use connector_sdk::Output;
use connector_sdk::PassContext;
use connector_sdk::PlanOutcome;
use connector_sdk::PlanProposal;
use connector_sdk::Publication;
use connector_sdk::RetireContext;
use connector_sdk::RetireOutcome;
use connector_sdk::Retry;
use connector_sdk::ReviewProjection;
use connector_sdk::TargetSlice;
use connector_sdk::UnknownSlot;
use serde::Deserialize;
use serde::Serialize;

use crate::slice::ComponentPin;
use crate::slice::DesiredSlice;
use crate::target::AccountFacts;
use crate::target::Deployment;
use crate::target::Target;
use crate::target::TargetError;

/// Fresh Cloudflare facts used to classify create versus update.
pub struct Observation {
    facts: AccountFacts,
    existing_workers: BTreeSet<String>,
}

/// Complete private Cloudflare plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CloudflarePlan {
    pub desired_digest: String,
    pub operations: Vec<PlannedOperation>,
}

/// One target mutation covered by the plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlannedOperation {
    pub action: PlanAction,
    pub worker_name: String,
    pub component: ComponentPin,
}

/// Cloudflare mutation classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlanAction {
    Create,
    Update,
    Delete,
}

/// Cloudflare lifecycle hooks.
pub struct CloudflareConnector {
    target: Target,
}

impl CloudflareConnector {
    #[must_use]
    pub fn new(target: Target) -> Self {
        Self { target }
    }
}

#[async_trait::async_trait]
impl Connector for CloudflareConnector {
    type Desired = DesiredSlice;
    type Observation = Observation;
    type Plan = CloudflarePlan;

    fn name(&self) -> &'static str {
        crate::CONNECTOR_NAME
    }

    fn decode(&self, slice: &TargetSlice) -> Result<Self::Desired, ContractError> {
        DesiredSlice::decode(slice)
    }

    fn prepare_transition(
        &self,
        _previous_slice: &TargetSlice,
        previous: &Self::Desired,
        _next_slice: &TargetSlice,
        next: &mut Self::Desired,
    ) -> Result<(), ContractError> {
        let current_names = next
            .components
            .iter()
            .map(|component| component.context.deployed_name())
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|error| ContractError::target(error.to_string()))?;
        next.removed_components = previous
            .components
            .iter()
            .filter(|component| {
                component
                    .context
                    .deployed_name()
                    .is_ok_and(|name| !current_names.contains(&name))
            })
            .cloned()
            .collect();
        Ok(())
    }

    fn concurrency_scope(&self, _desired: &Self::Desired) -> ConcurrencyScope {
        ConcurrencyScope::Connector
    }

    async fn observe(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
    ) -> Result<Self::Observation, PlanOutcome<Self::Plan>> {
        let facts = self
            .target
            .account_facts()
            .await
            .map_err(|error| blocked_target_plan("account facts", &error))?;
        let mut existing_workers = BTreeSet::new();
        for component in &desired.components {
            if component.context.is_tunnel() {
                continue;
            }
            let name = component
                .context
                .deployed_name()
                .map_err(|error| failed_plan("worker identity", error.to_string()))?;
            if self
                .target
                .worker_exists(&name)
                .await
                .map_err(|error| blocked_target_plan("worker observation", &error))?
            {
                existing_workers.insert(name);
            }
        }
        Ok(Observation {
            facts,
            existing_workers,
        })
    }

    async fn plan(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        observed: &Self::Observation,
    ) -> PlanOutcome<Self::Plan> {
        let blocked = blocked_inputs(desired);
        if !blocked.is_empty() {
            let mut proposal = PlanProposal::declarative(ReviewProjection {
                json: serde_json::json!({
                    "status": "blocked",
                    "blockedOnInputs": blocked,
                }),
                markdown: blocked_markdown(&blocked),
            });
            proposal.blocked_on_inputs = blocked;
            return PlanOutcome::Waiting {
                proposal,
                diagnostics: vec![Diagnostic::info(
                    "cloudflare.input.unbound",
                    "Cloudflare plan is blocked on required producer outputs",
                )],
                retry: Retry::after(Duration::from_secs(2)),
            };
        }

        let mut operations = Vec::new();
        for component in &desired.removed_components {
            let worker_name = match component.context.deployed_name() {
                Ok(value) => value,
                Err(error) => return failed_plan("removed worker identity", error.to_string()),
            };
            operations.push(PlannedOperation {
                action: PlanAction::Delete,
                worker_name,
                component: component.clone(),
            });
        }
        for component in &desired.components {
            let worker_name = match component.context.deployed_name() {
                Ok(value) => value,
                Err(error) => return failed_plan("worker identity", error.to_string()),
            };
            let action = if observed.existing_workers.contains(&worker_name) {
                PlanAction::Update
            } else {
                PlanAction::Create
            };
            operations.push(PlannedOperation {
                action,
                worker_name,
                component: component.clone(),
            });
        }
        let plan = CloudflarePlan {
            desired_digest: format!("blake3:{}", hex::encode(desired.digest())),
            operations,
        };
        let projection = review_projection(&plan, desired, &observed.facts);
        if plan.operations.is_empty() {
            return PlanOutcome::Ready {
                proposal: PlanProposal::executable(plan, projection),
                outputs: Vec::new(),
                diagnostics: Vec::new(),
                publication: None,
            };
        }
        let mut proposal = PlanProposal::executable(plan, projection);
        for component in &desired.components {
            let hash = hex::encode(component.spec_hash);
            let fields = if component.context.is_tunnel() {
                ["tunnelId", "capability"]
            } else {
                ["deploymentId", "versionId"]
            };
            for field in fields {
                proposal.unknown_slots.push(UnknownSlot {
                    path: format!("/outputs/{hash}/{field}"),
                    reason: "Cloudflare assigns this value while applying the deployment".into(),
                });
            }
            if !component.context.is_tunnel() {
                proposal.unknown_slots.push(UnknownSlot {
                    path: format!("/outputs/{hash}/claimUrl"),
                    reason: "present only when Cloudflare returns a temporary-account claim URL"
                        .into(),
                });
            }
            if observed.facts.subdomain.is_empty() && !component.context.is_tunnel() {
                proposal.unknown_slots.push(UnknownSlot {
                    path: format!("/outputs/{hash}/url"),
                    reason: "workers.dev subdomain is discovered from Wrangler during apply".into(),
                });
            }
        }
        PlanOutcome::Apply(proposal)
    }

    async fn apply(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        approved: &Approved<Self::Plan>,
    ) -> ApplyOutcome {
        let plan = approved.plan();
        if plan.desired_digest != format!("blake3:{}", hex::encode(desired.digest())) {
            return ApplyOutcome::Failed(vec![Diagnostic::error(
                "cloudflare.plan.identity",
                "approved plan does not match the current desired level",
            )]);
        }
        let facts = match self.target.account_facts().await {
            Ok(value) => value,
            Err(error) => return target_waiting(&error),
        };
        let mut deployments = BTreeMap::new();
        for operation in &plan.operations {
            match operation.action {
                PlanAction::Delete => {
                    if let Err(error) = self.target.delete(&operation.component).await {
                        return target_failure(&error);
                    }
                }
                PlanAction::Create | PlanAction::Update => {
                    match self
                        .target
                        .deploy(desired, &operation.component, &facts)
                        .await
                    {
                        Ok(deployment) => {
                            deployments.insert(operation.component.spec_hash, deployment);
                        }
                        Err(error) => return target_failure(&error),
                    }
                }
            }
        }
        let outputs = match ready_outputs(desired, &deployments) {
            Ok(outputs) => outputs,
            Err(message) => {
                return ApplyOutcome::Failed(vec![Diagnostic::error(
                    "cloudflare.outputs.incomplete",
                    message,
                )]);
            }
        };
        ApplyOutcome::Ready {
            outputs,
            diagnostics: vec![Diagnostic::info(
                "cloudflare.plan.applied",
                format!(
                    "S2 plan {} applied {} Cloudflare mutations",
                    approved.digest(),
                    plan.operations.len()
                ),
            )],
            publication: (!desired.components.is_empty()).then(|| Publication {
                revision: format!("generation:{}", desired.generation),
                uri: "cloudflare://workers/deployments".into(),
            }),
        }
    }

    async fn retire(
        &self,
        _context: RetireContext<'_>,
        desired: Option<&Self::Desired>,
    ) -> RetireOutcome {
        let Some(desired) = desired else {
            return RetireOutcome::Blocked(vec![Diagnostic::error(
                "cloudflare.retire.unknown",
                "cannot retire an unknown Cloudflare graph",
            )]);
        };
        for component in &desired.components {
            if let Err(error) = self.target.delete(component).await {
                return match error {
                    TargetError::Config(detail) => RetireOutcome::Blocked(vec![Diagnostic::error(
                        "cloudflare.retire.blocked",
                        detail,
                    )]),
                    TargetError::Provider(_) | TargetError::Stage(_) => {
                        RetireOutcome::Waiting(Retry::after(Duration::from_secs(2)))
                    }
                };
            }
        }
        RetireOutcome::Absent
    }
}

fn blocked_inputs(desired: &DesiredSlice) -> Vec<BlockedInput> {
    let mut blocked = Vec::new();
    for component in &desired.components {
        for slot in &component.context.slots {
            let available = desired
                .upstream_outputs
                .iter()
                .find(|output| output.component_spec_hash == slot.producer_spec_hash)
                .and_then(|output| {
                    serde_json::from_slice::<serde_json::Value>(&output.values_json).ok()
                })
                .and_then(|value| value.get(&slot.output).cloned())
                .is_some();
            if !available {
                blocked.push(BlockedInput {
                    path: format!(
                        "/components/{}/vars/{}",
                        hex::encode(component.spec_hash),
                        slot.key
                    ),
                    producer: hex::encode(slot.producer_spec_hash),
                    output: slot.output.clone(),
                });
            }
        }
    }
    blocked
}

fn review_projection(
    plan: &CloudflarePlan,
    desired: &DesiredSlice,
    facts: &AccountFacts,
) -> ReviewProjection {
    let operations = plan
        .operations
        .iter()
        .map(|operation| {
            let vars = operation
                .component
                .context
                .slots
                .iter()
                .map(|slot| slot.key.clone())
                .collect::<Vec<_>>();
            serde_json::json!({
                "action": operation.action,
                "workerName": operation.worker_name,
                "vars": vars,
                "componentSpecHash": hex::encode(operation.component.spec_hash),
            })
        })
        .collect::<Vec<_>>();
    let planned_outputs = desired
        .components
        .iter()
        .map(|component| {
            let worker_name = component.context.deployed_name().unwrap_or_default();
            let url = (!facts.subdomain.is_empty() && !component.context.is_tunnel())
                .then(|| format!("https://{worker_name}.{}.workers.dev", facts.subdomain));
            serde_json::json!({
                "componentSpecHash": hex::encode(component.spec_hash),
                "workerName": worker_name,
                "url": url,
            })
        })
        .collect::<Vec<_>>();
    let markdown_operations = plan
        .operations
        .iter()
        .map(|operation| {
            format!(
                "- **{:?}** `{}` (vars: {})",
                operation.action,
                operation.worker_name,
                operation
                    .component
                    .context
                    .slots
                    .iter()
                    .map(|slot| slot.key.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    ReviewProjection {
        json: serde_json::json!({
            "desiredDigest": plan.desired_digest,
            "operations": operations,
            "plannedOutputs": planned_outputs,
        }),
        markdown: format!("# Cloudflare plan\n\n{markdown_operations}"),
    }
}

fn blocked_markdown(blocked: &[BlockedInput]) -> String {
    let items = blocked
        .iter()
        .map(|input| {
            format!(
                "- `{}` waits on `{}.{}`",
                input.path, input.producer, input.output
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("# Cloudflare plan\n\nBlocked inputs:\n\n{items}")
}

fn ready_outputs(
    desired: &DesiredSlice,
    deployments: &BTreeMap<[u8; 32], Deployment>,
) -> Result<Vec<Output>, String> {
    desired
        .components
        .iter()
        .map(|component| {
            let deployment = deployments
                .get(&component.spec_hash)
                .ok_or_else(|| format!("deployment missing for component {:?}", component.name))?;
            let worker_name = component
                .context
                .deployed_name()
                .map_err(|error| error.to_string())?;
            let mut values = if let Some(tunnel) = &component.context.tunnel {
                serde_json::json!({
                    "tunnelId": deployment.deployment_id,
                    "tunnelName": worker_name,
                    "privateHostname": format!("{}:{}", tunnel.origin_host, tunnel.origin_port),
                    "tokenRef": "connector-volume://henosis-tunnel/token",
                    "capability": deployment.version_id,
                })
            } else {
                serde_json::json!({
                    "url": deployment.url,
                    "workerName": worker_name,
                    "deploymentId": deployment.deployment_id,
                    "versionId": deployment.version_id,
                })
            };
            if let Some(claim_url) = &deployment.claim_url {
                values["claimUrl"] = serde_json::Value::String(claim_url.clone());
            }
            Ok(Output {
                component_spec_hash: component.spec_hash,
                values,
            })
        })
        .collect()
}

fn declarative_review(status: &str, phase: &str, detail: &str) -> ReviewProjection {
    ReviewProjection {
        json: serde_json::json!({"status": status, "phase": phase, "detail": detail}),
        markdown: format!("# Cloudflare plan\n\n**Status:** {status}\n\n{detail}"),
    }
}

fn failed_plan(phase: &str, detail: String) -> PlanOutcome<CloudflarePlan> {
    PlanOutcome::Failed {
        proposal: PlanProposal::declarative(declarative_review("failed", phase, &detail)),
        diagnostics: vec![Diagnostic::error("cloudflare.plan.invalid", detail)],
    }
}

fn blocked_target_plan(phase: &str, error: &TargetError) -> PlanOutcome<CloudflarePlan> {
    let detail = error.to_string();
    PlanOutcome::Waiting {
        proposal: PlanProposal::declarative(declarative_review("blocked", phase, &detail)),
        diagnostics: vec![Diagnostic::warning("cloudflare.target.unavailable", detail)],
        retry: Retry::after(Duration::from_secs(2)),
    }
}

fn target_waiting(error: &TargetError) -> ApplyOutcome {
    ApplyOutcome::Waiting {
        diagnostics: vec![Diagnostic::warning(
            "cloudflare.target.unavailable",
            error.to_string(),
        )],
        retry: Retry::after(Duration::from_secs(2)),
    }
}

fn target_failure(error: &TargetError) -> ApplyOutcome {
    match error {
        TargetError::Config(detail) => ApplyOutcome::Failed(vec![Diagnostic::error(
            "cloudflare.target.config",
            detail.clone(),
        )]),
        TargetError::Provider(_) | TargetError::Stage(_) => target_waiting(error),
    }
}

#[cfg(test)]
mod tests {
    use iddqd::IdOrdMap;

    use super::*;
    use crate::context::API_VERSION;
    use crate::context::ComponentContext;
    use crate::context::InputSlot;
    use crate::context::ProjectFile;
    use crate::slice::UpstreamOutput;
    use crate::target::TargetConfig;

    fn connector() -> CloudflareConnector {
        CloudflareConnector::new(Target::new(TargetConfig {
            wrangler: "wrangler".into(),
            account_id: None,
            api_token: None,
            wrangler_config: None,
            account_subdomain: Some("example".into()),
            secret_root: "/run/secrets".into(),
            api_base: "https://api.cloudflare.test".into(),
            tunnel_token_file: "/tmp/token".into(),
        }))
    }

    fn component(hash: u8, name: &str, slots: Vec<InputSlot>) -> ComponentPin {
        ComponentPin {
            spec_hash: [hash; 32],
            name: name.into(),
            context: ComponentContext {
                api_version: API_VERSION.into(),
                worker_name: name.into(),
                entry: "src/index.js".into(),
                assets_directory: None,
                environment: "dev".into(),
                files: vec![ProjectFile {
                    path: "src/index.js".into(),
                    bytes: b"export default {}".to_vec(),
                }],
                slots,
                tunnel: None,
            },
        }
    }

    fn desired(component: ComponentPin, outputs: Vec<UpstreamOutput>) -> DesiredSlice {
        DesiredSlice {
            graph_id: [9; 16],
            generation: 2,
            sequence: 3,
            components: IdOrdMap::from_iter_unique([component]).unwrap(),
            upstream_outputs: IdOrdMap::from_iter_unique(outputs).unwrap(),
            removed_components: Vec::new(),
        }
    }

    #[tokio::test]
    async fn plan_declares_worker_mutation_vars_and_apply_time_unknowns() {
        let slot = InputSlot {
            key: "BACKEND_URL".into(),
            producer: "api".into(),
            output: "url".into(),
            producer_spec_hash: [4; 32],
        };
        let desired = desired(
            component(5, "web", vec![slot]),
            vec![UpstreamOutput {
                component_spec_hash: [4; 32],
                values_json: serde_json::to_vec(&serde_json::json!({"url":"https://api"})).unwrap(),
            }],
        );
        let outcome = connector()
            .plan(
                PassContext {
                    connector: crate::CONNECTOR_NAME,
                    graph_id: desired.graph_id,
                    generation: desired.generation,
                    sequence: desired.sequence,
                    idempotency_key: "test",
                },
                &desired,
                &Observation {
                    facts: AccountFacts {
                        subdomain: "example".into(),
                    },
                    existing_workers: BTreeSet::new(),
                },
            )
            .await;
        let PlanOutcome::Apply(proposal) = outcome else {
            panic!("expected apply plan")
        };
        assert!(proposal.plan.is_some());
        assert!(
            proposal
                .unknown_slots
                .iter()
                .any(|slot| slot.path.ends_with("/deploymentId"))
        );
        assert_eq!(proposal.review.json["operations"][0]["action"], "create");
        assert_eq!(
            proposal.review.json["operations"][0]["vars"][0],
            "BACKEND_URL"
        );
    }

    #[tokio::test]
    async fn missing_producer_value_is_a_durable_blocked_plan() {
        let desired = desired(
            component(
                5,
                "web",
                vec![InputSlot {
                    key: "BACKEND_URL".into(),
                    producer: "api".into(),
                    output: "url".into(),
                    producer_spec_hash: [4; 32],
                }],
            ),
            Vec::new(),
        );
        let outcome = connector()
            .plan(
                PassContext {
                    connector: crate::CONNECTOR_NAME,
                    graph_id: desired.graph_id,
                    generation: desired.generation,
                    sequence: desired.sequence,
                    idempotency_key: "test",
                },
                &desired,
                &Observation {
                    facts: AccountFacts {
                        subdomain: "example".into(),
                    },
                    existing_workers: BTreeSet::new(),
                },
            )
            .await;
        let PlanOutcome::Waiting { proposal, .. } = outcome else {
            panic!("expected blocked plan")
        };
        assert_eq!(proposal.blocked_on_inputs.len(), 1);
        assert_eq!(proposal.blocked_on_inputs[0].output, "url");
        assert!(proposal.plan.is_none());
    }

    #[test]
    fn transition_carries_removed_workers_into_delete_plan_context() {
        let prior = desired(component(1, "old-worker", Vec::new()), Vec::new());
        let mut next = desired(component(2, "new-worker", Vec::new()), Vec::new());
        connector()
            .prepare_transition(
                &TargetSlice {
                    graph_id: prior.graph_id,
                    generation: 1,
                    sequence: 1,
                    connector: crate::CONNECTOR_NAME.into(),
                    components: Vec::new(),
                    upstream_outputs: Vec::new(),
                    superseded_components: Vec::new(),
                },
                &prior,
                &TargetSlice {
                    graph_id: next.graph_id,
                    generation: 2,
                    sequence: 2,
                    connector: crate::CONNECTOR_NAME.into(),
                    components: Vec::new(),
                    upstream_outputs: Vec::new(),
                    superseded_components: Vec::new(),
                },
                &mut next,
            )
            .unwrap();
        assert_eq!(next.removed_components[0].context.worker_name, "old-worker");
    }
}
