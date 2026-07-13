//! Durable two-phase Cloudflare reconciliation with persistent report delivery.

use std::collections::HashMap;
use std::fs::File;
use std::fs::{self};
use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use http::Uri;
use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::proto::ComponentDisposition;
use crate::proto::ComponentDispositionKind;
use crate::proto::ComponentOutputs;
use crate::proto::ConnectorCallbackServiceClient;
use crate::proto::Diagnostic;
use crate::proto::DiagnosticSeverity;
use crate::proto::PublicationEvidence;
use crate::proto::ReportSliceRequest;
use crate::proto::SliceReport;
use crate::slice::DesiredSlice;
use crate::slice::SliceError;
use crate::target::AccountFacts;
use crate::target::Deployment;
use crate::target::Target;
use crate::target::TargetError;

const CHECKPOINT_VERSION: u32 = 1;
const PUBLICATION_NAMESPACE: Uuid = Uuid::from_bytes([
    0x4f, 0x8f, 0x0c, 0xcd, 0x59, 0xa5, 0x55, 0x60, 0xa6, 0x8f, 0xa4, 0x24, 0x61, 0x29, 0xdd, 0x4e,
]);

/// Callback delivery boundary.
pub trait Reporter: Send + Sync + 'static {
    /// Atomically deliver one complete report level.
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>>;
}

/// Core callback transport failure.
#[derive(Debug, Error)]
#[error("core callback failed: {0}")]
pub struct ReportError(String);

/// Generated-contract callback client.
#[derive(Clone)]
pub struct CoreReporter {
    client: ConnectorCallbackServiceClient<HttpClient>,
}

impl CoreReporter {
    /// Build a callback client.
    pub fn new(uri: Uri, token: Option<String>) -> Self {
        let mut config = ClientConfig::new(uri);
        if let Some(token) = token {
            config = config.with_default_header("authorization", format!("Bearer {token}"));
        }
        Self {
            client: ConnectorCallbackServiceClient::new(HttpClient::plaintext(), config),
        }
    }
}

impl Reporter for CoreReporter {
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .report_slice(request)
                .await
                .map(|_| ())
                .map_err(|error| ReportError(error.to_string()))
        })
    }
}

/// Controller configuration.
#[derive(Clone, Debug)]
pub struct ReconcilerConfig {
    /// Connector-owned state root.
    pub state_dir: PathBuf,
}

/// Service-boundary reconciliation failure.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// Invalid shared request.
    #[error("invalid slice: {0}")]
    Invalid(#[from] SliceError),
    /// Retired graph cannot be reused.
    #[error("graph is retired")]
    Retired,
    /// Equal sequence changed content.
    #[error("slice sequence conflict")]
    SequenceConflict,
    /// Durable state failed.
    #[error("connector state failure: {0}")]
    State(String),
}

/// Durable level-triggered controller.
pub struct Reconciler {
    config: ReconcilerConfig,
    target: Target,
    reporter: Arc<dyn Reporter>,
    account_facts: Mutex<Option<AccountFacts>>,
    graph_locks: RwLock<HashMap<[u8; 16], Arc<Mutex<()>>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointEnvelope {
    version: u32,
    state: Checkpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Checkpoint {
    desired: DesiredSlice,
    phase: Phase,
    deployments: HashMap<String, DeploymentSnapshot>,
    pending_report: Option<ReportSnapshot>,
    retired: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Phase {
    Accepted,
    Planned,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentSnapshot {
    url: String,
    deployment_id: String,
    version_id: String,
    claim_url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReportSnapshot {
    request_id: Vec<u8>,
    publication_id: Option<Vec<u8>>,
    report: SliceReport,
}

impl Reconciler {
    /// Construct the controller.
    pub fn new(
        config: ReconcilerConfig,
        target: Target,
        reporter: Arc<dyn Reporter>,
    ) -> Result<Self, ReconcileError> {
        fs::create_dir_all(config.state_dir.join("graphs")).map_err(state)?;
        Ok(Self {
            config,
            target,
            reporter,
            account_facts: Mutex::new(None),
            graph_locks: RwLock::new(HashMap::new()),
        })
    }

    /// Accept a full desired level and begin plan/apply passes.
    pub async fn accept(
        self: &Arc<Self>,
        request: &crate::proto::ReconcileSliceRequestView<'_>,
    ) -> Result<u64, ReconcileError> {
        let desired = DesiredSlice::from_request(request)?;
        let lock = self.graph_lock(desired.graph_id).await;
        let _guard = lock.lock().await;
        let mut retained_deployments = HashMap::new();
        if let Some(mut current) = self.load(desired.graph_id)? {
            if current.retired {
                return Err(ReconcileError::Retired);
            }
            if desired.sequence < current.desired.sequence {
                return Ok(current.desired.sequence);
            }
            if desired.sequence == current.desired.sequence {
                if desired.digest() != current.desired.digest() {
                    return Err(ReconcileError::SequenceConflict);
                }
                self.schedule(desired.graph_id, desired.sequence, Duration::ZERO);
                return Ok(desired.sequence);
            }
            if desired.target_digest() == current.desired.target_digest() {
                let sequence = desired.sequence;
                current.desired = desired;
                current.pending_report = None;
                self.save(&current)?;
                self.schedule(current.desired.graph_id, sequence, Duration::ZERO);
                return Ok(sequence);
            }
            for component in &desired.components {
                let key = desired.deployment_key(component);
                if let Some(deployment) = current.deployments.remove(&key) {
                    retained_deployments.insert(key, deployment);
                }
            }
        }
        let sequence = desired.sequence;
        self.save(&Checkpoint {
            desired: desired.clone(),
            phase: Phase::Accepted,
            deployments: retained_deployments,
            pending_report: None,
            retired: false,
        })?;
        self.schedule(desired.graph_id, sequence, Duration::ZERO);
        Ok(sequence)
    }

    /// Resume report delivery and reconciliation from versioned checkpoints.
    pub fn resume(self: &Arc<Self>) -> Result<usize, ReconcileError> {
        let mut count = 0;
        for entry in fs::read_dir(self.config.state_dir.join("graphs")).map_err(state)? {
            let path = entry.map_err(state)?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let checkpoint = Self::load_path(&path)?;
            if checkpoint.retired {
                continue;
            }
            if let Some(snapshot) = checkpoint.pending_report.clone() {
                self.retry_report(checkpoint.desired.graph_id, snapshot);
            }
            self.schedule(
                checkpoint.desired.graph_id,
                checkpoint.desired.sequence,
                Duration::ZERO,
            );
            count += 1;
        }
        Ok(count)
    }

    /// Delete all Workers and terminally retire the graph.
    pub async fn retire(
        &self,
        graph_id: [u8; 16],
        generation: u64,
        sequence: u64,
    ) -> Result<u64, ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let mut checkpoint = self
            .load(graph_id)?
            .ok_or_else(|| ReconcileError::State("cannot retire unknown graph".into()))?;
        if checkpoint.desired.generation != generation || sequence < checkpoint.desired.sequence {
            return Err(ReconcileError::State(
                "retire identity does not match retained level".into(),
            ));
        }
        for component in &checkpoint.desired.components {
            self.target
                .delete(component)
                .await
                .map_err(|error| target_state(&error))?;
        }
        checkpoint.retired = true;
        checkpoint.pending_report = None;
        self.save(&checkpoint)?;
        Ok(generation)
    }

    async fn reconcile_once(
        self: Arc<Self>,
        graph_id: [u8; 16],
        expected_sequence: u64,
    ) -> Result<(), ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let Some(mut checkpoint) = self.load(graph_id)? else {
            return Ok(());
        };
        if checkpoint.retired || checkpoint.desired.sequence != expected_sequence {
            return Ok(());
        }
        match checkpoint.phase {
            Phase::Accepted => {
                self.plan_once(&mut checkpoint, graph_id, expected_sequence)
                    .await
            }
            Phase::Planned => {
                self.apply_once(&mut checkpoint, graph_id, expected_sequence)
                    .await
            }
            Phase::Ready => Ok(()),
        }
    }

    async fn plan_once(
        self: &Arc<Self>,
        checkpoint: &mut Checkpoint,
        graph_id: [u8; 16],
        expected_sequence: u64,
    ) -> Result<(), ReconcileError> {
        let facts = self.facts().await.map_err(|error| target_state(&error))?;
        checkpoint.phase = Phase::Planned;
        if facts.subdomain.is_empty() {
            self.save(checkpoint)?;
            self.schedule(graph_id, expected_sequence, Duration::ZERO);
            return Ok(());
        }
        let outputs = plan_outputs(&checkpoint.desired, &facts)?;
        let report = report_for(
            &checkpoint.desired,
            ComponentDispositionKind::Ready,
            outputs,
            vec![diagnostic(
                "cloudflare.plan.ready",
                "plan-time workers.dev URL published before deployment; the deployed core \
                 contract requires publishable slices to use the ready disposition",
            )],
            None,
        );
        self.publish(checkpoint, report, true)
    }

    async fn apply_once(
        self: &Arc<Self>,
        checkpoint: &mut Checkpoint,
        graph_id: [u8; 16],
        expected_sequence: u64,
    ) -> Result<(), ReconcileError> {
        let mut facts = self.facts().await.map_err(|error| target_state(&error))?;
        if facts.subdomain.is_empty()
            && let Some(subdomain) = checkpoint
                .deployments
                .values()
                .find_map(|deployment| workers_subdomain(&deployment.url))
        {
            facts.subdomain = subdomain;
        }
        let mut components = checkpoint.desired.components.iter().collect::<Vec<_>>();
        components.sort_by_key(|component| {
            let internal_dependencies = component
                .context
                .slots
                .iter()
                .filter(|slot| {
                    checkpoint
                        .desired
                        .components
                        .get(&slot.producer_spec_hash)
                        .is_some()
                })
                .count();
            (component.context.is_tunnel(), internal_dependencies)
        });
        for component in components {
            let deployment_key = checkpoint.desired.deployment_key(component);
            if checkpoint.deployments.contains_key(&deployment_key) {
                continue;
            }
            match self
                .target
                .deploy(&checkpoint.desired, component, &facts)
                .await
            {
                Ok(deployment) => {
                    if facts.subdomain.is_empty()
                        && let Some(subdomain) = workers_subdomain(&deployment.url)
                    {
                        facts.subdomain = subdomain;
                    }
                    checkpoint
                        .deployments
                        .insert(deployment_key, deployment.into());
                    self.save(checkpoint)?;
                }
                Err(TargetError::Config(detail)) if detail.contains("cloudflare.input.unbound") => {
                    let report = report_for(
                        &checkpoint.desired,
                        ComponentDispositionKind::Reconciling,
                        Vec::new(),
                        vec![diagnostic("cloudflare.input.unbound", &detail)],
                        None,
                    );
                    self.publish(checkpoint, report, false)?;
                    return Ok(());
                }
                Err(error) => {
                    let report = report_for(
                        &checkpoint.desired,
                        ComponentDispositionKind::Reconciling,
                        Vec::new(),
                        vec![diagnostic(
                            "cloudflare.target.unavailable",
                            &error.to_string(),
                        )],
                        None,
                    );
                    self.publish(checkpoint, report, false)?;
                    self.schedule(graph_id, expected_sequence, Duration::from_secs(2));
                    return Ok(());
                }
            }
            self.schedule(graph_id, expected_sequence, Duration::ZERO);
            return Ok(());
        }
        checkpoint.phase = Phase::Ready;
        let outputs = ready_outputs(&checkpoint.desired, &checkpoint.deployments)?;
        let evidence = PublicationEvidence::default()
            .with_revision(format!("generation:{}", checkpoint.desired.generation))
            .with_uri("cloudflare://workers/deployments");
        let report = report_for(
            &checkpoint.desired,
            ComponentDispositionKind::Ready,
            outputs,
            Vec::new(),
            Some(evidence),
        );
        self.publish(checkpoint, report, true)
    }

    async fn facts(&self) -> Result<AccountFacts, TargetError> {
        let mut facts = self.account_facts.lock().await;
        if let Some(value) = facts.clone() {
            return Ok(value);
        }
        let value = self.target.account_facts().await?;
        *facts = Some(value.clone());
        Ok(value)
    }

    fn publish(
        self: &Arc<Self>,
        checkpoint: &mut Checkpoint,
        report: SliceReport,
        stable: bool,
    ) -> Result<(), ReconcileError> {
        let publication_id = stable
            .then(|| publication_id(&checkpoint.desired, &report))
            .flatten();
        let snapshot = ReportSnapshot {
            request_id: Uuid::now_v7().as_bytes().to_vec(),
            publication_id,
            report,
        };
        checkpoint.pending_report = Some(snapshot.clone());
        self.save(checkpoint)?;
        self.retry_report(checkpoint.desired.graph_id, snapshot);
        Ok(())
    }

    fn retry_report(self: &Arc<Self>, graph_id: [u8; 16], snapshot: ReportSnapshot) {
        let reconciler = Arc::clone(self);
        tokio::spawn(async move {
            let mut delay = Duration::from_millis(250);
            loop {
                let Ok(Some(current)) = reconciler.load(graph_id) else {
                    return;
                };
                if current
                    .pending_report
                    .as_ref()
                    .map(|value| &value.request_id)
                    != Some(&snapshot.request_id)
                {
                    return;
                }
                let request = ReportSliceRequest {
                    request_id: Some(snapshot.request_id.clone()),
                    report: MessageField::some(snapshot.report.clone()),
                    publication_id: snapshot.publication_id.clone(),
                    ..Default::default()
                };
                match reconciler.reporter.report(request).await {
                    Ok(()) => {
                        let lock = reconciler.graph_lock(graph_id).await;
                        let guard = lock.lock().await;
                        let mut resume = None;
                        if let Ok(Some(mut current)) = reconciler.load(graph_id)
                            && current
                                .pending_report
                                .as_ref()
                                .map(|value| &value.request_id)
                                == Some(&snapshot.request_id)
                        {
                            current.pending_report = None;
                            if !snapshot.report.outputs.is_empty() {
                                resume = Some(current.desired.sequence);
                            }
                            let _ = reconciler.save(&current);
                        }
                        drop(guard);
                        if let Some(sequence) = resume {
                            reconciler.schedule(graph_id, sequence, Duration::ZERO);
                        }
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            graph_id = %hex::encode(graph_id),
                            sequence = snapshot.report.sequence.unwrap_or_default(),
                            error = %error,
                            "cloudflare report delivery failed"
                        );
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        });
    }

    fn schedule(self: &Arc<Self>, graph_id: [u8; 16], sequence: u64, delay: Duration) {
        let reconciler = Arc::clone(self);
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if let Err(error) = reconciler.reconcile_once(graph_id, sequence).await {
                tracing::error!(
                    graph_id = %hex::encode(graph_id),
                    sequence,
                    error = %error,
                    "cloudflare reconciliation pass failed"
                );
            }
        });
    }

    async fn graph_lock(&self, graph_id: [u8; 16]) -> Arc<Mutex<()>> {
        if let Some(lock) = self.graph_locks.read().await.get(&graph_id) {
            return Arc::clone(lock);
        }
        Arc::clone(
            self.graph_locks
                .write()
                .await
                .entry(graph_id)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    fn path(&self, graph_id: [u8; 16]) -> PathBuf {
        self.config
            .state_dir
            .join("graphs")
            .join(format!("{}.json", hex::encode(graph_id)))
    }

    fn load(&self, graph_id: [u8; 16]) -> Result<Option<Checkpoint>, ReconcileError> {
        let path = self.path(graph_id);
        match fs::read(&path) {
            Ok(_) => Self::load_path(&path).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(state(error)),
        }
    }

    fn load_path(path: &PathBuf) -> Result<Checkpoint, ReconcileError> {
        let envelope: CheckpointEnvelope =
            serde_json::from_slice(&fs::read(path).map_err(state)?).map_err(state)?;
        if envelope.version != CHECKPOINT_VERSION {
            return Err(ReconcileError::State(format!(
                "unsupported checkpoint version {}",
                envelope.version
            )));
        }
        Ok(envelope.state)
    }

    fn save(&self, checkpoint: &Checkpoint) -> Result<(), ReconcileError> {
        let path = self.path(checkpoint.desired.graph_id);
        let parent = path
            .parent()
            .ok_or_else(|| ReconcileError::State("checkpoint has no parent".into()))?;
        let mut temporary = NamedTempFile::new_in(parent).map_err(state)?;
        temporary
            .write_all(
                &serde_json::to_vec_pretty(&CheckpointEnvelope {
                    version: CHECKPOINT_VERSION,
                    state: checkpoint.clone(),
                })
                .map_err(state)?,
            )
            .map_err(state)?;
        temporary.as_file_mut().sync_all().map_err(state)?;
        temporary
            .persist(&path)
            .map_err(|error| state(error.error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(state)?;
        Ok(())
    }
}

impl From<Deployment> for DeploymentSnapshot {
    fn from(value: Deployment) -> Self {
        Self {
            url: value.url,
            deployment_id: value.deployment_id,
            version_id: value.version_id,
            claim_url: value.claim_url,
        }
    }
}

fn workers_subdomain(url: &str) -> Option<String> {
    let host = url.strip_prefix("https://")?.split('/').next()?;
    host.strip_suffix(".workers.dev")?
        .split_once('.')
        .map(|(_, subdomain)| subdomain.to_owned())
}

fn plan_outputs(
    desired: &DesiredSlice,
    facts: &AccountFacts,
) -> Result<Vec<ComponentOutputs>, ReconcileError> {
    desired
        .components
        .iter()
        .map(|component| {
            let resource_name = component
                .context
                .deployed_name()
                .map_err(|error| ReconcileError::State(error.to_string()))?;
            let value = if let Some(tunnel) = &component.context.tunnel {
                serde_json::json!({
                    "tunnelName": resource_name,
                    "privateHostname": format!("{}:{}", tunnel.origin_host, tunnel.origin_port),
                    "capability": "cloudflare-tunnel-api-pending"
                })
            } else {
                serde_json::json!({
                    "url": format!("https://{resource_name}.{}.workers.dev", facts.subdomain),
                    "workerName": resource_name
                })
            };
            output(component.spec_hash, &value)
        })
        .collect()
}

fn ready_outputs(
    desired: &DesiredSlice,
    deployments: &HashMap<String, DeploymentSnapshot>,
) -> Result<Vec<ComponentOutputs>, ReconcileError> {
    desired
        .components
        .iter()
        .map(|component| {
            let deployment = deployments
                .get(&desired.deployment_key(component))
                .ok_or_else(|| ReconcileError::State("ready deployment missing".into()))?;
            let resource_name = component
                .context
                .deployed_name()
                .map_err(|error| ReconcileError::State(error.to_string()))?;
            let mut value = if let Some(tunnel) = &component.context.tunnel {
                serde_json::json!({
                    "tunnelId": deployment.deployment_id,
                    "tunnelName": resource_name,
                    "privateHostname": format!("{}:{}", tunnel.origin_host, tunnel.origin_port),
                    "tokenRef": "connector-volume://henosis-tunnel/token",
                    "capability": deployment.version_id
                })
            } else {
                serde_json::json!({
                    "url": deployment.url,
                    "workerName": resource_name,
                    "deploymentId": deployment.deployment_id,
                    "versionId": deployment.version_id
                })
            };
            if let Some(claim_url) = &deployment.claim_url {
                value["claimUrl"] = serde_json::Value::String(claim_url.clone());
            }
            output(component.spec_hash, &value)
        })
        .collect()
}

fn output(hash: [u8; 32], value: &serde_json::Value) -> Result<ComponentOutputs, ReconcileError> {
    Ok(ComponentOutputs::default()
        .with_component_spec_hash(hash.to_vec())
        .with_values_json(serde_json::to_vec(&value).map_err(state)?))
}

fn report_for(
    desired: &DesiredSlice,
    kind: ComponentDispositionKind,
    outputs: Vec<ComponentOutputs>,
    diagnostics: Vec<Diagnostic>,
    publication: Option<PublicationEvidence>,
) -> SliceReport {
    let mut report = SliceReport {
        graph_id: Some(desired.graph_id.to_vec()),
        generation: Some(desired.generation),
        connector: Some(crate::CONNECTOR_NAME.into()),
        dispositions: desired
            .components
            .iter()
            .map(|component| {
                ComponentDisposition::default()
                    .with_component_spec_hash(component.spec_hash.to_vec())
                    .with_kind(kind)
            })
            .collect(),
        outputs,
        diagnostics,
        sequence: Some(desired.sequence),
        ..Default::default()
    };
    if let Some(publication) = publication {
        report.publication = MessageField::some(publication);
    }
    report
}

fn diagnostic(code: &str, message: &str) -> Diagnostic {
    Diagnostic::default()
        .with_code(code)
        .with_message(message)
        .with_severity(DiagnosticSeverity::Info)
}

fn publication_id(desired: &DesiredSlice, report: &SliceReport) -> Option<Vec<u8>> {
    if report.outputs.is_empty() {
        return None;
    }
    let bytes = serde_json::to_vec(&(
        desired.graph_id,
        desired.generation,
        crate::CONNECTOR_NAME,
        report
            .outputs
            .iter()
            .map(|output| (&output.component_spec_hash, &output.values_json))
            .collect::<Vec<_>>(),
    ))
    .expect("publication identity serializes");
    Some(
        Uuid::new_v5(&PUBLICATION_NAMESPACE, &bytes)
            .as_bytes()
            .to_vec(),
    )
}

fn state(error: impl std::fmt::Display) -> ReconcileError {
    ReconcileError::State(error.to_string())
}
fn target_state(error: &TargetError) -> ReconcileError {
    ReconcileError::State(error.to_string())
}
