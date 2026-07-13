//! Wrangler deployment boundary.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use base64::Engine as _;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::retry::Backoff;
use crate::slice::ComponentPin;
use crate::slice::DesiredSlice;

/// Cloudflare account and Wrangler execution configuration.
#[derive(Clone, Debug)]
pub struct TargetConfig {
    /// Wrangler executable.
    pub wrangler: PathBuf,
    /// Account identifier.
    pub account_id: Option<String>,
    /// API token used by Wrangler and account API.
    pub api_token: Option<String>,
    /// Writable Wrangler OAuth configuration copied from the host.
    pub wrangler_config: Option<PathBuf>,
    /// Explicit subdomain override, avoiding an API lookup in tests/local use.
    pub account_subdomain: Option<String>,
    /// Directory containing connector-resolvable secret refs.
    pub secret_root: PathBuf,
    /// Cloudflare REST API root, overridable for contract tests.
    pub api_base: String,
    /// Shared file consumed by the benchmark cloudflared service.
    pub tunnel_token_file: PathBuf,
}

/// Plan-time account facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountFacts {
    /// workers.dev account subdomain.
    pub subdomain: String,
}

/// One successful deployment observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Deployment {
    /// Worker URL.
    pub url: String,
    /// Deployment identifier.
    pub deployment_id: String,
    /// Version identifier.
    pub version_id: String,
    /// Temporary-account claim URL, when applicable.
    pub claim_url: Option<String>,
}

/// Provider boundary failure.
#[derive(Debug, Error)]
pub enum TargetError {
    /// Required configuration is absent.
    #[error("cloudflare configuration: {0}")]
    Config(String),
    /// Provider or Wrangler operation failed.
    #[error("cloudflare operation failed: {0}")]
    Provider(String),
    /// Filesystem staging failed.
    #[error("deployment staging failed: {0}")]
    Stage(String),
}

/// Cloudflare target adapter.
#[derive(Clone, Debug)]
pub struct Target {
    config: TargetConfig,
    client: reqwest::Client,
    account_gate: Arc<Mutex<Instant>>,
}

#[derive(Deserialize)]
struct SubdomainEnvelope {
    result: SubdomainResult,
}

#[derive(Deserialize)]
struct SubdomainResult {
    subdomain: String,
}

#[derive(Deserialize)]
struct WranglerDeployment {
    id: String,
    versions: Vec<WranglerVersion>,
}

#[derive(Deserialize)]
struct WranglerVersion {
    version_id: String,
}

#[derive(Deserialize)]
struct TunnelEnvelope<T> {
    success: bool,
    result: Option<T>,
    #[serde(default)]
    errors: Vec<TunnelApiError>,
}

#[derive(Deserialize)]
struct TunnelApiError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct TunnelResult {
    id: String,
}

#[derive(Serialize)]
struct CreateTunnelRequest<'a> {
    name: &'a str,
    tunnel_secret: &'a str,
}

impl Target {
    /// Construct an adapter.
    #[must_use]
    pub fn new(config: TargetConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            account_gate: Arc::new(Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .expect("one second before now is representable"),
            )),
        }
    }

    async fn account_slot(&self) -> tokio::sync::MutexGuard<'_, Instant> {
        let mut previous = self.account_gate.lock().await;
        let wait = Duration::from_secs(1).saturating_sub(previous.elapsed());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        *previous = Instant::now();
        previous
    }

    fn account_token(&self) -> Result<Option<String>, TargetError> {
        if let Some(token) = &self.config.api_token {
            return Ok(Some(token.clone()));
        }
        let Some(path) = &self.config.wrangler_config else {
            return Ok(None);
        };
        let source = fs::read_to_string(path).map_err(|error| {
            TargetError::Config(format!("cannot read Wrangler OAuth config: {error}"))
        })?;
        let config: toml::Value = toml::from_str(&source).map_err(|error| {
            TargetError::Config(format!("cannot parse Wrangler OAuth config: {error}"))
        })?;
        Ok(config
            .get("oauth_token")
            .and_then(toml::Value::as_str)
            .map(str::to_owned))
    }

    /// Fetch the account subdomain exactly once per process caller cache.
    pub async fn account_facts(&self) -> Result<AccountFacts, TargetError> {
        if let Some(subdomain) = &self.config.account_subdomain {
            return Ok(AccountFacts {
                subdomain: subdomain.clone(),
            });
        }
        let Some(account) = self.config.account_id.as_ref() else {
            if self.account_token()?.is_none() {
                return Ok(AccountFacts {
                    subdomain: String::new(),
                });
            }
            return Err(TargetError::Config(
                "CLOUDFLARE_ACCOUNT_ID is required when durable credentials are configured".into(),
            ));
        };
        let token = self.account_token()?.ok_or_else(|| {
            TargetError::Config(
                "Cloudflare API token or Wrangler OAuth credentials are required".into(),
            )
        })?;
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        loop {
            let slot = self.account_slot().await;
            let response = self
                .client
                .get(format!(
                    "{}/accounts/{account}/workers/subdomain",
                    self.config.api_base
                ))
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(retry_after);
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let delay = backoff.next(retry_after);
                tracing::warn!(?delay, "Cloudflare subdomain API rate limited; backing off");
                drop(slot);
                tokio::time::sleep(delay).await;
                continue;
            }
            if !status.is_success() {
                return Err(TargetError::Provider(format!(
                    "workers subdomain API returned {status}"
                )));
            }
            let envelope: SubdomainEnvelope = response
                .json()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            return Ok(AccountFacts {
                subdomain: envelope.result.subdomain,
            });
        }
    }

    /// Deploy a component with current-generation bindings without editing
    /// native files.
    pub async fn deploy(
        &self,
        desired: &DesiredSlice,
        component: &ComponentPin,
        facts: &AccountFacts,
    ) -> Result<Deployment, TargetError> {
        if component.context.is_tunnel() {
            return self.deploy_tunnel(component).await;
        }
        let directory =
            tempfile::tempdir().map_err(|error| TargetError::Stage(error.to_string()))?;
        stage(&component.context.files, directory.path())?;
        sanitize_symbolic_vars(component, directory.path())?;
        let worker_name = component
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        let bindings = resolve_bindings(desired, component, facts, &self.config.secret_root)?;
        let mut args = vec!["deploy".into(), "--name".into(), worker_name.clone()];
        for (key, value) in &bindings.plain {
            args.push("--var".into());
            args.push(format!("{key}:{value}"));
        }
        let output = self
            .run_wrangler(Some(directory.path()), &args, None)
            .await?;
        for (key, value) in &bindings.secrets {
            self.run_wrangler(
                Some(directory.path()),
                &[
                    "secret".into(),
                    "put".into(),
                    key.clone(),
                    "--name".into(),
                    worker_name.clone(),
                ],
                Some(value),
            )
            .await?;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let (deployment_id, version_id) = self.deployment_identity(&worker_name).await?;
        let claim_url = stdout
            .split_whitespace()
            .find(|word| word.starts_with("https://dash.cloudflare.com/sign-up/workers-and-pages"))
            .map(|value| {
                value
                    .trim_end_matches(|c: char| !c.is_ascii_alphanumeric() && c != '/')
                    .to_owned()
            });
        Ok(Deployment {
            url: deployed_url(&stdout, &worker_name, &facts.subdomain),
            deployment_id,
            version_id,
            claim_url,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn deploy_tunnel(&self, component: &ComponentPin) -> Result<Deployment, TargetError> {
        let account = self.config.account_id.as_ref().ok_or_else(|| {
            TargetError::Config("CLOUDFLARE_ACCOUNT_ID is required for Tunnel resources".into())
        })?;
        let token = self.account_token()?.ok_or_else(|| {
            TargetError::Config(
                "Cloudflare API token or Wrangler OAuth credentials are required for Tunnel resources"
                    .into(),
            )
        })?;
        let tunnel = component
            .context
            .tunnel
            .as_ref()
            .ok_or_else(|| TargetError::Config("Tunnel context is missing".into()))?;
        let name = component
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        let mut secret = Vec::with_capacity(32);
        secret.extend_from_slice(Uuid::now_v7().as_bytes());
        secret.extend_from_slice(Uuid::now_v7().as_bytes());
        let secret = base64::engine::general_purpose::STANDARD.encode(secret);
        let mut create_backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        let (status, envelope) = loop {
            let slot = self.account_slot().await;
            let response = self
                .client
                .post(format!(
                    "{}/accounts/{account}/cfd_tunnel",
                    self.config.api_base
                ))
                .bearer_auth(&token)
                .json(&CreateTunnelRequest {
                    name: &name,
                    tunnel_secret: &secret,
                })
                .send()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(retry_after);
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let delay = create_backoff.next(retry_after);
                tracing::warn!(?delay, "Cloudflare Tunnel create rate limited; backing off");
                drop(slot);
                tokio::time::sleep(delay).await;
                continue;
            }
            let envelope: TunnelEnvelope<TunnelResult> = response
                .json()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            break (status, envelope);
        };
        if !status.is_success() || !envelope.success {
            return Err(TargetError::Provider(format!(
                "Tunnel create API returned {status}: {}",
                tunnel_errors(&envelope.errors)
            )));
        }
        let created = envelope
            .result
            .ok_or_else(|| TargetError::Provider("Tunnel create API returned no result".into()))?;
        let mut token_backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        let (token_status, token_envelope) = loop {
            let slot = self.account_slot().await;
            let response = self
                .client
                .get(format!(
                    "{}/accounts/{account}/cfd_tunnel/{}/token",
                    self.config.api_base, created.id
                ))
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(retry_after);
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let delay = token_backoff.next(retry_after);
                tracing::warn!(
                    ?delay,
                    "Cloudflare Tunnel token API rate limited; backing off"
                );
                drop(slot);
                tokio::time::sleep(delay).await;
                continue;
            }
            let envelope: TunnelEnvelope<String> = response
                .json()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            break (status, envelope);
        };
        if !token_status.is_success() || !token_envelope.success {
            return Err(TargetError::Provider(format!(
                "Tunnel token API returned {token_status}: {}",
                tunnel_errors(&token_envelope.errors)
            )));
        }
        let tunnel_token = token_envelope
            .result
            .ok_or_else(|| TargetError::Provider("Tunnel token API returned no result".into()))?;
        if let Some(parent) = self.config.tunnel_token_file.parent() {
            fs::create_dir_all(parent).map_err(|error| TargetError::Stage(error.to_string()))?;
        }
        fs::write(&self.config.tunnel_token_file, tunnel_token)
            .map_err(|error| TargetError::Stage(error.to_string()))?;
        Ok(Deployment {
            url: format!("http://{}:{}", tunnel.origin_host, tunnel.origin_port),
            deployment_id: created.id,
            version_id: "workers-vpc-tunnel-v1".into(),
            claim_url: None,
        })
    }

    async fn run_wrangler(
        &self,
        current_dir: Option<&Path>,
        args: &[String],
        stdin: Option<&str>,
    ) -> Result<std::process::Output, TargetError> {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        loop {
            let slot = self.account_slot().await;
            let mut command = Command::new(&self.config.wrangler);
            command.args(args);
            if let Some(directory) = current_dir {
                command.current_dir(directory);
            }
            if let Some(account) = &self.config.account_id {
                command.env("CLOUDFLARE_ACCOUNT_ID", account);
            }
            if let Some(token) = &self.config.api_token {
                command.env("CLOUDFLARE_API_TOKEN", token);
            }
            if stdin.is_some() {
                command.stdin(Stdio::piped());
            }
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
            let mut child = command
                .spawn()
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            if let Some(value) = stdin {
                child
                    .stdin
                    .take()
                    .ok_or_else(|| TargetError::Provider("wrangler stdin unavailable".into()))?
                    .write_all(value.as_bytes())
                    .await
                    .map_err(|error| TargetError::Provider(error.to_string()))?;
            }
            let output = child
                .wait_with_output()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            if output.status.success() {
                return Ok(output);
            }
            let detail = String::from_utf8_lossy(&output.stderr).into_owned();
            if !wrangler_rate_limited(&detail) {
                return Err(TargetError::Provider(detail));
            }
            let delay = backoff.next(None);
            tracing::warn!(?delay, "Wrangler rate limited; backing off before retry");
            drop(slot);
            tokio::time::sleep(delay).await;
        }
    }

    async fn deployment_identity(
        &self,
        worker_name: &str,
    ) -> Result<(String, String), TargetError> {
        let output = self
            .run_wrangler(
                None,
                &[
                    "deployments".into(),
                    "list".into(),
                    "--name".into(),
                    worker_name.into(),
                    "--json".into(),
                ],
                None,
            )
            .await?;
        let deployments: Vec<WranglerDeployment> =
            serde_json::from_slice(&output.stdout).map_err(|error| {
                TargetError::Provider(format!("invalid Wrangler deployment JSON: {error}"))
            })?;
        let deployment = deployments.first().ok_or_else(|| {
            TargetError::Provider("Wrangler returned no deployment after deploy".into())
        })?;
        let version = deployment
            .versions
            .first()
            .ok_or_else(|| TargetError::Provider("Wrangler deployment has no version".into()))?;
        Ok((deployment.id.clone(), version.version_id.clone()))
    }

    /// Delete a deployed Worker.
    pub async fn delete(&self, component: &ComponentPin) -> Result<(), TargetError> {
        if component.context.is_tunnel() {
            return Err(TargetError::Config(
                "Tunnel retirement requires the retained provider tunnel id".into(),
            ));
        }
        let worker_name = component
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        self.run_wrangler(
            None,
            &[
                "delete".into(),
                "--name".into(),
                worker_name,
                "--force".into(),
            ],
            None,
        )
        .await
        .map(|_| ())
    }
}

fn retry_after(value: &reqwest::header::HeaderValue) -> Option<Duration> {
    value
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

fn wrangler_rate_limited(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("10429")
        || detail.contains("too many requests")
        || detail.contains("rate limit")
        || detail.contains("status code: 429")
}

fn deployed_url(stdout: &str, worker_name: &str, subdomain: &str) -> String {
    let observed = stdout
        .split_whitespace()
        .find(|word| word.starts_with("https://") && word.contains(".workers.dev"));
    let Some(value) = observed else {
        return format!("https://{worker_name}.{subdomain}.workers.dev");
    };
    value
        .trim_end_matches(|character: char| {
            !character.is_ascii_alphanumeric() && !matches!(character, '/' | '.' | '-')
        })
        .to_owned()
}

#[derive(Debug)]
struct Bindings {
    plain: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

fn resolve_bindings(
    desired: &DesiredSlice,
    component: &ComponentPin,
    facts: &AccountFacts,
    secret_root: &Path,
) -> Result<Bindings, TargetError> {
    let mut plain = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    for slot in &component.context.slots {
        let text = resolve_slot(desired, slot, facts)?;
        if slot.output.ends_with("Ref") {
            secrets.insert(slot.key.clone(), resolve_secret_ref(&text, secret_root)?);
        } else {
            plain.insert(slot.key.clone(), text);
        }
    }
    Ok(Bindings { plain, secrets })
}

fn resolve_slot(
    desired: &DesiredSlice,
    slot: &crate::context::InputSlot,
    facts: &AccountFacts,
) -> Result<String, TargetError> {
    if let Some(output) = desired.upstream_outputs.get(&slot.producer_spec_hash) {
        let values: serde_json::Value = serde_json::from_slice(&output.values_json)
            .map_err(|error| TargetError::Config(error.to_string()))?;
        if let Some(value) = values.get(&slot.output) {
            return Ok(match value.as_str() {
                Some(value) => value.to_owned(),
                None => value.to_string(),
            });
        }
    }

    if let Some(producer) = desired.components.get(&slot.producer_spec_hash) {
        let worker_name = producer
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        return match slot.output.as_str() {
            "url" => Ok(format!(
                "https://{worker_name}.{}.workers.dev",
                facts.subdomain
            )),
            "workerName" => Ok(worker_name),
            _ => Err(unbound(slot)),
        };
    }

    Err(unbound(slot))
}

fn unbound(slot: &crate::context::InputSlot) -> TargetError {
    TargetError::Config(format!(
        "cloudflare.input.unbound: {} requires {}.{}",
        slot.key, slot.producer, slot.output
    ))
}

fn tunnel_errors(errors: &[TunnelApiError]) -> String {
    if errors.is_empty() {
        return "no structured error detail".into();
    }
    errors
        .iter()
        .map(|error| format!("{}: {}", error.code, error.message))
        .collect::<Vec<_>>()
        .join(", ")
}

fn resolve_secret_ref(reference: &str, secret_root: &Path) -> Result<String, TargetError> {
    let name = reference.strip_prefix("docker-secret://").ok_or_else(|| {
        TargetError::Config(format!(
            "unsupported secret reference {reference:?}; plaintext values never cross core"
        ))
    })?;
    fs::read_to_string(secret_root.join(name))
        .map(|value| value.trim_end().to_owned())
        .map_err(|error| {
            TargetError::Config(format!("cannot resolve secret ref {reference:?}: {error}"))
        })
}

fn sanitize_symbolic_vars(component: &ComponentPin, root: &Path) -> Result<(), TargetError> {
    if component.context.slots.is_empty() {
        return Ok(());
    }
    let path = root.join("wrangler.toml");
    let source =
        fs::read_to_string(&path).map_err(|error| TargetError::Stage(error.to_string()))?;
    let mut config: toml::Value =
        toml::from_str(&source).map_err(|error| TargetError::Stage(error.to_string()))?;
    if let Some(vars) = config.get_mut("vars").and_then(toml::Value::as_table_mut) {
        for slot in &component.context.slots {
            vars.remove(&slot.key);
        }
    }
    let source =
        toml::to_string_pretty(&config).map_err(|error| TargetError::Stage(error.to_string()))?;
    fs::write(path, source).map_err(|error| TargetError::Stage(error.to_string()))
}

fn stage(files: &[crate::context::ProjectFile], root: &Path) -> Result<(), TargetError> {
    for file in files {
        let path = root.join(&file.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| TargetError::Stage(error.to_string()))?;
        }
        fs::write(path, &file.bytes).map_err(|error| TargetError::Stage(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ComponentContext;
    use crate::context::InputSlot;
    use crate::slice::ComponentPin;
    use crate::slice::UpstreamOutput;
    use iddqd::IdOrdMap;

    #[test]
    fn classifies_wrangler_rate_limits() {
        assert!(wrangler_rate_limited(
            "A request to the Cloudflare API failed (10429)"
        ));
        assert!(wrangler_rate_limited(
            "HTTP status code: 429 Too Many Requests"
        ));
        assert!(!wrangler_rate_limited("Authentication error [code: 10000]"));
    }

    #[test]
    fn parses_retry_after_seconds() {
        let value = reqwest::header::HeaderValue::from_static("37");
        assert_eq!(retry_after(&value), Some(Duration::from_secs(37)));
    }

    #[test]
    fn missing_current_generation_value_is_unbound() {
        let component = ComponentPin {
            spec_hash: [1; 32],
            name: "api".into(),
            context: ComponentContext {
                api_version: crate::context::API_VERSION.into(),
                worker_name: "api".into(),
                entry: "src/index.js".into(),
                assets_directory: None,
                environment: "dev".into(),
                files: vec![],
                slots: vec![InputSlot {
                    key: "URL".into(),
                    producer: "db".into(),
                    output: "restUrl".into(),
                    producer_spec_hash: [2; 32],
                }],
                tunnel: None,
            },
        };
        let desired = DesiredSlice {
            graph_id: [0; 16],
            generation: 1,
            sequence: 1,
            components: IdOrdMap::new(),
            upstream_outputs: IdOrdMap::<UpstreamOutput>::new(),
        };
        let error = resolve_bindings(
            &desired,
            &component,
            &AccountFacts {
                subdomain: "example".into(),
            },
            Path::new("/run/secrets"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("cloudflare.input.unbound"));
        assert!(error.contains("db.restUrl"));
    }

    #[test]
    fn resolves_plan_time_url_between_workers_in_one_slice() {
        let producer = ComponentPin {
            spec_hash: [2; 32],
            name: "api".into(),
            context: ComponentContext {
                api_version: crate::context::API_VERSION.into(),
                worker_name: "api".into(),
                entry: "src/index.js".into(),
                assets_directory: None,
                environment: "dev".into(),
                files: vec![],
                slots: vec![],
                tunnel: None,
            },
        };
        let consumer = ComponentPin {
            spec_hash: [3; 32],
            name: "web".into(),
            context: ComponentContext {
                api_version: crate::context::API_VERSION.into(),
                worker_name: "web".into(),
                entry: "src/index.js".into(),
                assets_directory: None,
                environment: "dev".into(),
                files: vec![],
                slots: vec![InputSlot {
                    key: "BACKEND_URL".into(),
                    producer: "api".into(),
                    output: "url".into(),
                    producer_spec_hash: [2; 32],
                }],
                tunnel: None,
            },
        };
        let mut components = IdOrdMap::new();
        components.insert_unique(producer).unwrap();
        components.insert_unique(consumer.clone()).unwrap();
        let desired = DesiredSlice {
            graph_id: [0; 16],
            generation: 1,
            sequence: 1,
            components,
            upstream_outputs: IdOrdMap::<UpstreamOutput>::new(),
        };
        let bindings = resolve_bindings(
            &desired,
            &consumer,
            &AccountFacts {
                subdomain: "example".into(),
            },
            Path::new("/run/secrets"),
        )
        .unwrap();
        assert_eq!(
            bindings.plain.get("BACKEND_URL").map(String::as_str),
            Some("https://api-dev.example.workers.dev")
        );
    }
}
