//! Wrangler deployment boundary.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;

use serde::Deserialize;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

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
    /// Explicit subdomain override, avoiding an API lookup in tests/local use.
    pub account_subdomain: Option<String>,
    /// Directory containing connector-resolvable secret refs.
    pub secret_root: PathBuf,
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
}

#[derive(Deserialize)]
struct SubdomainEnvelope {
    result: SubdomainResult,
}

#[derive(Deserialize)]
struct SubdomainResult {
    subdomain: String,
}

impl Target {
    /// Construct an adapter.
    #[must_use]
    pub fn new(config: TargetConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Fetch the account subdomain exactly once per process caller cache.
    pub async fn account_facts(&self) -> Result<AccountFacts, TargetError> {
        if let Some(subdomain) = &self.config.account_subdomain {
            return Ok(AccountFacts {
                subdomain: subdomain.clone(),
            });
        }
        let account = self.config.account_id.as_ref().ok_or_else(|| {
            TargetError::Config(
                "CLOUDFLARE_ACCOUNT_ID is required for plan-time workers.dev URLs".into(),
            )
        })?;
        let token = self.config.api_token.as_ref().ok_or_else(|| {
            TargetError::Config(
                "CLOUDFLARE_API_TOKEN is required for plan-time workers.dev URLs".into(),
            )
        })?;
        let response = self
            .client
            .get(format!(
                "https://api.cloudflare.com/client/v4/accounts/{account}/workers/subdomain"
            ))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| TargetError::Provider(error.to_string()))?;
        if !response.status().is_success() {
            return Err(TargetError::Provider(format!(
                "workers subdomain API returned {}",
                response.status()
            )));
        }
        let envelope: SubdomainEnvelope = response
            .json()
            .await
            .map_err(|error| TargetError::Provider(error.to_string()))?;
        Ok(AccountFacts {
            subdomain: envelope.result.subdomain,
        })
    }

    /// Deploy a component with current-generation bindings without editing
    /// native files.
    pub async fn deploy(
        &self,
        desired: &DesiredSlice,
        component: &ComponentPin,
        facts: &AccountFacts,
    ) -> Result<Deployment, TargetError> {
        let directory =
            tempfile::tempdir().map_err(|error| TargetError::Stage(error.to_string()))?;
        stage(&component.context.files, directory.path())?;
        let worker_name = component
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        let bindings = resolve_bindings(desired, component, &self.config.secret_root)?;
        let mut command = Command::new(&self.config.wrangler);
        command
            .current_dir(directory.path())
            .arg("deploy")
            .arg("--name")
            .arg(&worker_name);
        if let Some(account) = &self.config.account_id {
            command.env("CLOUDFLARE_ACCOUNT_ID", account);
        }
        if let Some(token) = &self.config.api_token {
            command.env("CLOUDFLARE_API_TOKEN", token);
        }
        for (key, value) in &bindings.plain {
            command.arg("--var").arg(format!("{key}:{value}"));
        }
        let output = command
            .output()
            .await
            .map_err(|error| TargetError::Provider(error.to_string()))?;
        if !output.status.success() {
            return Err(TargetError::Provider(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        for (key, value) in &bindings.secrets {
            let mut secret = Command::new(&self.config.wrangler);
            secret
                .current_dir(directory.path())
                .arg("secret")
                .arg("put")
                .arg(key)
                .arg("--name")
                .arg(&worker_name)
                .stdin(Stdio::piped());
            if let Some(account) = &self.config.account_id {
                secret.env("CLOUDFLARE_ACCOUNT_ID", account);
            }
            if let Some(token) = &self.config.api_token {
                secret.env("CLOUDFLARE_API_TOKEN", token);
            }
            let mut child = secret
                .spawn()
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            child
                .stdin
                .take()
                .ok_or_else(|| TargetError::Provider("wrangler secret stdin unavailable".into()))?
                .write_all(value.as_bytes())
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            let status = child
                .wait()
                .await
                .map_err(|error| TargetError::Provider(error.to_string()))?;
            if !status.success() {
                return Err(TargetError::Provider(format!(
                    "wrangler secret put failed for {key}"
                )));
            }
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let version_id =
            extract_after(&stdout, "Version ID:").unwrap_or_else(|| blake3_id(stdout.as_bytes()));
        let deployment_id = version_id.clone();
        let claim_url = stdout
            .split_whitespace()
            .find(|word| word.starts_with("https://dash.cloudflare.com/sign-up/workers-and-pages"))
            .map(|value| {
                value
                    .trim_end_matches(|c: char| !c.is_ascii_alphanumeric() && c != '/')
                    .to_owned()
            });
        Ok(Deployment {
            url: format!("https://{worker_name}.{}.workers.dev", facts.subdomain),
            deployment_id,
            version_id,
            claim_url,
        })
    }

    /// Delete a deployed Worker.
    pub async fn delete(&self, component: &ComponentPin) -> Result<(), TargetError> {
        let worker_name = component
            .context
            .deployed_name()
            .map_err(|error| TargetError::Config(error.to_string()))?;
        let mut command = Command::new(&self.config.wrangler);
        command
            .arg("delete")
            .arg("--name")
            .arg(worker_name)
            .arg("--force");
        if let Some(account) = &self.config.account_id {
            command.env("CLOUDFLARE_ACCOUNT_ID", account);
        }
        if let Some(token) = &self.config.api_token {
            command.env("CLOUDFLARE_API_TOKEN", token);
        }
        let output = command
            .output()
            .await
            .map_err(|error| TargetError::Provider(error.to_string()))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(TargetError::Provider(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }
}

#[derive(Debug)]
struct Bindings {
    plain: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

fn resolve_bindings(
    desired: &DesiredSlice,
    component: &ComponentPin,
    secret_root: &Path,
) -> Result<Bindings, TargetError> {
    let mut plain = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    for slot in &component.context.slots {
        let output = desired
            .upstream_outputs
            .get(&slot.producer_spec_hash)
            .ok_or_else(|| {
                TargetError::Config(format!(
                    "cloudflare.input.unbound: {} requires {}.{}",
                    slot.key, slot.producer, slot.output
                ))
            })?;
        let values: serde_json::Value = serde_json::from_slice(&output.values_json)
            .map_err(|error| TargetError::Config(error.to_string()))?;
        let value = values.get(&slot.output).ok_or_else(|| {
            TargetError::Config(format!(
                "cloudflare.input.unbound: {} requires {}.{}",
                slot.key, slot.producer, slot.output
            ))
        })?;
        let text = match value.as_str() {
            Some(value) => value.to_owned(),
            None => value.to_string(),
        };
        if slot.output.ends_with("Ref") {
            secrets.insert(slot.key.clone(), resolve_secret_ref(&text, secret_root)?);
        } else {
            plain.insert(slot.key.clone(), text);
        }
    }
    Ok(Bindings { plain, secrets })
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

fn extract_after(output: &str, marker: &str) -> Option<String> {
    output
        .lines()
        .find_map(|line| {
            line.split_once(marker)
                .map(|(_, value)| value.trim().to_owned())
        })
        .filter(|value| !value.is_empty())
}

fn blake3_id(bytes: &[u8]) -> String {
    hex::encode(&blake3::hash(bytes).as_bytes()[..16])
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
            },
        };
        let desired = DesiredSlice {
            graph_id: [0; 16],
            generation: 1,
            sequence: 1,
            components: IdOrdMap::new(),
            upstream_outputs: IdOrdMap::<UpstreamOutput>::new(),
        };
        let error = resolve_bindings(&desired, &component, Path::new("/run/secrets"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("cloudflare.input.unbound"));
        assert!(error.contains("db.restUrl"));
    }
}
