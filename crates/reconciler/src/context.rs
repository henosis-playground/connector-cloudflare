//! Connector-owned deployment context derived from native Wrangler files.

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Version of the opaque connector context.
pub const API_VERSION: &str = "henosis.dev/cloudflare-worker/v1";

/// Complete immutable Worker source material and binding declarations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComponentContext {
    /// Context schema version.
    pub api_version: String,
    /// Native Wrangler name before environment mapping.
    pub worker_name: String,
    /// Relative entry module path.
    pub entry: String,
    /// Optional static-assets directory.
    pub assets_directory: Option<String>,
    /// Environment mapping selected by authoring.
    pub environment: String,
    /// Native project files required by Wrangler.
    pub files: Vec<ProjectFile>,
    /// Runtime variable slots.
    pub slots: Vec<InputSlot>,
}

/// One file copied from the native Worker project into the deployment boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectFile {
    /// Slash-separated path relative to the project root.
    pub path: String,
    /// Exact bytes.
    pub bytes: Vec<u8>,
}

/// One `[vars]` value supplied from an upstream component output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputSlot {
    /// Wrangler variable name.
    pub key: String,
    /// Human-facing upstream component name.
    pub producer: String,
    /// Upstream output property.
    pub output: String,
    /// Immutable producer spec hash resolved by authoring.
    pub producer_spec_hash: [u8; 32],
}

/// Context parse or semantic failure.
#[derive(Debug, Error)]
pub enum ContextError {
    /// JSON encoding is invalid.
    #[error("connector context is invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A semantic invariant is violated.
    #[error("{0}")]
    Invalid(String),
}

impl ComponentContext {
    /// Parse and validate an opaque context received from core.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ContextError> {
        let value: Self = serde_json::from_slice(bytes)?;
        if value.api_version != API_VERSION {
            return Err(ContextError::Invalid(format!(
                "apiVersion must be {API_VERSION:?}"
            )));
        }
        validate_dns_label(&value.worker_name)?;
        if value.entry.starts_with('/') || value.entry.contains("..") {
            return Err(ContextError::Invalid(
                "entry must be a safe relative path".into(),
            ));
        }
        if value.files.is_empty() {
            return Err(ContextError::Invalid("files must not be empty".into()));
        }
        for file in &value.files {
            if file.path.starts_with('/') || file.path.contains("..") {
                return Err(ContextError::Invalid(format!(
                    "project file path {:?} is unsafe",
                    file.path
                )));
            }
        }
        Ok(value)
    }

    /// Stable deployed Worker name for a graph environment.
    pub fn deployed_name(&self) -> Result<String, ContextError> {
        worker_name(&self.worker_name, &self.environment)
    }
}

/// Map native name and graph environment to a valid stable Worker name.
pub fn worker_name(base: &str, environment: &str) -> Result<String, ContextError> {
    let value = match environment {
        "dev" | "prod" => format!("{base}-{environment}"),
        preview if preview.starts_with("preview_") => {
            let suffix = preview.trim_start_matches("preview_");
            let suffix = suffix.get(..suffix.len().min(12)).unwrap_or(suffix);
            format!("{base}-preview-{suffix}").to_ascii_lowercase()
        }
        _ => {
            return Err(ContextError::Invalid(format!(
                "unsupported environment {environment:?}"
            )));
        }
    };
    let value = if value.len() <= 63 {
        value
    } else {
        let digest = hex::encode(&blake3::hash(value.as_bytes()).as_bytes()[..4]);
        let keep = 63 - digest.len() - 1;
        format!("{}-{digest}", value.get(..keep).unwrap_or(&value))
    };
    validate_dns_label(&value)?;
    Ok(value)
}

fn validate_dns_label(value: &str) -> Result<(), ContextError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(ContextError::Invalid(format!(
            "Worker name {value:?} must be a lowercase DNS label no longer than 63 characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_names_are_bounded_and_deterministic() {
        let name = worker_name("service-f", "preview_01jzzzzzzzzzzzzzzzzzzzzzzz").unwrap();
        assert_eq!(name, "service-f-preview-01jzzzzzzzzz");
        assert!(name.len() <= 63);
    }
}
