//! Pure native-Wrangler component derivation; no marker files are consulted.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use henosis_cloudflare_reconciler::CONNECTOR_NAME;
use henosis_cloudflare_reconciler::context::API_VERSION;
use henosis_cloudflare_reconciler::context::ComponentContext;
use henosis_cloudflare_reconciler::context::InputSlot;
use henosis_cloudflare_reconciler::context::ProjectFile;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use walkdir::WalkDir;

/// Complete material used to register a core component spec.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedComponent {
    /// Native Worker name.
    pub name: String,
    /// Connector registry key.
    pub connector: String,
    /// Henosis output-shape schema.
    pub outputs_schema: serde_json::Value,
    /// Upstream immutable spec hashes.
    pub depends_on: Vec<Vec<u8>>,
    /// Connector-owned deployment context.
    pub connector_context: ComponentContext,
}

impl DerivedComponent {
    /// Encode opaque connector context.
    pub fn connector_context_bytes(&self) -> Result<Vec<u8>, DeriveError> {
        serde_json::to_vec(&self.connector_context).map_err(DeriveError::Serialize)
    }

    /// Encode output contract.
    pub fn outputs_schema_bytes(&self) -> Result<Vec<u8>, DeriveError> {
        serde_json::to_vec(&self.outputs_schema).map_err(DeriveError::Serialize)
    }
}

/// Native Wrangler derivation failure.
#[derive(Debug, Error)]
pub enum DeriveError {
    /// Filesystem access failed.
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Wrangler TOML is malformed.
    #[error("invalid TOML in {path}: {source}")]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    /// Native project cannot map to a component.
    #[error("invalid Wrangler project at {path}: {message}")]
    Invalid { path: PathBuf, message: String },
    /// Context encoding failed.
    #[error("cannot serialize connector context: {0}")]
    Serialize(serde_json::Error),
}

#[derive(Debug, Deserialize)]
struct Wrangler {
    name: String,
    main: Option<String>,
    assets: Option<Assets>,
    #[serde(default)]
    vars: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
struct Assets {
    directory: String,
}

/// Derive from `wrangler.toml` and native project files only.
pub fn derive_component(
    root: impl AsRef<Path>,
    environment: &str,
    dependencies: &BTreeMap<String, [u8; 32]>,
) -> Result<DerivedComponent, DeriveError> {
    let root = root.as_ref();
    let config_path = root.join("wrangler.toml");
    let source = fs::read_to_string(&config_path).map_err(|source| DeriveError::Read {
        path: config_path.clone(),
        source,
    })?;
    let config: Wrangler = toml::from_str(&source).map_err(|source| DeriveError::Toml {
        path: config_path.clone(),
        source,
    })?;
    if config.name.is_empty() {
        return invalid(&config_path, "name must not be empty");
    }
    let entry = config.main.clone().unwrap_or_else(|| "src/index.js".into());
    let entry_path = root.join(&entry);
    if !entry_path.is_file() {
        return invalid(&config_path, format!("main entry {entry:?} does not exist"));
    }

    let mut slots = Vec::new();
    for (key, value) in &config.vars {
        let Some(value) = value.as_str() else {
            continue;
        };
        if let Some((producer, output)) = parse_slot(value) {
            let producer_spec_hash =
                dependencies
                    .get(producer)
                    .copied()
                    .ok_or_else(|| DeriveError::Invalid {
                        path: config_path.clone(),
                        message: format!(
                            "[vars].{key} names producer {producer:?}, but authoring did not \
                             receive its immutable spec hash"
                        ),
                    })?;
            slots.push(InputSlot {
                key: key.clone(),
                producer: producer.into(),
                output: output.into(),
                producer_spec_hash,
            });
        }
    }
    slots.sort_by(|left, right| left.key.cmp(&right.key));

    let mut paths = vec![config_path.clone(), entry_path];
    if let Some(assets) = &config.assets {
        let directory = root.join(&assets.directory);
        if !directory.is_dir() {
            return invalid(
                &config_path,
                format!("assets directory {:?} does not exist", assets.directory),
            );
        }
        for entry in WalkDir::new(directory).sort_by_file_name() {
            let entry = entry.map_err(|source| DeriveError::Invalid {
                path: config_path.clone(),
                message: source.to_string(),
            })?;
            if entry.file_type().is_file() {
                paths.push(entry.path().to_path_buf());
            }
        }
    }
    paths.sort();
    paths.dedup();
    let files = paths
        .into_iter()
        .map(|path| project_file(root, &path))
        .collect::<Result<Vec<_>, _>>()?;
    let context = ComponentContext {
        api_version: API_VERSION.into(),
        worker_name: config.name.clone(),
        entry,
        assets_directory: config.assets.map(|assets| assets.directory),
        environment: environment.into(),
        files,
        slots,
    };
    ComponentContext::from_bytes(&serde_json::to_vec(&context).map_err(DeriveError::Serialize)?)
        .map_err(|source| DeriveError::Invalid {
            path: config_path,
            message: source.to_string(),
        })?;

    Ok(DerivedComponent {
        name: config.name,
        connector: CONNECTOR_NAME.into(),
        outputs_schema: serde_json::json!({
            "kind": "object",
            "shape": {
                "url": {"kind": "url", "role": "ui"},
                "workerName": {"kind": "string"},
                "deploymentId": {"kind": "string"},
                "versionId": {"kind": "string"},
                "claimUrl": {"kind": "url"}
            }
        }),
        depends_on: dependencies.values().map(|hash| hash.to_vec()).collect(),
        connector_context: context,
    })
}

fn parse_slot(value: &str) -> Option<(&str, &str)> {
    let inner = value.strip_prefix("${henosis:")?.strip_suffix('}')?;
    let (producer, output) = inner.rsplit_once('.')?;
    (!producer.is_empty() && !output.is_empty()).then_some((producer, output))
}

fn project_file(root: &Path, path: &Path) -> Result<ProjectFile, DeriveError> {
    let relative = path.strip_prefix(root).map_err(|_| DeriveError::Invalid {
        path: path.into(),
        message: "file escapes project root".into(),
    })?;
    let bytes = fs::read(path).map_err(|source| DeriveError::Read {
        path: path.into(),
        source,
    })?;
    Ok(ProjectFile {
        path: relative.to_string_lossy().replace('\\', "/"),
        bytes,
    })
}

fn invalid<T>(path: &Path, message: impl Into<String>) -> Result<T, DeriveError> {
    Err(DeriveError::Invalid {
        path: path.into(),
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_worker_and_slots_without_marker_file() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("src")).unwrap();
        fs::write(
            root.path().join("src/index.js"),
            "export default { fetch() {} }",
        )
        .unwrap();
        fs::write(
            root.path().join("wrangler.toml"),
            "name = \"service-e\"\nmain = \"src/index.js\"\n[vars]\nSUPABASE_URL = \
             \"${henosis:database.restUrl}\"\n",
        )
        .unwrap();
        let hash = [7; 32];
        let derived = derive_component(
            root.path(),
            "dev",
            &BTreeMap::from([("database".into(), hash)]),
        )
        .unwrap();
        assert_eq!(derived.name, "service-e");
        assert_eq!(derived.depends_on, vec![hash.to_vec()]);
        assert_eq!(derived.connector_context.slots[0].output, "restUrl");
        assert!(!root.path().join("henosis.toml").exists());
    }
}
