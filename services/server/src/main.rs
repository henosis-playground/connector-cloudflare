//! Cloudflare connector target configuration and SDK bootstrap.

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use connector_sdk::RuntimeConfig;
use connector_sdk::S2PlanStore;
use connector_sdk::S2PlanStoreConfig;
use connector_sdk::ServeConfig;
use henosis_cloudflare_reconciler::CloudflareConnector;
use henosis_cloudflare_reconciler::target::Target;
use henosis_cloudflare_reconciler::target::TargetConfig;
use http::Uri;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    prepare_wrangler_config()?;
    let target = Target::new(TargetConfig {
        wrangler: PathBuf::from(string_env("HENOSIS_WRANGLER", "wrangler")),
        account_id: env::var("CLOUDFLARE_ACCOUNT_ID")
            .ok()
            .filter(|value| !value.is_empty()),
        api_token: env::var("CLOUDFLARE_API_TOKEN")
            .ok()
            .filter(|value| !value.is_empty()),
        wrangler_config: Some(wrangler_config_path()),
        account_subdomain: env::var("CLOUDFLARE_ACCOUNT_SUBDOMAIN")
            .ok()
            .filter(|value| !value.is_empty()),
        secret_root: PathBuf::from(string_env("HENOSIS_SECRET_ROOT", "/run/secrets")),
        api_base: string_env(
            "CLOUDFLARE_API_BASE",
            "https://api.cloudflare.com/client/v4",
        ),
        tunnel_token_file: PathBuf::from(string_env(
            "HENOSIS_TUNNEL_TOKEN_FILE",
            "/var/lib/henosis-tunnel/token",
        )),
    });
    let plan_store = S2PlanStore::connect(&S2PlanStoreConfig {
        access_token: required("S2_ACCESS_TOKEN")?,
        account_endpoint: required("S2_ACCOUNT_ENDPOINT")?,
        basin_endpoint: required("S2_BASIN_ENDPOINT")?,
        basin: required("S2_BASIN")?,
        stream_prefix: string_env("HENOSIS_PLAN_STREAM_PREFIX", "henosis-plans-v1"),
    })?;
    connector_sdk::serve(
        ServeConfig {
            bind: string_env("HENOSIS_BIND", "0.0.0.0:8083"),
            core_uri: string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?,
            core_token: env::var("HENOSIS_CORE_TOKEN")
                .ok()
                .filter(|value| !value.is_empty()),
            runtime: RuntimeConfig::new(
                PathBuf::from(string_env(
                    "HENOSIS_STATE_DIR",
                    "/var/lib/henosis-connector-cloudflare/state",
                ))
                .join("sdk-v1"),
                plan_store,
            ),
            telemetry_filter: "henosis=info,connector_sdk=info".into(),
        },
        CloudflareConnector::new(target),
    )
    .await?;
    Ok(())
}

fn prepare_wrangler_config() -> Result<(), std::io::Error> {
    let Ok(source) = env::var("HENOSIS_WRANGLER_CONFIG_SOURCE") else {
        return Ok(());
    };
    let destination = wrangler_config_path();
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(Path::new(&source), destination)?;
    Ok(())
}

fn wrangler_config_path() -> PathBuf {
    PathBuf::from(string_env(
        "XDG_CONFIG_HOME",
        "/var/lib/henosis-connector-cloudflare/xdg",
    ))
    .join(".wrangler/config/default.toml")
}

fn string_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    env::var(name).map_err(|_| format!("required environment variable {name} is missing").into())
}
