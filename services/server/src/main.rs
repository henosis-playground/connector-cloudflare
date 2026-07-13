//! Cloudflare connector service process.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::get;
use connectrpc::Router;
use henosis_cloudflare_reconciler::ConnectorHandler;
use henosis_cloudflare_reconciler::reconciler::CoreReporter;
use henosis_cloudflare_reconciler::reconciler::Reconciler;
use henosis_cloudflare_reconciler::reconciler::ReconcilerConfig;
use henosis_cloudflare_reconciler::target::Target;
use henosis_cloudflare_reconciler::target::TargetConfig;
use http::Uri;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("henosis=info")),
        )
        .try_init()?;
    let reporter = Arc::new(CoreReporter::new(
        string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?,
        env::var("HENOSIS_CORE_TOKEN")
            .ok()
            .filter(|value| !value.is_empty()),
    ));
    let target = Target::new(TargetConfig {
        wrangler: PathBuf::from(string_env("HENOSIS_WRANGLER", "wrangler")),
        account_id: env::var("CLOUDFLARE_ACCOUNT_ID")
            .ok()
            .filter(|value| !value.is_empty()),
        api_token: env::var("CLOUDFLARE_API_TOKEN")
            .ok()
            .filter(|value| !value.is_empty()),
        account_subdomain: env::var("CLOUDFLARE_ACCOUNT_SUBDOMAIN")
            .ok()
            .filter(|value| !value.is_empty()),
        secret_root: PathBuf::from(string_env("HENOSIS_SECRET_ROOT", "/run/secrets")),
    });
    let reconciler = Arc::new(Reconciler::new(
        ReconcilerConfig {
            state_dir: PathBuf::from(string_env(
                "HENOSIS_STATE_DIR",
                "/var/lib/henosis-connector-cloudflare/state",
            )),
        },
        target,
        reporter,
    )?);
    reconciler.resume()?;
    let connect = Router::new().add_service(Arc::new(ConnectorHandler::new(reconciler)));
    let router = axum::Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .fallback_service(connect.into_axum_service());
    let listener =
        tokio::net::TcpListener::bind(string_env("HENOSIS_BIND", "0.0.0.0:8083")).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn string_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
