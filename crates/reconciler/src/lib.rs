//! Cloudflare Workers connector with explicit S2-backed plans.

pub mod context;
pub mod reconciler;
mod retry;
pub mod slice;
pub mod target;

/// Connector registry key.
pub const CONNECTOR_NAME: &str = "cloudflare";

pub use reconciler::CloudflareConnector;
