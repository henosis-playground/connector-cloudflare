//! Print a core-registration-ready component derived from a native Wrangler
//! project.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use henosis_cloudflare_authoring::derive_component;
use henosis_cloudflare_authoring::derive_tunnel;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let first = arguments.next().ok_or(
        "usage: derive-component <root> <environment> [producer=64hex ...] | --tunnel <name> \
         <environment> <origin-host> <origin-port>",
    )?;
    if first == "--tunnel" {
        let name = arguments.next().ok_or("tunnel name is required")?;
        let environment = arguments.next().ok_or("environment is required")?;
        let origin_host = arguments.next().ok_or("origin host is required")?;
        let origin_port = arguments.next().ok_or("origin port is required")?.parse()?;
        let component = derive_tunnel(&name, &environment, &origin_host, origin_port)?;
        println!("{}", serde_json::to_string_pretty(&component)?);
        return Ok(());
    }
    let root = PathBuf::from(first);
    let environment = arguments.next().ok_or("environment is required")?;
    let mut dependencies = BTreeMap::new();
    for argument in arguments {
        let (name, hash) = argument
            .split_once('=')
            .ok_or("dependency must be producer=64hex")?;
        let bytes = hex::decode(hash)?;
        let hash: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "dependency hash must contain 32 bytes")?;
        dependencies.insert(name.to_owned(), hash);
    }
    let component = derive_component(root, &environment, &dependencies)?;
    println!("{}", serde_json::to_string_pretty(&component)?);
    Ok(())
}
