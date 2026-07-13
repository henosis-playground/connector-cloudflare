//! Print a core-registration-ready component derived from a native Wrangler
//! project.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use henosis_cloudflare_authoring::derive_component;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let root = PathBuf::from(
        arguments
            .next()
            .ok_or("usage: derive-component <root> <environment> [producer=64hex ...]")?,
    );
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
