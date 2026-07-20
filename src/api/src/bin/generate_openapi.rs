use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

fn committed_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../console/openapi.yaml")
}

fn generated_yaml() -> Result<String> {
    tickr_api::http::routes::openapi_yaml().context("serialize OpenAPI YAML")
}

fn main() -> Result<()> {
    let check = std::env::args().skip(1).any(|arg| arg == "--check");
    let path = committed_path();
    let generated = generated_yaml()?;
    if check {
        let committed = std::fs::read_to_string(&path)
            .with_context(|| format!("read committed OpenAPI at {}", path.display()))?;
        if committed != generated {
            bail!("{} is stale; run `just generate-openapi`", path.display());
        }
    } else {
        std::fs::write(&path, generated)
            .with_context(|| format!("write generated OpenAPI to {}", path.display()))?;
        println!("wrote {}", path.display());
    }
    Ok(())
}
