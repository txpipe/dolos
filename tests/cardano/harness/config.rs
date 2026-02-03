use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct XtaskConfig {
    pub instances_root: PathBuf,
}

impl Default for XtaskConfig {
    fn default() -> Self {
        Self {
            instances_root: PathBuf::from("./xtask/instances"),
        }
    }
}

pub fn load_xtask_config(repo_root: &Path) -> Result<XtaskConfig> {
    let config_path = repo_root.join("xtask.toml");
    if !config_path.exists() {
        return Ok(XtaskConfig::default());
    }

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading xtask config: {}", config_path.display()))?;
    let config = toml::from_str(&raw).context("parsing xtask.toml")?;
    Ok(config)
}

pub fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
