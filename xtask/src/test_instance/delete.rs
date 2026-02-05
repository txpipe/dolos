//! delete test-instance command implementation.

use anyhow::{bail, Context, Result};
use clap::Args;

use crate::config::{load_xtask_config, Network};
use crate::util::resolve_path;

/// Arguments for the delete test-instance command.
#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Target epoch
    #[arg(long)]
    pub epoch: u64,

    /// Confirm deletion
    #[arg(long, action)]
    pub yes: bool,
}

/// Run the delete test-instance command.
pub fn run(args: &DeleteArgs) -> Result<()> {
    if !args.yes {
        bail!("refusing to delete without --yes");
    }

    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let instance_name = format!("test-{}-{}", args.network.as_str(), args.epoch);
    let instance_dir = instances_root.join(&instance_name);

    if !instance_dir.exists() {
        bail!("instance not found: {}", instance_dir.display());
    }

    if !instance_name.starts_with("test-") {
        bail!("refusing to delete non test-* instance: {}", instance_name);
    }

    std::fs::remove_dir_all(&instance_dir)
        .with_context(|| format!("deleting instance: {}", instance_dir.display()))?;

    println!("Deleted instance: {}", instance_dir.display());

    Ok(())
}
