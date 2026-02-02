//! create test-instance command implementation.

use anyhow::{bail, Context, Result};
use clap::Args;
use xshell::Shell;

use crate::bootstrap;
use crate::config::{load_xtask_config, Network};
use crate::util::resolve_path;

/// Arguments for the create test-instance command.
#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Stop syncing at the beginning of this epoch
    #[arg(long)]
    pub epoch: u64,

    /// Enable verbose output for the bootstrap command
    #[arg(long, action)]
    pub verbose: bool,
}

/// Run the create test-instance command.
pub fn run(sh: &Shell, args: &CreateArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let instance_name = format!("test-{}-{}", args.network.as_str(), args.epoch);
    let instance_dir = instances_root.join(&instance_name);

    if instance_dir.exists() {
        bail!(
            "instance already exists: {}\n       Use: cargo xtask test-instance delete --network {} --epoch {} --yes",
            instance_dir.display(),
            args.network.as_str(),
            args.epoch
        );
    }

    let bootstrap_args = bootstrap::BootstrapArgs {
        network: args.network.clone(),
        stop_epoch: args.epoch,
        name: None,
        force: false,
        verbose: args.verbose,
    };

    bootstrap::run(sh, &bootstrap_args)?;

    Ok(())
}
