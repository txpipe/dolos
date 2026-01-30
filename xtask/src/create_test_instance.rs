//! create-test-instance command implementation.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use xshell::Shell;

use crate::bootstrap;
use crate::config::{load_xtask_config, Network};
use crate::ground_truth;
use crate::util::resolve_path;

/// Arguments for the create-test-instance command.
#[derive(Debug, Args)]
pub struct CreateTestInstanceArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Stop syncing at the beginning of this epoch
    #[arg(long)]
    pub epoch: u64,

    /// Skip ground-truth generation
    #[arg(long, action)]
    pub skip_ground_truth: bool,

    /// Skip bootstrap step
    #[arg(long, action)]
    pub skip_bootstrap: bool,
}

/// Run the create-test-instance command.
pub fn run(sh: &Shell, args: &CreateTestInstanceArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let instance_name = format!("test-{}-{}", args.network.as_str(), args.epoch);
    let instance_dir = instances_root.join(&instance_name);

    if instance_dir.exists() && !args.skip_bootstrap {
        bail!(
            "instance already exists: {}\n       Use: cargo xtask delete-test-instance --network {} --epoch {} --yes",
            instance_dir.display(),
            args.network.as_str(),
            args.epoch
        );
    }

    if !instance_dir.exists() && args.skip_bootstrap {
        bail!(
            "instance does not exist for ground truth: {}",
            instance_dir.display()
        );
    }

    if args.skip_bootstrap && args.skip_ground_truth {
        bail!("nothing to do: both bootstrap and ground truth are skipped");
    }

    let bootstrap_args = bootstrap::BootstrapArgs {
        network: args.network.clone(),
        stop_epoch: args.epoch,
        name: None,
        force: false,
    };

    let bootstrap_error = if args.skip_bootstrap {
        None
    } else {
        bootstrap::run(sh, &bootstrap_args).err()
    };

    if args.skip_ground_truth {
        if let Some(error) = bootstrap_error {
            return Err(anyhow!(
                "bootstrap failed (ground truth skipped): {}",
                error
            ));
        }
        return Ok(());
    }

    let ground_truth_args = ground_truth::GroundTruthArgs {
        network: args.network.clone(),
        epoch: args.epoch,
        force: false,
    };

    let ground_truth_result = ground_truth::run(&ground_truth_args);

    if let Err(ground_truth_error) = ground_truth_result {
        if let Some(error) = bootstrap_error {
            return Err(anyhow!(
                "ground truth failed: {}; bootstrap failed: {}",
                ground_truth_error,
                error
            ));
        }
        return Err(ground_truth_error);
    }

    if let Some(error) = bootstrap_error {
        return Err(anyhow!(
            "bootstrap failed (ground truth succeeded): {}",
            error
        ));
    }

    Ok(())
}
