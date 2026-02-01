//! cardano-ground-truth generate command implementation.
//!
//! Generates ground-truth fixtures from cardano-db-sync for integration tests.

use anyhow::{bail, Context, Result};
use clap::Args;

use crate::config::{load_xtask_config, Network};
use crate::util::resolve_path;

/// Arguments for cardano-ground-truth command.
#[derive(Debug, Args)]
pub struct GenerateArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Generate ground-truth from origin up to this epoch (inclusive)
    #[arg(long)]
    pub epoch: u64,

    /// Overwrite existing ground-truth files
    #[arg(long, action)]
    pub force: bool,
}

/// Run the cardano-ground-truth command.
pub fn run(args: &GenerateArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let dbsync_url = xtask_config
        .dbsync
        .url_for_network(&args.network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no dbsync URL configured for network '{}' in xtask.toml",
                args.network.as_str()
            )
        })?;

    // Ground-truth goes inside the instance folder
    let instance_name = format!("test-{}-{}", args.network.as_str(), args.epoch);
    let instance_dir = instances_root.join(&instance_name);
    let output_dir = instance_dir.join("ground-truth");

    if !instance_dir.exists() {
        bail!(
            "instance not found: {}\n       Run: cargo xtask test-instance create --network {} --epoch {}",
            instance_dir.display(),
            args.network.as_str(),
            args.epoch
        );
    }

    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("creating output dir: {}", output_dir.display()))?;

    println!(
        "Generating ground-truth for {} epoch {} from DBSync...",
        args.network.as_str(),
        args.epoch
    );
    println!(" DBSync URL: {}", dbsync_url);
    println!("  Output dir: {}", output_dir.display());

    // Fetch data from DBSync
    let eras = super::eras::fetch(dbsync_url, args.epoch, &args.network)?;
    let epoch_limit = args.epoch.saturating_sub(1);
    let epochs = super::epochs::fetch(dbsync_url, epoch_limit)?;

    // Write eras.csv
    let eras_path = output_dir.join("eras.csv");
    super::eras::write_csv(&eras_path, &eras).with_context(|| "writing eras csv")?;
    println!("  Wrote: {}", eras_path.display());

    // Write epochs.csv
    let epochs_path = output_dir.join("epochs.csv");
    super::epochs::write_csv(&epochs_path, &epochs).with_context(|| "writing epochs csv")?;
    println!(" Wrote: {}", epochs_path.display());

    // Write pparams.csv
    let pparams = super::pparams::fetch(dbsync_url, epoch_limit)?;
    let pparams_path = output_dir.join("pparams.csv");
    super::pparams::write_csv(&pparams_path, &pparams).with_context(|| "writing pparams csv")?;
    println!("  Wrote: {}", pparams_path.display());

    // Fetch and write pools, accounts, rewards for earned_epoch (= epoch - 1)
    let subject_epoch = args.epoch.saturating_sub(2);

    let delegation_rows = super::delegation::fetch(dbsync_url, subject_epoch)?;
    let delegation_path = output_dir.join(format!("delegation-{}.csv", subject_epoch));
    super::delegation::write_csv(&delegation_path, &delegation_rows)
        .with_context(|| "writing delegation csv")?;
    println!("  Wrote: {}", delegation_path.display());

    let stake_rows = super::stake::fetch(dbsync_url, subject_epoch)?;
    let stake_path = output_dir.join(format!("stake-{}.csv", subject_epoch));
    super::stake::write_csv(&stake_path, &stake_rows).with_context(|| "writing stake csv")?;
    println!("  Wrote: {}", stake_path.display());

    let reward_rows = super::rewards::fetch(dbsync_url, subject_epoch)?;
    let rewards_path = output_dir.join("rewards.csv");
    super::rewards::write_csv(&rewards_path, &reward_rows)
        .with_context(|| "writing rewards csv")?;
    println!("  Wrote: {}", rewards_path.display());

    println!("Ground-truth generation complete.");

    Ok(())
}
