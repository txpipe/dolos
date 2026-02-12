//! cardano-ground-truth generate command implementation.
//!
//! Generates ground-truth fixtures from cardano-db-sync for integration tests.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::config::{load_xtask_config, Network};

/// Arguments for cardano-ground-truth command.
#[derive(Debug, Args)]
pub struct GenerateArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Subject epoch: the epoch being analyzed (ground-truth covers epochs 1..=subject_epoch)
    #[arg(long)]
    pub subject_epoch: u64,

    /// Output directory for generated CSV files
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Overwrite existing ground-truth files
    #[arg(long, action)]
    pub force: bool,

    /// Only download eras data
    #[arg(long, action)]
    pub only_eras: bool,

    /// Only download epochs data
    #[arg(long, action)]
    pub only_epochs: bool,

    /// Only download protocol parameters
    #[arg(long, action)]
    pub only_pparams: bool,

    /// Only download delegation snapshots
    #[arg(long, action)]
    pub only_delegation: bool,

    /// Only download stake snapshots
    #[arg(long, action)]
    pub only_stake: bool,

    /// Only download rewards data
    #[arg(long, action)]
    pub only_rewards: bool,
}

/// Run the cardano-ground-truth command.
pub fn run(args: &GenerateArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let dbsync_url = xtask_config
        .dbsync
        .url_for_network(&args.network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no dbsync URL configured for network '{}' in xtask.toml",
                args.network.as_str()
            )
        })?;

    let output_dir = &args.output_dir;

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output dir: {}", output_dir.display()))?;

    // Check if any --only-* flag was specified
    let has_only_flags = args.only_eras
        || args.only_epochs
        || args.only_pparams
        || args.only_delegation
        || args.only_stake
        || args.only_rewards;

    if has_only_flags {
        println!(
            "Generating selected ground-truth for {} subject_epoch {} from DBSync...",
            args.network.as_str(),
            args.subject_epoch
        );
    } else {
        println!(
            "Generating ground-truth for {} subject_epoch {} from DBSync...",
            args.network.as_str(),
            args.subject_epoch
        );
    }
    println!("  DBSync URL: {}", dbsync_url);
    println!("  Output dir: {}", output_dir.display());

    // performance_epoch = subject_epoch - 2: the epoch where rewards were earned.
    // RUPD at subject_epoch uses the mark snapshot (subject - 1) to compute rewards
    // for performance done in subject - 2.
    let performance_epoch = args.subject_epoch.saturating_sub(2);

    // Conditionally fetch and write each dataset
    if !has_only_flags || args.only_eras {
        let eras = super::eras::fetch(dbsync_url, args.subject_epoch, &args.network)?;
        let eras_path = output_dir.join("eras.csv");
        super::eras::write_csv(&eras_path, &eras).with_context(|| "writing eras csv")?;
        println!("  Wrote: {}", eras_path.display());
    }

    if !has_only_flags || args.only_epochs {
        let epochs = super::epochs::fetch(dbsync_url, args.subject_epoch)?;
        let epochs_path = output_dir.join("epochs.csv");
        super::epochs::write_csv(&epochs_path, &epochs).with_context(|| "writing epochs csv")?;
        println!("  Wrote: {}", epochs_path.display());
    }

    if !has_only_flags || args.only_pparams {
        let pparams = super::pparams::fetch(dbsync_url, args.subject_epoch)?;
        let pparams_path = output_dir.join("pparams.csv");
        super::pparams::write_csv(&pparams_path, &pparams)
            .with_context(|| "writing pparams csv")?;
        println!("  Wrote: {}", pparams_path.display());
    }

    if !has_only_flags || args.only_delegation {
        let delegation_rows = super::delegation::fetch(dbsync_url, performance_epoch)?;
        let delegation_path = output_dir.join(format!("delegation-{}.csv", performance_epoch));
        super::delegation::write_csv(&delegation_path, &delegation_rows)
            .with_context(|| "writing delegation csv")?;
        println!("  Wrote: {}", delegation_path.display());
    }

    if !has_only_flags || args.only_stake {
        let stake_rows = super::stake::fetch(dbsync_url, performance_epoch)?;
        let stake_path = output_dir.join(format!("stake-{}.csv", performance_epoch));
        super::stake::write_csv(&stake_path, &stake_rows).with_context(|| "writing stake csv")?;
        println!("  Wrote: {}", stake_path.display());
    }

    if !has_only_flags || args.only_rewards {
        let reward_rows = super::rewards::fetch(dbsync_url, performance_epoch)?;
        let rewards_path = output_dir.join("rewards.csv");
        super::rewards::write_csv(&rewards_path, &reward_rows)
            .with_context(|| "writing rewards csv")?;
        println!("  Wrote: {}", rewards_path.display());
    }

    println!("Ground-truth generation complete.");

    Ok(())
}
