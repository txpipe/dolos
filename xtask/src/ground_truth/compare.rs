//! Compare Dolos CSV outputs with DBSync ground-truth CSVs.

use anyhow::{Context, Result};
use clap::Args;

use crate::config::{load_xtask_config, Network};
use crate::util::{compare_csvs, resolve_path};

/// Arguments for comparing CSV ground-truth fixtures.
#[derive(Debug, Args)]
pub struct CompareArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Instance epoch (used to resolve instance path)
    #[arg(long)]
    pub epoch: u64,

    /// Optional instance name override
    #[arg(long)]
    pub instance_name: Option<String>,
}

pub fn run(args: &CompareArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let stop_epoch = args.epoch;
    let subject_epoch = stop_epoch.saturating_sub(2);

    let instance_name = args
        .instance_name
        .clone()
        .unwrap_or_else(|| format!("test-{}-{}", args.network.as_str(), stop_epoch));
    let instance_dir = instances_root.join(&instance_name);

    if !instance_dir.exists() {
        anyhow::bail!(
            "instance not found: {}\n       Run: cargo xtask test-instance create --network {} --epoch {}",
            instance_dir.display(),
            args.network.as_str(),
            stop_epoch
        );
    }

    let ground_truth_dir = instance_dir.join("ground-truth");
    let dumps_dir = instance_dir.join("dumps");
    std::fs::create_dir_all(&dumps_dir)
        .with_context(|| format!("creating dumps dir: {}", dumps_dir.display()))?;

    let config_path = instance_dir.join("dolos.toml");
    if !config_path.exists() {
        anyhow::bail!("instance config not found: {}", config_path.display());
    }

    let mut total_diffs = 0usize;
    let mut failed = Vec::new();

    // Compare eras
    let eras_dolos_path = dumps_dir.join("eras.csv");
    super::eras::dump_dolos_csv(&config_path, &eras_dolos_path)?;
    let eras_gt_path = ground_truth_dir.join("eras.csv");
    println!("Comparing eras (full state)");
    println!("file1: {}", eras_dolos_path.display());
    println!("file2: {}", eras_gt_path.display());
    let n = compare_csvs(&eras_dolos_path, &eras_gt_path, &[0], 20)?;
    total_diffs += n;
    if n > 0 { failed.push("eras"); }

    // Compare epochs
    let epochs_dolos_path = dumps_dir.join("epochs.csv");
    super::epochs::dump_dolos_csv(&config_path, stop_epoch, &epochs_dolos_path)?;
    let epochs_gt_path = ground_truth_dir.join("epochs.csv");
    println!("\nComparing epochs (stop epoch {})", stop_epoch);
    println!("file1: {}", epochs_dolos_path.display());
    println!("file2: {}", epochs_gt_path.display());
    let n = compare_csvs(&epochs_dolos_path, &epochs_gt_path, &[0], 20)?;
    total_diffs += n;
    if n > 0 { failed.push("epochs"); }

    // Compare delegation
    let delegation_dolos_path = super::delegation::dolos_csv_path(&dumps_dir, subject_epoch);
    let delegation_gt_path = ground_truth_dir.join(format!("delegation-{}.csv", subject_epoch));
    println!("\nComparing delegation (subject epoch {})", subject_epoch);
    println!("file1: {}", delegation_dolos_path.display());
    println!("file2: {}", delegation_gt_path.display());
    let n = compare_csvs(&delegation_dolos_path, &delegation_gt_path, &[0], 20)?;
    total_diffs += n;
    if n > 0 { failed.push("delegation"); }

    // Compare stake
    let stake_dolos_path = super::stake::dolos_csv_path(&dumps_dir, subject_epoch);
    let stake_gt_path = ground_truth_dir.join(format!("stake-{}.csv", subject_epoch));
    println!("\nComparing stake (subject epoch {})", subject_epoch);
    println!("file1: {}", stake_dolos_path.display());
    println!("file2: {}", stake_gt_path.display());
    let n = compare_csvs(&stake_dolos_path, &stake_gt_path, &[0, 1], 20)?;
    total_diffs += n;
    if n > 0 { failed.push("stake"); }

    // Compare rewards
    let rewards_dolos_path = dumps_dir.join("rewards.csv");
    super::rewards::dump_dolos_csv(&config_path, subject_epoch, &rewards_dolos_path)?;
    let rewards_gt_path = ground_truth_dir.join("rewards.csv");
    println!("\nComparing rewards (subject epoch {})", subject_epoch);
    println!("file1: {}", rewards_dolos_path.display());
    println!("file2: {}", rewards_gt_path.display());
    let n = compare_csvs(&rewards_dolos_path, &rewards_gt_path, &[0, 1, 3, 4], 20)?;
    total_diffs += n;
    if n > 0 { failed.push("rewards"); }

    if total_diffs > 0 {
        anyhow::bail!(
            "{} total differences in: {}",
            total_diffs,
            failed.join(", ")
        );
    }

    println!("\nAll comparisons passed.");
    Ok(())
}
