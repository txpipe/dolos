//! `fixture pull` — pull a test-fixture bundle and extract layers locally.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;
use serde_json::Value;
use xshell::Shell;

use crate::config::{load_xtask_config, Network};
use crate::fixture::{
    build_ref, ground_truth_key, oras, pack, resolve_local_dir, seed_key, upstream_key,
    ANN_SEED_EPOCH, ANN_UPSTREAM_END, ANN_UPSTREAM_START, GROUND_TRUTH_LAYER_FILE,
    SEED_LAYER_FILE, UPSTREAM_LAYER_FILE,
};

#[derive(Debug, Args)]
pub struct PullArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Subject epoch identifying the test-fixture tag
    #[arg(long)]
    pub epoch: u64,

    /// Overwrite existing local fixture dirs for this test
    #[arg(long, action)]
    pub force: bool,

    /// Resolve the manifest but skip the pull/extract
    #[arg(long, action)]
    pub dry_run: bool,
}

pub fn run(sh: &Shell, args: &PullArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let local_dir = resolve_local_dir(&repo_root, &xtask_config)?;

    let registry = xtask_config
        .fixtures
        .registry
        .as_deref()
        .context("fixtures.registry not configured in xtask.toml")?;

    let network_str = args.network.as_str();
    let reference = build_ref(registry, network_str, args.epoch);

    println!("Pulling {reference}");

    if args.dry_run {
        println!("(dry-run, not calling oras manifest fetch or pull)");
        return Ok(());
    }

    let manifest = oras::manifest_fetch(sh, &reference)?;

    let seed_epoch = read_u64_annotation(&manifest, ANN_SEED_EPOCH)?;
    let upstream_start = read_u64_annotation(&manifest, ANN_UPSTREAM_START)?;
    let upstream_end = read_u64_annotation(&manifest, ANN_UPSTREAM_END)?;

    let seed_dest = local_dir
        .join("seeds")
        .join(seed_key(network_str, seed_epoch));
    let gt_dest = local_dir
        .join("ground-truth")
        .join(ground_truth_key(network_str, args.epoch));
    let upstream_dest = local_dir
        .join("upstream")
        .join(upstream_key(network_str, upstream_start, upstream_end));

    println!("  seed ({seed_epoch}):       {}", seed_dest.display());
    println!("  ground-truth ({}):  {}", args.epoch, gt_dest.display());
    println!(
        "  upstream ({upstream_start}-{upstream_end}): {}",
        upstream_dest.display()
    );

    if !args.force {
        let all_exist = seed_dest.exists() && gt_dest.exists() && upstream_dest.exists();
        if all_exist {
            println!("All destinations already exist; skipping (pass --force to overwrite)");
            return Ok(());
        }
    }

    std::fs::create_dir_all(local_dir.join("seeds"))?;
    std::fs::create_dir_all(local_dir.join("ground-truth"))?;
    std::fs::create_dir_all(local_dir.join("upstream"))?;

    let download = tempfile::Builder::new()
        .prefix("dolos-fixture-pull-")
        .tempdir_in(&local_dir)
        .context("creating pull staging dir")?;

    oras::pull(sh, &reference, download.path())?;

    let seed_tar = download.path().join(SEED_LAYER_FILE);
    let gt_tar = download.path().join(GROUND_TRUTH_LAYER_FILE);
    let upstream_tar = download.path().join(UPSTREAM_LAYER_FILE);

    for (label, tar, dest) in [
        ("seed", &seed_tar, &seed_dest),
        ("ground-truth", &gt_tar, &gt_dest),
        ("upstream", &upstream_tar, &upstream_dest),
    ] {
        if !tar.exists() {
            bail!(
                "{label} layer not found in pulled artifact (expected {})",
                tar.display()
            );
        }
        extract_atomic(tar, dest, args.force)
            .with_context(|| format!("extracting {label} to {}", dest.display()))?;
    }

    println!("Pull complete");
    Ok(())
}

fn read_u64_annotation(manifest: &Value, key: &str) -> Result<u64> {
    let value = manifest
        .get("annotations")
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_str())
        .with_context(|| format!("manifest annotation `{key}` missing or not a string"))?;

    value
        .parse::<u64>()
        .with_context(|| format!("parsing annotation `{key}` (value: {value:?})"))
}

/// Extract `tar_path` into `dest` by unpacking into a sibling temp dir and
/// renaming atomically. If `dest` exists, it is removed first (only when
/// `force` is set — caller has already vetted the overwrite policy).
fn extract_atomic(tar_path: &Path, dest: &Path, force: bool) -> Result<()> {
    let parent = dest
        .parent()
        .context("fixture destination has no parent dir")?;

    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating parent {}", parent.display()))?;

    let staged = tempfile::Builder::new()
        .prefix(".dolos-fixture-extract-")
        .tempdir_in(parent)
        .with_context(|| format!("creating staging dir in {}", parent.display()))?;

    pack::extract_tar_zst(tar_path, staged.path())?;

    if dest.exists() {
        if !force {
            println!(
                "  destination exists, leaving as-is: {}",
                dest.display()
            );
            return Ok(());
        }
        remove_dir_all_best_effort(dest)?;
    }

    let staged_path: PathBuf = staged.keep();
    std::fs::rename(&staged_path, dest)
        .with_context(|| format!("rename {} -> {}", staged_path.display(), dest.display()))?;

    Ok(())
}

fn remove_dir_all_best_effort(path: &Path) -> Result<()> {
    std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))
}
