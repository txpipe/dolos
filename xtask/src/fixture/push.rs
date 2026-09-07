//! `fixture push` — pack and push a test-fixture bundle.

use anyhow::{bail, Context, Result};
use clap::Args;
use xshell::Shell;

use crate::config::{load_xtask_config, Network};
use crate::fixture::{
    build_ref, ground_truth_key, oras, pack, resolve_local_dir, seed_key, upstream_key,
    ANN_NETWORK, ANN_SCHEMA_VERSION, ANN_SEED_EPOCH, ANN_SUBJECT_EPOCH, ANN_UPSTREAM_END,
    ANN_UPSTREAM_START, ARTIFACT_TYPE, GROUND_TRUTH_LAYER_FILE, GROUND_TRUTH_MEDIA_TYPE,
    SCHEMA_VERSION, SEED_LAYER_FILE, SEED_MEDIA_TYPE, UPSTREAM_LAYER_FILE, UPSTREAM_MEDIA_TYPE,
};

const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Args)]
pub struct PushArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Subject epoch for this test (used as the tag's epoch component)
    #[arg(long)]
    pub epoch: u64,

    /// Epoch of the seed to bundle — resolves to `seeds/{network}-{seed_epoch}/`
    #[arg(long)]
    pub seed_epoch: u64,

    /// Start of the upstream range to bundle
    #[arg(long)]
    pub upstream_start: u64,

    /// End of the upstream range to bundle
    #[arg(long)]
    pub upstream_end: u64,

    /// Show what would be pushed without calling oras
    #[arg(long, action)]
    pub dry_run: bool,
}

pub fn run(sh: &Shell, args: &PushArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let local_dir = resolve_local_dir(&repo_root, &xtask_config)?;

    let registry = xtask_config
        .fixtures
        .registry
        .as_deref()
        .context("fixtures.registry not configured in xtask.toml")?;

    let network_str = args.network.as_str();

    let seed_dir = local_dir
        .join("seeds")
        .join(seed_key(network_str, args.seed_epoch));
    let gt_dir = local_dir
        .join("ground-truth")
        .join(ground_truth_key(network_str, args.epoch));
    let upstream_dir = local_dir.join("upstream").join(upstream_key(
        network_str,
        args.upstream_start,
        args.upstream_end,
    ));

    for (label, dir) in [
        ("seed", &seed_dir),
        ("ground-truth", &gt_dir),
        ("upstream", &upstream_dir),
    ] {
        if !dir.exists() {
            bail!("{label} source dir not found: {}", dir.display());
        }
    }

    let reference = build_ref(registry, network_str, args.epoch);

    println!("Push target: {reference}");
    println!("  seed:         {}", seed_dir.display());
    println!("  ground-truth: {}", gt_dir.display());
    println!("  upstream:     {}", upstream_dir.display());

    if args.dry_run {
        println!("(dry-run, not packing or pushing)");
        return Ok(());
    }

    let staging = tempfile::Builder::new()
        .prefix("dolos-fixture-push-")
        .tempdir()
        .context("creating push staging dir")?;

    let seed_tar = staging.path().join(SEED_LAYER_FILE);
    let gt_tar = staging.path().join(GROUND_TRUTH_LAYER_FILE);
    let upstream_tar = staging.path().join(UPSTREAM_LAYER_FILE);

    println!("Packing seed…");
    pack::pack_tar_zst(&seed_dir, &seed_tar, DEFAULT_ZSTD_LEVEL)?;
    println!("  -> {} ({})", seed_tar.display(), human_size(&seed_tar)?);

    println!("Packing ground-truth…");
    pack::pack_tar_zst(&gt_dir, &gt_tar, DEFAULT_ZSTD_LEVEL)?;
    println!("  -> {} ({})", gt_tar.display(), human_size(&gt_tar)?);

    println!("Packing upstream…");
    pack::pack_tar_zst(&upstream_dir, &upstream_tar, DEFAULT_ZSTD_LEVEL)?;
    println!(
        "  -> {} ({})",
        upstream_tar.display(),
        human_size(&upstream_tar)?
    );

    let annotations = vec![
        (ANN_SCHEMA_VERSION.to_string(), SCHEMA_VERSION.to_string()),
        (ANN_NETWORK.to_string(), network_str.to_string()),
        (ANN_SUBJECT_EPOCH.to_string(), args.epoch.to_string()),
        (ANN_SEED_EPOCH.to_string(), args.seed_epoch.to_string()),
        (
            ANN_UPSTREAM_START.to_string(),
            args.upstream_start.to_string(),
        ),
        (
            ANN_UPSTREAM_END.to_string(),
            args.upstream_end.to_string(),
        ),
    ];

    let layers = [
        oras::Layer {
            file: &seed_tar,
            media_type: SEED_MEDIA_TYPE,
        },
        oras::Layer {
            file: &gt_tar,
            media_type: GROUND_TRUTH_MEDIA_TYPE,
        },
        oras::Layer {
            file: &upstream_tar,
            media_type: UPSTREAM_MEDIA_TYPE,
        },
    ];

    // `oras push` needs to see layer files by just their filename so that the
    // auto-populated `org.opencontainers.image.title` annotation matches what
    // pull expects. Run with cwd set to staging so relative paths match.
    let _dir_guard = sh.push_dir(staging.path());

    let layers_rel: Vec<oras::Layer> = layers
        .iter()
        .map(|l| oras::Layer {
            file: std::path::Path::new(l.file.file_name().unwrap()),
            media_type: l.media_type,
        })
        .collect();

    println!("Pushing to {reference}…");
    oras::push(sh, &reference, ARTIFACT_TYPE, &annotations, &layers_rel)?;

    println!("Push complete: {reference}");
    Ok(())
}

fn human_size(path: &std::path::Path) -> Result<String> {
    let bytes = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    Ok(format_bytes(bytes))
}

fn format_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} {}", UNITS[unit])
    } else {
        format!("{size:.2} {}", UNITS[unit])
    }
}
