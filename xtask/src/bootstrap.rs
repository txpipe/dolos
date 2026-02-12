//! bootstrap-mithril-local command implementation.
//!
//! Bootstraps a Dolos instance from a pre-downloaded Mithril snapshot.

use anyhow::{bail, Context, Result};
use clap::Args;
use dolos_cardano::include;
use dolos_core::config::{ChainConfig, RootConfig};
use xshell::{cmd, Shell};

use crate::config::{load_xtask_config, Network};
use crate::util::{copy_dir_recursive, dir_has_entries, resolve_path};

/// Arguments for the bootstrap-mithril-local command.
#[derive(Debug, Args)]
pub struct BootstrapArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Stop syncing at the beginning of this epoch
    #[arg(long)]
    pub stop_epoch: u64,

    /// Optional instance name; defaults to "test-{network}-{epoch}"
    #[arg(long)]
    pub name: Option<String>,

    /// Overwrite existing instance data
    #[arg(long, action)]
    pub force: bool,

    /// Enable verbose output
    #[arg(long, action)]
    pub verbose: bool,

    /// Skip using the seed and bootstrap from scratch
    #[arg(long, action)]
    pub skip_seed: bool,
}

/// Run the bootstrap-mithril-local command.
pub fn run(sh: &Shell, args: &BootstrapArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let snapshot_dir = resolve_path(
        &repo_root,
        xtask_config.snapshots.path_for_network(&args.network),
    );

    if !snapshot_dir.exists() {
        bail!("snapshot directory not found: {}", snapshot_dir.display());
    }

    let instance_name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("test-{}-{}", args.network.as_str(), args.stop_epoch));

    let instance_dir = instances_root.join(&instance_name);
    let data_dir = instance_dir.join("data");
    let config_path = instance_dir.join("dolos.toml");

    std::fs::create_dir_all(&instance_dir)
        .with_context(|| format!("creating instance dir: {}", instance_dir.display()))?;

    let seed_path = xtask_config
        .seeds
        .path_for_network(&args.network)
        .map(|p| resolve_path(&repo_root, p));

    if data_dir.exists() && dir_has_entries(&data_dir)? && !args.force && seed_path.is_none() {
        bail!(
            "instance data already exists (use --force to overwrite): {}",
            data_dir.display()
        );
    }

    let template_path = repo_root
        .join("xtask")
        .join("templates")
        .join(format!("default-{}.toml", args.network.as_str()));

    let template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("reading template file: {}", template_path.display()))?;

    let mut config: RootConfig = toml::from_str(&template)
        .with_context(|| format!("parsing template: {}", template_path.display()))?;

    config.storage.path = data_dir.clone();

    let ChainConfig::Cardano(mut chain_config) = config.chain;

    chain_config.stop_epoch = Some(args.stop_epoch as _);
    config.chain = ChainConfig::Cardano(chain_config);

    match args.network {
        Network::Mainnet => include::mainnet::save(&instance_dir)?,
        Network::Preview => include::preview::save(&instance_dir)?,
        Network::Preprod => include::preprod::save(&instance_dir)?,
    }

    config.genesis.byron_path = instance_dir.join("byron.json");
    config.genesis.shelley_path = instance_dir.join("shelley.json");
    config.genesis.alonzo_path = instance_dir.join("alonzo.json");
    config.genesis.conway_path = instance_dir.join("conway.json");

    if let Some(ref mut ouroboros) = config.serve.ouroboros {
        ouroboros.listen_path = instance_dir.join("dolos.socket");
    }

    let serialized = toml::to_string_pretty(&config).context("serializing config")?;
    std::fs::write(&config_path, serialized)
        .with_context(|| format!("writing config: {}", config_path.display()))?;

    // Only use seed if not skipped and configured
    if !args.skip_seed {
        if let Some(ref seed) = seed_path {
            if seed.exists() {
                println!("Copying seed data: {}", seed.display());
                copy_dir_recursive(seed, &data_dir)?;
            } else {
                bail!("seed directory not found: {}", seed.display());
            }
        }
    } else {
        println!("Skipping seed (--skip-seed specified), bootstrapping from scratch");
    }

    println!("Bootstrapping instance: {}", instance_name);
    println!("  Config: {}", config_path.display());
    println!("  Snapshot: {}", snapshot_dir.display());
    if let Some(ref seed) = seed_path {
        if !args.skip_seed {
            println!("  Seed: {}", seed.display());
        } else {
            println!("  Seed: (skipped)");
        }
    }

    let sh = sh.clone();
    let _dir_guard = sh.push_dir(&repo_root);

    let verbose_flag: &[&str] = if args.verbose { &["--verbose"] } else { &[] };

    cmd!(
        sh,
        "cargo run --release --bin dolos -- --config {config_path} bootstrap mithril --skip-download --skip-validation --retain-snapshot --download-dir {snapshot_dir} {verbose_flag...}"
    )
    .run()?;

    println!("Bootstrap complete: {}", instance_dir.display());

    Ok(())
}
