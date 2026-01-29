use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use dolos_core::config::{CardanoConfig, ChainConfig, RootConfig};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use xshell::{cmd, Shell};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run external tests
    ExternalTest,

    /// Bootstrap a local Mithril snapshot into an instance
    BootstrapMithrilLocal(BootstrapArgs),
}

#[derive(Debug, Args)]
struct BootstrapArgs {
    #[arg(long, value_enum)]
    network: Network,

    #[arg(long)]
    stop_epoch: u64,

    /// Optional instance name; defaults to "{network}-{epoch}"
    #[arg(long)]
    name: Option<String>,

    /// Overwrite existing instance data
    #[arg(long, action)]
    force: bool,
}

#[derive(Clone, Debug, ValueEnum)]
enum Network {
    Mainnet,
    Preview,
    Preprod,
}

impl Network {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Preview => "preview",
            Self::Preprod => "preprod",
        }
    }
}

#[derive(Debug, Deserialize)]
struct XtaskConfig {
    instances_root: PathBuf,
    snapshots: SnapshotConfig,
}

#[derive(Debug, Deserialize)]
struct SnapshotConfig {
    mainnet: PathBuf,
    preview: PathBuf,
    preprod: PathBuf,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mainnet: PathBuf::from("./xtask/snapshots/mainnet"),
            preview: PathBuf::from("./xtask/snapshots/preview"),
            preprod: PathBuf::from("./xtask/snapshots/preprod"),
        }
    }
}

impl Default for XtaskConfig {
    fn default() -> Self {
        Self {
            instances_root: PathBuf::from("./xtask/instances"),
            snapshots: SnapshotConfig::default(),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse_from(normalize_args());
    let sh = Shell::new()?;

    match cli.command {
        Commands::ExternalTest => {
            println!("Running smoke tests...");
            cmd!(sh, "cargo test --test smoke -- --ignored --nocapture").run()?;
        }
        Commands::BootstrapMithrilLocal(args) => run_bootstrap_mithril_local(&sh, &args)?,
    }

    Ok(())
}

fn normalize_args() -> Vec<String> {
    let mut args: Vec<String> = std::env::args().collect();

    if args.get(1).is_some_and(|arg| arg == "xtask") {
        args.remove(1);
    }

    args
}

fn run_bootstrap_mithril_local(sh: &Shell, args: &BootstrapArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let snapshot_dir = resolve_path(
        &repo_root,
        match args.network {
            Network::Mainnet => &xtask_config.snapshots.mainnet,
            Network::Preview => &xtask_config.snapshots.preview,
            Network::Preprod => &xtask_config.snapshots.preprod,
        },
    );

    if !snapshot_dir.exists() {
        bail!("snapshot directory not found: {}", snapshot_dir.display());
    }

    let instance_name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("{}-{}", args.network.as_str(), args.stop_epoch));

    let instance_dir = instances_root.join(instance_name);
    let data_dir = instance_dir.join("data");
    let config_path = instance_dir.join("dolos.toml");

    std::fs::create_dir_all(&instance_dir)
        .with_context(|| format!("creating instance dir: {}", instance_dir.display()))?;

    if data_dir.exists() && dir_has_entries(&data_dir)? && !args.force {
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

    let mut chain_config = match config.chain {
        ChainConfig::Cardano(cardano) => cardano,
    };

    chain_config.stop_epoch = Some(args.stop_epoch as _);
    config.chain = ChainConfig::Cardano(chain_config);

    let genesis_dir = repo_root
        .join("examples")
        .join(format!("sync-{}", args.network.as_str()));
    config.genesis.byron_path = genesis_dir.join("byron.json");
    config.genesis.shelley_path = genesis_dir.join("shelley.json");
    config.genesis.alonzo_path = genesis_dir.join("alonzo.json");
    config.genesis.conway_path = genesis_dir.join("conway.json");

    if let Some(ref mut ouroboros) = config.serve.ouroboros {
        ouroboros.listen_path = instance_dir.join("dolos.socket");
    }

    let serialized = toml::to_string_pretty(&config).context("serializing config")?;
    std::fs::write(&config_path, serialized)
        .with_context(|| format!("writing config: {}", config_path.display()))?;

    let mut sh = sh.clone();
    sh.change_dir(&repo_root);

    cmd!(
        sh,
        "cargo run --release --bin dolos -- --config {config_path} bootstrap mithril --skip-download --skip-validation --retain-snapshot --download-dir {snapshot_dir}"
    )
    .run()?
    ;

    Ok(())
}

fn load_xtask_config(repo_root: &Path) -> Result<XtaskConfig> {
    let config_path = repo_root.join("xtask.toml");
    if !config_path.exists() {
        return Ok(XtaskConfig::default());
    }

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading xtask config: {}", config_path.display()))?;
    let config = toml::from_str(&raw).context("parsing xtask.toml")?;
    Ok(config)
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn dir_has_entries(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let mut entries =
        std::fs::read_dir(path).with_context(|| format!("reading dir: {}", path.display()))?;
    Ok(entries.next().is_some())
}
