use miette::{Context, IntoDiagnostic};
use std::path::Path;
use tracing::{info, warn};

use dolos::prelude::*;

use crate::feedback::Feedback;
use dolos_core::config::RootConfig;

mod archive;
mod helpers;
mod state;

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    #[arg(long, default_value = "./snapshot")]
    download_dir: String,

    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,

    /// Skip the Mithril certificate validation
    #[arg(long, action)]
    skip_validation: bool,

    /// Assume the snapshot is already available in the download dir
    #[arg(long, action)]
    skip_download: bool,

    /// Retain downloaded snapshot instead of deleting it
    #[arg(long, action)]
    retain_snapshot: bool,

    /// Number of blocks to process in each chunk, more is faster but uses more
    /// memory
    #[arg(long, default_value = "500")]
    chunk_size: usize,

    /// Only process the state pass
    #[arg(long, action)]
    state_only: bool,

    /// Only process the archive pass
    #[arg(long, action)]
    archive_only: bool,

    #[arg(long, action)]
    verbose: bool,

    #[arg(long)]
    start_from: Option<ChainPoint>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            download_dir: "./snapshot".to_string(),
            skip_if_not_empty: Default::default(),
            skip_validation: Default::default(),
            skip_download: Default::default(),
            retain_snapshot: Default::default(),
            verbose: Default::default(),
            chunk_size: 500,
            state_only: Default::default(),
            archive_only: Default::default(),
            start_from: None,
        }
    }
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    if args.verbose {
        crate::common::setup_tracing(&config.logging)?;
    }

    let mithril = config
        .mithril
        .as_ref()
        .ok_or(miette::miette!("missing mithril config"))?;

    let target_directory = Path::new(&args.download_dir);

    if !target_directory.exists() {
        std::fs::create_dir_all(target_directory)
            .map_err(|err| miette::miette!(err.to_string()))
            .context(format!(
                "Failed to create directory: {}",
                target_directory.display()
            ))?;
    }

    if !args.skip_download {
        helpers::fetch_snapshot(args, mithril, feedback)
            .await
            .map_err(|err| miette::miette!(err.to_string()))
            .context("fetching and validating mithril snapshot")?;
    } else {
        warn!("skipping download, assuming download dir has snapshot and it's validated")
    }

    if args.state_only && args.archive_only {
        miette::bail!("cannot use --state-only and --archive-only together");
    }

    let run_state = !args.archive_only;
    let run_archive = !args.state_only;

    let immutable_path = Path::new(&args.download_dir).join("immutable");

    if run_state {
        state::import_hardano_into_state(args, config, &immutable_path, feedback, args.chunk_size)
            .await?;
    }

    if run_archive {
        archive::import_hardano_into_archive(
            args,
            config,
            &immutable_path,
            feedback,
            args.chunk_size,
        )
        .await?;
    }

    if !args.retain_snapshot {
        info!("deleting downloaded snapshot");

        std::fs::remove_dir_all(Path::new(&args.download_dir))
            .into_diagnostic()
            .context("removing downloaded snapshot")?;
    }

    println!("bootstrap complete, run `dolos daemon` to start the node");

    Ok(())
}
