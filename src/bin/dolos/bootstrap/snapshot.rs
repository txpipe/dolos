use flate2::read::GzDecoder;
use miette::{Context, IntoDiagnostic};
use tar::Archive;
use tracing::info;

use crate::feedback::{Feedback, ProgressReader};

#[derive(Debug, clap::Args, Default)]
pub struct Args {
    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,
}

fn fetch_snapshot(config: &crate::Config, feedback: &Feedback, args: &Args) -> miette::Result<()> {
    let snapshot_url = config
        .snapshot
        .as_ref()
        .map(|x| &x.download_url)
        .ok_or_else(|| miette::miette!("Snapshot URL not specified in config"))?;

    // Check if exists and is not empty.
    if let Ok(mut entries) = std::fs::read_dir(&config.storage.path) {
        if entries.next().is_some() && args.skip_if_not_empty {
            info!("Skipping bootstrap, data already present.");
            return Ok(());
        }
    }

    std::fs::create_dir_all(&config.storage.path)
        .into_diagnostic()
        .context("Failed to create target directory")?;

    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10)) // Follow up to 10 redirects
        .build()
        .into_diagnostic()
        .context("Failed to build HTTP client")?;

    let response = client
        .get(snapshot_url)
        .send()
        .into_diagnostic()
        .context("Failed to download snapshot")?;

    let progress = feedback.bytes_progress_bar();

    let total_size = response.content_length().unwrap_or(0);
    progress.set_length(total_size);

    let response = ProgressReader::new(response, progress);

    let tar_gz = GzDecoder::new(response);
    let mut archive = Archive::new(tar_gz);

    archive
        .unpack(&config.storage.path)
        .into_diagnostic()
        .context("Failed to extract snapshot")?;

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    fetch_snapshot(config, feedback, args)?;

    Ok(())
}
