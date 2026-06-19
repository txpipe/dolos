use std::path::PathBuf;

use dolos_core::config::RootConfig;
use flate2::read::GzDecoder;
use inquire::list_option::ListOption;
use miette::{Context, IntoDiagnostic};
use tar::Archive;

use super::ranged;
use crate::feedback::{Feedback, ProgressReader};

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {
    /// The variant of the snapshot to download (full, ledger).
    #[arg(long, default_value = "full")]
    pub variant: String,

    /// The point in history of the snapshot (eg: era, epoch or `latest`).
    #[arg(long, default_value = "latest")]
    pub point: String,

    /// Path to a local snapshot tar.gz file to import instead of downloading.
    #[arg(long)]
    pub file: Option<PathBuf>,
}

impl Args {
    pub fn inquire() -> miette::Result<Self> {
        let variant = inquire::Select::new(
            "which variant of the snapshot would you like to use?",
            vec![
                ListOption::new(0, "full snapshot (ledger + chain history)"),
                ListOption::new(1, "ledger snapshot (just the ledger)"),
            ],
        )
        .prompt()
        .into_diagnostic()?;

        let variant = match variant.index {
            0 => "full".to_string(),
            1 => "ledger".to_string(),
            _ => unreachable!(),
        };

        Ok(Self {
            variant,
            point: "latest".to_string(),
            file: None,
        })
    }
}

const DEFAULT_URL_TEMPLATE: &str =
    "https://dolos-snapshots.txpipe.cloud/${VERSION}/${NETWORK}/${VARIANT}/${POINT}.tar.gz";

fn define_snapshot_url(config: &RootConfig, args: &Args) -> Option<String> {
    if config.upstream.is_emulator() {
        return None;
    }

    let magic = config.chain.magic();

    let download_url_template = config
        .snapshot
        .as_ref()
        .map(|x| x.download_url.to_owned())
        .unwrap_or(DEFAULT_URL_TEMPLATE.to_owned());

    let snapshot_url = download_url_template
        .replace("${VERSION}", &config.storage.version.to_string())
        .replace("${NETWORK}", &magic.to_string())
        .replace("${POINT}", &args.point)
        .replace("${VARIANT}", &args.variant);

    Some(snapshot_url)
}

fn import_local_snapshot(config: &RootConfig, path: &PathBuf) -> miette::Result<()> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root)
        .into_diagnostic()
        .context("Failed to create target directory")?;

    let file = std::fs::File::open(path)
        .into_diagnostic()
        .context("Failed to open local snapshot file")?;

    let tar_gz = GzDecoder::new(file);
    let mut archive = Archive::new(tar_gz);

    archive
        .unpack(root)
        .into_diagnostic()
        .context("Failed to extract snapshot")?;

    Ok(())
}

fn fetch_snapshot(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root)
        .into_diagnostic()
        .context("Failed to create target directory")?;

    let snapshot_url = define_snapshot_url(config, args).ok_or(miette::miette!(
        "can't find a valid snapshot for this configuration"
    ))?;

    let client = ranged::build_client()?;

    let probe = ranged::probe(&client, &snapshot_url)?;

    if probe.supports_ranges && probe.total_size > 0 {
        fetch_snapshot_ranged(root, &client, snapshot_url, probe.total_size, feedback)
    } else {
        // Fall back to a single streamed response for endpoints that don't
        // advertise range support (e.g. a custom download_url behind a proxy
        // that strips Accept-Ranges).
        fetch_snapshot_streaming(root, snapshot_url, feedback)
    }
}

/// Download via bounded byte ranges staged on disk, extracting as chunks land.
/// Resilient to servers (such as Cloudflare R2) that drop long-lived, slowly
/// drained streamed responses.
fn fetch_snapshot_ranged(
    root: &PathBuf,
    client: &reqwest::blocking::Client,
    snapshot_url: String,
    total_size: u64,
    feedback: &Feedback,
) -> miette::Result<()> {
    let staging = root.join(".dolos-snapshot-tmp");

    // Start from a clean staging dir in case a previous attempt left chunks behind.
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging)
        .into_diagnostic()
        .context("Failed to create snapshot staging directory")?;

    let progress = feedback.bytes_progress_bar();
    progress.set_length(total_size);
    // Keep the bar redrawing even while the downloader is blocked on
    // backpressure (waiting for the extractor to free a window slot), so it
    // never looks frozen during a legitimate pause.
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    let reader = ranged::ranged_reader(
        client.clone(),
        snapshot_url,
        total_size,
        staging.clone(),
        progress,
    );

    let tar_gz = GzDecoder::new(reader);
    let mut archive = Archive::new(tar_gz);

    let result = archive
        .unpack(root)
        .into_diagnostic()
        .context("Failed to extract snapshot");

    // Drop the archive (and its reader) before tearing down staging so the
    // downloader thread is joined and all chunk files are released.
    drop(archive);
    let _ = std::fs::remove_dir_all(&staging);

    result
}

/// Stream a single HTTP response directly into the extractor. Used only when the
/// endpoint does not support range requests.
fn fetch_snapshot_streaming(
    root: &PathBuf,
    snapshot_url: String,
    feedback: &Feedback,
) -> miette::Result<()> {
    // A single full-body stream must NOT carry an overall request timeout, which
    // would cap the entire multi-GB transfer. Use a dedicated untimed client.
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .into_diagnostic()
        .context("Failed to build HTTP client")?;

    let response = client
        .get(snapshot_url)
        .send()
        .into_diagnostic()
        .context("Failed to download snapshot")?;

    let response = response
        .error_for_status()
        .into_diagnostic()
        .context("Failed to download snapshot")?;

    let progress = feedback.bytes_progress_bar();

    let total_size = response.content_length().unwrap_or(0);
    progress.set_length(total_size);

    let response = ProgressReader::new(response, progress);

    let tar_gz = GzDecoder::new(response);
    let mut archive = Archive::new(tar_gz);

    archive
        .unpack(root)
        .into_diagnostic()
        .context("Failed to extract snapshot")?;

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    if let Some(path) = &args.file {
        import_local_snapshot(config, path)?;
    } else {
        fetch_snapshot(config, args, feedback)?;
    }

    Ok(())
}
