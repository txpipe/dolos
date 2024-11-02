use flate2::read::GzDecoder;
use inquire::list_option::ListOption;
use miette::{Context, IntoDiagnostic};
use tar::Archive;

use crate::feedback::{Feedback, ProgressReader};

#[derive(Debug, clap::Args, Default, Clone)]
pub struct Args {
    /// The variant of the snapshot to download (full, ledger).
    #[arg(long)]
    pub variant: String,

    /// The point in history of the snapshot (eg: era, epoch or `latest`).
    #[arg(long, default_value = "latest")]
    pub point: String,
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
        })
    }
}

const DEFAULT_URL_TEMPLATE: &str =
    "https://dolos-snapshots.s3-accelerate.amazonaws.com/v0/${NETWORK}/${VARIANT}/${POINT}.tar.gz";

fn define_snapshot_url(config: &crate::Config, args: &Args) -> String {
    let download_url_template = config
        .snapshot
        .as_ref()
        .map(|x| x.download_url.to_owned())
        .unwrap_or(DEFAULT_URL_TEMPLATE.to_owned());

    download_url_template
        .replace("${NETWORK}", &config.upstream.network_magic.to_string())
        .replace("${POINT}", &args.point)
        .replace("${VARIANT}", &args.variant)
}

fn fetch_snapshot(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let snapshot_url = define_snapshot_url(config, args)
        .replace("${NETWORK}", &config.upstream.network_magic.to_string())
        .replace("${POINT}", &args.point)
        .replace("${VARIANT}", &args.variant);

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
        .unpack(&config.storage.path)
        .into_diagnostic()
        .context("Failed to extract snapshot")?;

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    fetch_snapshot(config, args, feedback)?;

    Ok(())
}
