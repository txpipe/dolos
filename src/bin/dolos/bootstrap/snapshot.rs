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

    /// Where to restore from, as a URL. Today only `file://DIR`, naming a
    /// stele directory written by `dolos snapshot publish --output-dir`.
    ///
    /// Parsed by clap rather than by `run`, so an unusable source is refused
    /// before `--force` has cleared anything: the flags that decide what to do
    /// with existing data are handled a layer above this command.
    #[arg(long)]
    pub source: Option<Source>,
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
            source: None,
        })
    }
}

/// Where a `--source` points.
///
/// The scheme is what selects a restore path, which is why this is parsed
/// rather than sniffed: a directory that happens to look like a stele and a URL
/// that says it is one are different claims, and only the second is the
/// operator's.
#[derive(Debug, Clone)]
pub enum Source {
    /// A stele directory on this filesystem.
    Dir(PathBuf),
}

impl std::str::FromStr for Source {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // `file:///abs/path` is the spelled-out form and leaves a leading slash
        // behind, which is the absolute path. `file://relative/path` is the one
        // an operator actually types, and leaves a relative one. Both work, and
        // neither is guessed at: what follows the scheme is the path.
        if let Some(path) = raw.strip_prefix("file://") {
            if path.is_empty() {
                return Err(format!("{raw:?} names no directory"));
            }

            return Ok(Self::Dir(PathBuf::from(path)));
        }

        if raw.starts_with("oci://") {
            return Err(
                "registry sources are not implemented yet; publish to a directory with \
                 `dolos snapshot publish --output-dir` and restore from it with \
                 `--source file://DIR`"
                    .to_owned(),
            );
        }

        Err(format!(
            "{raw:?} is not a snapshot source; the only scheme implemented today is `file://DIR`"
        ))
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

/// Stream a single HTTP response directly into the extractor. Used only when
/// the endpoint does not support range requests.
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

/// Restore a stele directory into this node's stores.
///
/// Everything below this is `dolos_snapshot::restore`, which is generic over
/// the store traits; what only this function knows is the node — its magic,
/// which comes from genesis and never from a file an operator can edit, and its
/// `sync.max_history`, which is what bounds how much chain history a restore
/// bothers to fetch.
fn restore_stele(config: &RootConfig, dir: &std::path::Path) -> miette::Result<()> {
    let root = crate::common::ensure_storage_path(config)
        .into_diagnostic()
        .context("creating the storage directory")?;

    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let (plan, summary) = dolos_snapshot::restore::restore_dir(
        dir,
        u64::from(genesis.network_magic()),
        config.sync.max_history,
        &root,
        &stores.archive,
        &stores.state,
        &stores.indexes,
    )
    .into_diagnostic()
    .context("restoring the stele")?;

    println!(
        "network:  {} ({})",
        plan.position.network.name(),
        plan.position.network.magic()
    );
    println!("cursor:   {}", plan.position.point);
    println!("sequence: {}", plan.sequence);

    if plan.skipped_epochs > 0 {
        println!(
            "epochs:   {} restored, {} skipped by sync.max_history",
            plan.epochs.len(),
            plan.skipped_epochs,
        );
    } else {
        println!("epochs:   {}", plan.epochs.len());
    }

    println!(
        "restored: {} blocks, {} logs, {} index records, {} entities, {} utxos",
        summary.blocks, summary.logs, summary.index_records, summary.entities, summary.utxos,
    );

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    // The tarball path is unchanged and stays the default: `--source` is what
    // opts into a stele, and until the registry transport lands there is
    // nothing to make it the default of.
    if let Some(Source::Dir(dir)) = &args.source {
        return restore_stele(config, dir);
    }

    if let Some(path) = &args.file {
        import_local_snapshot(config, path)?;
    } else {
        fetch_snapshot(config, args, feedback)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_source_names_a_directory() {
        for (raw, expected) in [
            ("file:///var/lib/dolos/stele", "/var/lib/dolos/stele"),
            ("file://stele", "stele"),
            ("file://./stele", "./stele"),
        ] {
            let Source::Dir(dir) = raw.parse::<Source>().unwrap();
            assert_eq!(dir, PathBuf::from(expected), "{raw:?}");
        }
    }

    #[test]
    fn an_unimplemented_or_unknown_scheme_is_refused() {
        for raw in [
            "oci://ghcr.io/txpipe/dolos-snapshots/mainnet",
            "https://example.invalid/snapshot",
            "/var/lib/dolos/stele",
            "file://",
            "",
        ] {
            assert!(raw.parse::<Source>().is_err(), "{raw:?}");
        }
    }
}
