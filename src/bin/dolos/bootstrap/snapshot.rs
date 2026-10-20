use std::io::Read;
use std::path::{Component, Path, PathBuf};

use dolos_core::config::{RootConfig, StorageConfig};
use dolos_fjall::flatfiles::parse_segment_filename;
use flate2::read::GzDecoder;
use inquire::list_option::ListOption;
use miette::{Context, IntoDiagnostic};
use tar::Archive;

use dolos::storage::CURRENT_STORAGE_VERSION;

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

    /// URL template to download from; `${VERSION}`, `${NETWORK}`,
    /// `${VARIANT}` and `${POINT}` are substituted. Defaults to the official
    /// snapshot host
    #[arg(long, value_name = "TEMPLATE")]
    pub download_url: Option<String>,
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
            download_url: None,
        })
    }
}

const DEFAULT_URL_TEMPLATE: &str =
    "https://dolos-snapshots.txpipe.cloud/${VERSION}/${NETWORK}/${VARIANT}/${POINT}.tar.gz";

fn define_snapshot_url(config: &RootConfig, args: &Args) -> miette::Result<Option<String>> {
    if config.upstream.as_ref().is_some_and(|x| x.is_emulator()) {
        return Ok(None);
    }

    let magic = crate::common::resolve_network(config)?.magic();

    Ok(Some(snapshot_url(args, magic)))
}

/// `${VERSION}` is the storage version this binary writes: a snapshot is
/// only usable by a binary that reads its layout.
fn snapshot_url(args: &Args, magic: u64) -> String {
    args.download_url
        .as_deref()
        .unwrap_or(DEFAULT_URL_TEMPLATE)
        .replace("${VERSION}", CURRENT_STORAGE_VERSION)
        .replace("${NETWORK}", &magic.to_string())
        .replace("${POINT}", &args.point)
        .replace("${VARIANT}", &args.variant)
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

    unpack(&mut archive, &config.storage)
        .into_diagnostic()
        .context("Failed to extract snapshot")?;

    Ok(())
}

/// The file name of a segment the snapshot keeps under `archive/`, in the
/// segment store's own form. Nothing else, a drive-prefixed name on Windows
/// included, is joined onto `blocks_path`.
fn segment_name(path: &Path) -> Option<PathBuf> {
    let mut components = path.components();

    match (components.next(), components.next(), components.next()) {
        (Some(Component::Normal(dir)), Some(Component::Normal(name)), None)
            if dir == "archive" && name.to_str().and_then(parse_segment_filename).is_some() =>
        {
            Some(PathBuf::from(name))
        }
        _ => None,
    }
}

/// Unpack a snapshot into the storage root, and its segment files into the
/// node's `blocks_path` when it keeps them outside the archive directory.
fn unpack<R: Read>(archive: &mut Archive<R>, storage: &StorageConfig) -> std::io::Result<()> {
    let root = &storage.path;

    let Some(segments) = storage.blocks_path() else {
        return archive.unpack(root);
    };

    std::fs::create_dir_all(root)?;
    std::fs::create_dir_all(&segments)?;

    let mut directories = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let kind = entry.header().entry_type();

        let segment = if kind.is_file() {
            segment_name(&entry.path()?)
        } else {
            None
        };

        if kind.is_dir() {
            directories.push(entry);
        } else if let Some(name) = segment {
            entry.unpack(segments.join(name))?;
        } else {
            entry.unpack_in(root)?;
        }
    }

    // As `Archive::unpack` does: directories last and deepest first, so that
    // their permissions cannot get in the way of what goes inside them.
    directories.sort_by(|a, b| b.path_bytes().cmp(&a.path_bytes()));

    for mut directory in directories {
        directory.unpack_in(root)?;
    }

    Ok(())
}

fn fetch_snapshot(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root)
        .into_diagnostic()
        .context("Failed to create target directory")?;

    let snapshot_url = define_snapshot_url(config, args)?.ok_or(miette::miette!(
        "can't find a valid snapshot for this configuration"
    ))?;

    let client = ranged::build_client()?;

    let probe = ranged::probe(&client, &snapshot_url)?;

    if probe.supports_ranges && probe.total_size > 0 {
        fetch_snapshot_ranged(
            &config.storage,
            &client,
            snapshot_url,
            probe.total_size,
            feedback,
        )
    } else {
        // Fall back to a single streamed response for endpoints that don't
        // advertise range support (e.g. a custom --download-url behind a proxy
        // that strips Accept-Ranges).
        fetch_snapshot_streaming(&config.storage, snapshot_url, feedback)
    }
}

/// Download via bounded byte ranges staged on disk, extracting as chunks land.
/// Resilient to servers (such as Cloudflare R2) that drop long-lived, slowly
/// drained streamed responses.
fn fetch_snapshot_ranged(
    storage: &StorageConfig,
    client: &reqwest::blocking::Client,
    snapshot_url: String,
    total_size: u64,
    feedback: &Feedback,
) -> miette::Result<()> {
    let staging = storage.path.join(".dolos-snapshot-tmp");

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

    let result = unpack(&mut archive, storage)
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
    storage: &StorageConfig,
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

    unpack(&mut archive, storage)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(download_url: Option<&str>) -> Args {
        Args {
            variant: "full".to_owned(),
            point: "latest".to_owned(),
            file: None,
            download_url: download_url.map(str::to_owned),
        }
    }

    #[test]
    fn only_the_archives_own_segment_files_are_segments() {
        assert_eq!(
            segment_name(Path::new("archive/000012.segment")),
            Some(PathBuf::from("000012.segment"))
        );

        for path in [
            "archive",
            "archive/index/000012.segment",
            "archive/000012.segment.tmp",
            "archive/12.segment",
            "archive/C:x.segment",
            "archive/C:000012.segment",
            "state/000012.segment",
            "000012.segment",
        ] {
            assert_eq!(segment_name(Path::new(path)), None, "{path}");
        }
    }

    #[test]
    fn the_default_template_names_this_binarys_storage_version() {
        assert_eq!(
            snapshot_url(&args(None), 2),
            format!("https://dolos-snapshots.txpipe.cloud/{CURRENT_STORAGE_VERSION}/2/full/latest.tar.gz")
        );
    }

    #[test]
    fn an_explicit_template_wins() {
        assert_eq!(
            snapshot_url(
                &args(Some(
                    "https://mirror.example/${VERSION}/${NETWORK}-${VARIANT}-${POINT}.tgz"
                )),
                1,
            ),
            format!("https://mirror.example/{CURRENT_STORAGE_VERSION}/1-full-latest.tgz")
        );
    }
}
