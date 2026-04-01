use clap::Parser;
use dolos::storage::ArchiveStoreBackend;
use dolos_core::config::RootConfig;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::{bail, IntoDiagnostic as _};
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use tar::Builder;

#[derive(Debug, Parser)]
pub struct Args {
    /// the path to export to
    #[arg(short, long)]
    output: PathBuf,

    // Whether to include archive
    #[arg(long, action)]
    include_archive: bool,

    // Whether to include state
    #[arg(long, action)]
    include_state: bool,

    // Whether to include indexes
    #[arg(long, action)]
    include_indexes: bool,

    /// Skip the compact and integrity check of the archive database
    #[arg(long, action)]
    skip_sanitization: bool,
}

fn is_macos_metadata(path: &Path) -> bool {
    if path
        .components()
        .any(|component| component.as_os_str() == OsStr::new("__MACOSX"))
    {
        return true;
    }

    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };

    file_name == ".DS_Store" || file_name.starts_with("._")
}

/// Create a tar header with deterministic metadata for the given path.
fn deterministic_header(source: &Path) -> miette::Result<tar::Header> {
    let metadata = std::fs::metadata(source).into_diagnostic()?;
    let mut header = tar::Header::new_gnu();

    if metadata.is_dir() {
        header.set_entry_type(tar::EntryType::Directory);
        header.set_size(0);
    } else {
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(metadata.len());
    }

    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);
    header.set_username("").into_diagnostic()?;
    header.set_groupname("").into_diagnostic()?;
    header.set_mode(if metadata.is_dir() { 0o755 } else { 0o644 });
    header.set_cksum();

    Ok(header)
}

fn append_path_filtered(
    archive: &mut Builder<GzEncoder<File>>,
    source: &Path,
    name: &Path,
) -> miette::Result<()> {
    if source.is_dir() {
        append_dir_filtered(archive, source, name)?;
        return Ok(());
    }

    let header = deterministic_header(source)?;
    let file = File::open(source).into_diagnostic()?;
    archive.append_data(&mut header.clone(), name, file).into_diagnostic()?;

    Ok(())
}

fn append_dir_filtered(
    archive: &mut Builder<GzEncoder<File>>,
    source: &Path,
    name: &Path,
) -> miette::Result<()> {
    let header = deterministic_header(source)?;
    archive.append_data(&mut header.clone(), name, &[] as &[u8]).into_diagnostic()?;

    let mut entries: Vec<_> = std::fs::read_dir(source)
        .into_diagnostic()?
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();

        if is_macos_metadata(&path) {
            continue;
        }

        let entry_name = name.join(entry.file_name());

        if path.is_dir() {
            append_dir_filtered(archive, &path, &entry_name)?;
        } else {
            let header = deterministic_header(&path)?;
            let file = File::open(&path).into_diagnostic()?;
            archive.append_data(&mut header.clone(), &entry_name, file).into_diagnostic()?;
        }
    }

    Ok(())
}

fn prepare_archive(
    archive: &mut dolos_redb3::archive::ArchiveStore<dolos_cardano::CardanoError>,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = archive.db_mut();
    pb.set_message("compacting archive");
    db.compact().into_diagnostic()?;

    pb.set_message("checking archive integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let pb = feedback.indeterminate_progress_bar();

    let export_file = File::create(&args.output).into_diagnostic()?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut archive = Builder::new(encoder);

    let mut stores = crate::common::open_data_stores(config)?;
    let root = crate::common::ensure_storage_path(config)?;

    // prepare_archive requires direct redb access
    match &mut stores.archive {
        ArchiveStoreBackend::Redb(s) if !args.skip_sanitization => {
            prepare_archive(s, &pb)?
        }
        ArchiveStoreBackend::Redb(_) => {}
        ArchiveStoreBackend::NoOp(_) => {
            bail!("export command is not available for noop archive backend")
        }
    }

    if args.include_archive {
        let path = root.join("archive");

        append_path_filtered(&mut archive, &path, Path::new("archive"))?;

        pb.set_message("creating archive");
    }

    if args.include_state {
        let path = root.join("state");

        append_path_filtered(&mut archive, &path, Path::new("state"))?;

        pb.set_message("creating archive");
    }

    if args.include_indexes {
        let path = root.join("index");

        append_path_filtered(&mut archive, &path, Path::new("index"))?;

        pb.set_message("creating archive");
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}
