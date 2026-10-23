use clap::Parser;
use dolos::storage::{ArchiveStoreBackend, StateStoreBackend};
use dolos_core::config::RootConfig;
use dolos_fjall::flatfiles::parse_segment_filename;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::{bail, IntoDiagnostic as _};
use std::ffi::OsStr;
use std::fs::{DirEntry, File};
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

/// A segment file in the store's own naming.
fn is_segment(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .and_then(parse_segment_filename)
        .is_some()
}

/// `path` as the filesystem resolves it, so that every spelling of a directory
/// compares equal; made absolute where the filesystem cannot resolve it (some
/// Windows volumes) rather than matching nothing.
fn resolved(path: &Path) -> Option<PathBuf> {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .ok()
}

/// `source` under `name`, leaving out the segment files kept in `segments`, a
/// [`resolved`] path, when that directory is `source` or lies inside it.
fn append_path_filtered(
    archive: &mut Builder<GzEncoder<File>>,
    source: &Path,
    name: &Path,
    segments: Option<&Path>,
) -> miette::Result<()> {
    if source.is_dir() {
        append_dir_filtered(archive, source, name, segments)?;
        return Ok(());
    }

    let header = deterministic_header(source)?;
    let file = File::open(source).into_diagnostic()?;
    archive
        .append_data(&mut header.clone(), name, file)
        .into_diagnostic()?;

    Ok(())
}

fn append_dir_filtered(
    archive: &mut Builder<GzEncoder<File>>,
    source: &Path,
    name: &Path,
    segments: Option<&Path>,
) -> miette::Result<()> {
    let header = deterministic_header(source)?;
    archive
        .append_data(&mut header.clone(), name, &[] as &[u8])
        .into_diagnostic()?;

    let mut entries: Vec<_> = std::fs::read_dir(source)
        .into_diagnostic()?
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    entries.sort_by_key(|e| e.file_name());

    let holds_segments =
        segments.is_some_and(|segments| resolved(source).is_some_and(|dir| dir == segments));

    for entry in entries {
        let path = entry.path();

        if is_macos_metadata(&path) || holds_segments && is_segment(&entry) {
            continue;
        }

        let entry_name = name.join(entry.file_name());

        if path.is_dir() {
            append_dir_filtered(archive, &path, &entry_name, segments)?;
        } else {
            let header = deterministic_header(&path)?;
            let file = File::open(&path).into_diagnostic()?;
            archive
                .append_data(&mut header.clone(), &entry_name, file)
                .into_diagnostic()?;
        }
    }

    Ok(())
}

/// The archive directory under `archive/`; for a node keeping its segment
/// files at `segments_dir`, its `index/` with those segments beside it, and
/// left out of `index/` where `segments_dir` lies inside it. That is the
/// default layout, so a snapshot restores the same way whatever `blocks_path`
/// the node it came from had.
fn append_archive(
    archive: &mut Builder<GzEncoder<File>>,
    archive_dir: &Path,
    segments_dir: Option<&Path>,
) -> miette::Result<()> {
    let Some(segments_dir) = segments_dir else {
        return append_path_filtered(archive, archive_dir, Path::new("archive"), None);
    };

    let name = Path::new("archive");
    let header = deterministic_header(archive_dir)?;
    archive
        .append_data(&mut header.clone(), name, &[] as &[u8])
        .into_diagnostic()?;

    let mut segments: Vec<_> = std::fs::read_dir(segments_dir)
        .into_diagnostic()?
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    segments.retain(is_segment);
    segments.sort_by_key(|entry| entry.file_name());

    for entry in segments {
        append_path_filtered(archive, &entry.path(), &name.join(entry.file_name()), None)?;
    }

    append_path_filtered(
        archive,
        &archive_dir.join("index"),
        &name.join("index"),
        resolved(segments_dir).as_deref(),
    )
}

fn ensure_state_exportable(state: &StateStoreBackend, include_state: bool) -> miette::Result<()> {
    if matches!(state, StateStoreBackend::Memory(_)) && include_state {
        bail!("the in-memory state keeps nothing on disk to export");
    }

    Ok(())
}

fn create_export_file(
    output: &Path,
    state: &StateStoreBackend,
    include_state: bool,
) -> miette::Result<File> {
    ensure_state_exportable(state, include_state)?;
    File::create(output).into_diagnostic()
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let pb = feedback.indeterminate_progress_bar();

    let mut stores = crate::common::open_data_stores(config)?;
    let root = crate::common::ensure_storage_path(config)?;

    let export_file = create_export_file(&args.output, &stores.state, args.include_state)?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut archive = Builder::new(encoder);

    match &mut stores.archive {
        ArchiveStoreBackend::LogsOnly(_) | ArchiveStoreBackend::TagReads(_)
            if !args.skip_sanitization =>
        {
            bail!("archive sanitization needs exclusive access to the archive database")
        }
        // The memory archive holds its blocks in process, so there is no
        // `<root>/archive` for `--include-archive` to pick up. The other
        // stores may still be on disk, so only the archive half is refused.
        ArchiveStoreBackend::Memory(_) if args.include_archive => {
            bail!("the in-memory archive keeps nothing on disk to export")
        }
        // Sanitization requires direct backend access: fjall major-compacts
        // both keyspaces (an LSM has no offline integrity check to run).
        ArchiveStoreBackend::Fjall(s) if !args.skip_sanitization => {
            pb.set_message("compacting archive");
            s.compact().into_diagnostic()?;
        }
        ArchiveStoreBackend::Memory(_)
        | ArchiveStoreBackend::LogsOnly(_)
        | ArchiveStoreBackend::TagReads(_)
        | ArchiveStoreBackend::Fjall(_) => {}
        ArchiveStoreBackend::NoOp(_) => {
            bail!("`history.mode = \"none\"` keeps no archive to export")
        }
    }

    // Ensure all stores flush pending work and release filesystem state before
    // we archive their on-disk files.
    stores.wal.shutdown().into_diagnostic()?;
    stores.state.shutdown().into_diagnostic()?;
    stores.archive.shutdown().into_diagnostic()?;
    drop(stores);

    let segments = config.storage.blocks_path();

    if args.include_archive {
        let path = root.join("archive");

        append_archive(&mut archive, &path, segments.as_deref())?;

        pb.set_message("creating archive");
    }

    if args.include_state {
        let path = root.join("state");

        // A `blocks_path` at or under the state directory holds the archive's
        // segments, which travel with the archive.
        let segments = segments.as_deref().and_then(resolved);

        append_path_filtered(&mut archive, &path, Path::new("state"), segments.as_deref())?;

        pb.set_message("creating archive");
    }

    archive.finish().into_diagnostic()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn including_an_in_memory_state_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("existing.tar.gz");
        std::fs::write(&output, b"keep me").unwrap();
        let state = StateStoreBackend::in_memory().unwrap();

        ensure_state_exportable(&state, false).unwrap();

        let error = create_export_file(&output, &state, true).unwrap_err();
        assert!(error.to_string().contains("nothing on disk to export"));
        assert_eq!(std::fs::read(output).unwrap(), b"keep me");
    }

    #[test]
    fn a_path_the_filesystem_cannot_resolve_is_still_compared() {
        let missing = Path::new("no/such/dir");

        assert_eq!(
            resolved(missing),
            Some(std::path::absolute(missing).unwrap())
        );
    }
}
