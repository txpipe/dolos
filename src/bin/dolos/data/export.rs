use clap::Parser;
use dolos::storage::{ArchiveStoreBackend, IndexStoreBackend, StateStoreBackend};
use dolos_core::config::RootConfig;
use flate2::write::GzEncoder;
use flate2::Compression;
use miette::{bail, Context as _, IntoDiagnostic as _};
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

    /// Rebuild stores into canonical form before archiving.
    ///
    /// Each store is re-written from a snapshot into a fresh database so that
    /// two exports of the same chain data produce identical exports. The live
    /// stores are not modified. Mutually exclusive with --skip-sanitization.
    ///
    /// Byte-identity guarantee by backend:
    ///   archive/  (redb)  — byte-for-byte identical across independent runs.
    ///   state/    (fjall) — content-identical (same keys/values), but NOT
    ///                       bit-for-bit identical: Fjall SSTable files embed
    ///                       wall-clock timestamps that differ between runs.
    ///   index/    (fjall) — same caveat as state/.
    ///
    /// Track the upstream Fjall fix at:
    /// https://github.com/fjall-rs/lsm-tree/issues/296
    #[arg(long, action, conflicts_with = "skip_sanitization")]
    canonical: bool,
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
    archive
        .append_data(&mut header.clone(), name, file)
        .into_diagnostic()?;

    Ok(())
}

fn append_dir_filtered(
    archive: &mut Builder<GzEncoder<File>>,
    source: &Path,
    name: &Path,
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
            archive
                .append_data(&mut header.clone(), &entry_name, file)
                .into_diagnostic()?;
        }
    }

    Ok(())
}

/// Tar `archive/`, replacing `archive/index` with `canonical_index`.
fn append_archive_with_canonical_index(
    tar: &mut Builder<GzEncoder<File>>,
    source_dir: &Path,
    tar_name: &Path,
    canonical_index: &Path,
) -> miette::Result<()> {
    let header = deterministic_header(source_dir)?;
    tar.append_data(&mut header.clone(), tar_name, &[] as &[u8])
        .into_diagnostic()?;

    let mut entries: Vec<_> = std::fs::read_dir(source_dir)
        .into_diagnostic()?
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();

        if is_macos_metadata(&path) {
            continue;
        }

        let entry_name = tar_name.join(entry.file_name());

        if entry.file_name() == OsStr::new("index") {
            // Substitute the live index with the rebuilt canonical version.
            let mut header = deterministic_header(canonical_index)?;
            header.set_size(std::fs::metadata(canonical_index).into_diagnostic()?.len());
            header.set_cksum();
            let file = File::open(canonical_index).into_diagnostic()?;
            tar.append_data(&mut header.clone(), &entry_name, file)
                .into_diagnostic()?;
        } else {
            append_path_filtered(tar, &path, &entry_name)?;
        }
    }

    Ok(())
}

fn prepare_archive(
    archive: &mut dolos_redb3::archive::ArchiveStore,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<()> {
    let db = archive.db_mut();
    pb.set_message("compacting archive");
    db.compact().into_diagnostic()?;

    pb.set_message("checking archive integrity");
    db.check_integrity().into_diagnostic()?;

    Ok(())
}

/// Rebuild each requested store into a fresh canonical copy.
///
/// Temp paths are registered in `CanonicalPaths` as soon as they are created, so `Drop`
/// removes them even when a later rebuild step fails.
fn rebuild_stores_canonical(
    stores: &crate::common::Stores,
    root: &Path,
    args: &Args,
    pb: &crate::feedback::ProgressBar,
) -> miette::Result<CanonicalPaths> {
    let mut paths = CanonicalPaths::default();

    if args.include_archive {
        match &stores.archive {
            ArchiveStoreBackend::Redb(s) => {
                // Keep temp file outside archive/ so it is never swept into the tar.
                let dest = root.join("archive_index.canonical.tmp");
                let _ = std::fs::remove_file(&dest); // remove stale from a previous failed run
                paths.archive_index = Some(dest);
                pb.set_message("rebuilding archive index");
                s.rebuild_index_to(paths.archive_index.as_ref().unwrap())
                    .into_diagnostic()
                    .context("rebuild archive index")?;
            }
            ArchiveStoreBackend::NoOp(_) => {
                bail!("--canonical is not available for noop archive backend")
            }
        }
    }

    if args.include_state {
        match &stores.state {
            StateStoreBackend::Fjall(s) => {
                pb.println(
                    "warning: --canonical: state/ will be content-identical but NOT \
                     bit-for-bit identical — Fjall SSTable files embed wall-clock timestamps \
                     that differ between runs (https://github.com/fjall-rs/lsm-tree/issues/296)",
                );
                let dest = root.join("state.canonical.tmp");
                let _ = std::fs::remove_dir_all(&dest); // remove stale from a previous failed run
                paths.state = Some(dest);
                pb.set_message("rebuilding state store");
                s.rebuild_canonical(paths.state.as_ref().unwrap())
                    .into_diagnostic()
                    .context("rebuild state store")?;
            }
            StateStoreBackend::Redb(_) => {
                bail!("--canonical is not yet supported for Redb state backend")
            }
        }
    }

    if args.include_indexes {
        match &stores.indexes {
            IndexStoreBackend::Fjall(s) => {
                pb.println(
                    "warning: --canonical: index/ will be content-identical but NOT \
                     bit-for-bit identical — Fjall SSTable files embed wall-clock timestamps \
                     that differ between runs (https://github.com/fjall-rs/lsm-tree/issues/296)",
                );
                let dest = root.join("index.canonical.tmp");
                let _ = std::fs::remove_dir_all(&dest); // remove stale from a previous failed run
                paths.index = Some(dest);
                pb.set_message("rebuilding index store");
                s.rebuild_canonical(paths.index.as_ref().unwrap())
                    .into_diagnostic()
                    .context("rebuild index store")?;
            }
            IndexStoreBackend::Redb(_) => {
                bail!("--canonical is not yet supported for Redb index backend")
            }
            IndexStoreBackend::NoOp(_) => {}
        }
    }

    Ok(paths)
}

#[derive(Default)]
struct CanonicalPaths {
    archive_index: Option<PathBuf>,
    state: Option<PathBuf>,
    index: Option<PathBuf>,
}

impl Drop for CanonicalPaths {
    fn drop(&mut self) {
        if let Some(p) = self.archive_index.take() {
            let _ = std::fs::remove_file(&p);
        }
        if let Some(p) = self.state.take() {
            let _ = std::fs::remove_dir_all(&p);
        }
        if let Some(p) = self.index.take() {
            let _ = std::fs::remove_dir_all(&p);
        }
    }
}

pub fn run(
    config: &RootConfig,
    args: &Args,
    feedback: &crate::feedback::Feedback,
) -> miette::Result<()> {
    let pb = feedback.indeterminate_progress_bar();

    let export_file = File::create(&args.output).into_diagnostic()?;
    let encoder = GzEncoder::new(export_file, Compression::default());
    let mut tar = Builder::new(encoder);

    let mut stores = crate::common::open_data_stores(config)?;
    let root = crate::common::ensure_storage_path(config)?;

    if args.canonical {
        // Rebuild stores before shutdown so we can snapshot them.
        let canonical = rebuild_stores_canonical(&stores, &root, args, &pb)?;

        // Shut down all live stores before accessing their files.
        stores.wal.shutdown().into_diagnostic()?;
        stores.state.shutdown().into_diagnostic()?;
        stores.archive.shutdown().into_diagnostic()?;
        stores.indexes.shutdown().into_diagnostic()?;
        drop(stores);

        let result = (|| {
            if args.include_archive {
                pb.set_message("archiving (canonical)");
                let archive_dir = root.join("archive");
                match &canonical.archive_index {
                    Some(idx) => {
                        append_archive_with_canonical_index(
                            &mut tar,
                            &archive_dir,
                            Path::new("archive"),
                            idx,
                        )?;
                    }
                    None => {
                        append_dir_filtered(&mut tar, &archive_dir, Path::new("archive"))?;
                    }
                }
            }

            if args.include_state {
                pb.set_message("archiving state (canonical)");
                let live_state = root.join("state");
                let state_src = canonical.state.as_deref().unwrap_or(&live_state);
                append_dir_filtered(&mut tar, state_src, Path::new("state"))?;
            }

            if args.include_indexes {
                pb.set_message("archiving indexes (canonical)");
                let live_index = root.join("index");
                let index_src = canonical.index.as_deref().unwrap_or(&live_index);
                append_dir_filtered(&mut tar, index_src, Path::new("index"))?;
            }

            tar.finish().into_diagnostic()
        })();

        drop(canonical); // clean up temp dirs/files before propagating any error
        result?;
    } else {
        // Non-canonical path: compact + integrity check archive, then tar live stores.
        match &mut stores.archive {
            ArchiveStoreBackend::Redb(s) if !args.skip_sanitization => prepare_archive(s, &pb)?,
            ArchiveStoreBackend::Redb(_) => {}
            ArchiveStoreBackend::NoOp(_) => {
                bail!("export command is not available for noop archive backend")
            }
        }

        // Ensure all stores flush pending work and release filesystem state before
        // we archive their on-disk files.
        stores.wal.shutdown().into_diagnostic()?;
        stores.state.shutdown().into_diagnostic()?;
        stores.archive.shutdown().into_diagnostic()?;
        stores.indexes.shutdown().into_diagnostic()?;
        drop(stores);

        if args.include_archive {
            let path = root.join("archive");
            append_path_filtered(&mut tar, &path, Path::new("archive"))?;
            pb.set_message("creating archive");
        }

        if args.include_state {
            let path = root.join("state");
            append_path_filtered(&mut tar, &path, Path::new("state"))?;
            pb.set_message("creating archive");
        }

        if args.include_indexes {
            let path = root.join("index");
            append_path_filtered(&mut tar, &path, Path::new("index"))?;
            pb.set_message("creating archive");
        }

        tar.finish().into_diagnostic()?;
    }

    Ok(())
}
