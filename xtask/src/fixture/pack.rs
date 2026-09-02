//! tar + zstd helpers for fixture archives.

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

/// Pack the *contents* of `src_dir` into a tar.zst at `out_path`.
///
/// The archive has no leading `src_dir` path component — entries are relative
/// to `src_dir`. Extraction therefore writes straight into whatever directory
/// the caller chooses, with no kind/epoch prefix embedded in the archive.
pub fn pack_tar_zst(src_dir: &Path, out_path: &Path, level: i32) -> Result<()> {
    let out_file =
        File::create(out_path).with_context(|| format!("creating {}", out_path.display()))?;

    let zstd_writer = zstd::Encoder::new(out_file, level)
        .context("creating zstd encoder")?
        .auto_finish();

    let mut tar_builder = tar::Builder::new(zstd_writer);
    // Follow symlinks so the archive contains the real content rather than a
    // symlink entry pointing at a path that won't exist on the consumer side.
    tar_builder.follow_symlinks(true);

    tar_builder
        .append_dir_all(".", src_dir)
        .with_context(|| format!("adding contents of {} to tar", src_dir.display()))?;

    tar_builder.finish().context("finishing tar")?;

    Ok(())
}

/// Stream-extract a tar.zst archive at `src_path` into `dst_dir`.
///
/// `dst_dir` is created if it does not exist. Existing contents are not
/// cleared — the caller is expected to extract into a fresh temp dir and
/// rename into place to get atomicity.
pub fn extract_tar_zst(src_path: &Path, dst_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dst_dir)
        .with_context(|| format!("creating {}", dst_dir.display()))?;

    let src_file =
        File::open(src_path).with_context(|| format!("opening {}", src_path.display()))?;

    let zstd_reader = zstd::Decoder::new(src_file).context("creating zstd decoder")?;

    let mut tar_archive = tar::Archive::new(zstd_reader);
    tar_archive.set_preserve_permissions(true);
    tar_archive
        .unpack(dst_dir)
        .with_context(|| format!("extracting into {}", dst_dir.display()))?;

    Ok(())
}
