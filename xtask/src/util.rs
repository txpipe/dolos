//! Shared utilities for xtask commands.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Resolve a path relative to a base directory.
///
/// If the path is absolute, returns it as-is.
/// If the path is relative, joins it with the base directory.
pub fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

/// Recursively copy a directory tree from `src` to `dst`.
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)
        .with_context(|| format!("creating dir: {}", dst.display()))?;

    for entry in std::fs::read_dir(src)
        .with_context(|| format!("reading dir: {}", src.display()))?
    {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path).with_context(|| {
                format!(
                    "copying {} -> {}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        }
    }

    Ok(())
}

/// Check if a directory exists and has at least one entry.
pub fn dir_has_entries(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let mut entries =
        std::fs::read_dir(path).with_context(|| format!("reading dir: {}", path.display()))?;
    Ok(entries.next().is_some())
}
