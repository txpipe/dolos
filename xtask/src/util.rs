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

/// Check if a directory exists and has at least one entry.
pub fn dir_has_entries(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    let mut entries =
        std::fs::read_dir(path).with_context(|| format!("reading dir: {}", path.display()))?;
    Ok(entries.next().is_some())
}
