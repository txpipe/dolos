//! Thin wrappers around the `oras` CLI.
//!
//! We shell out via xshell rather than depending on a Rust OCI library to
//! keep xtask light and match the rest of this crate's pattern (which already
//! shells out to `dolos`, `psql`, etc.).

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;
use xshell::{cmd, Shell};

/// A single layer to push: local file + OCI media type.
pub struct Layer<'a> {
    pub file: &'a Path,
    pub media_type: &'a str,
}

/// Push a multi-layer artifact to `reference` (e.g. `ghcr.io/foo/bar:tag`).
///
/// `artifact_type` sets the manifest's `artifactType`; `annotations` are
/// manifest-level annotations applied via `--annotation`.
pub fn push(
    sh: &Shell,
    reference: &str,
    artifact_type: &str,
    annotations: &[(String, String)],
    layers: &[Layer],
) -> Result<()> {
    let mut ann_args: Vec<String> = Vec::with_capacity(annotations.len() * 2);
    for (k, v) in annotations {
        ann_args.push("--annotation".to_string());
        ann_args.push(format!("{k}={v}"));
    }

    let mut file_args: Vec<String> = Vec::with_capacity(layers.len());
    for layer in layers {
        file_args.push(format!("{}:{}", layer.file.display(), layer.media_type));
    }

    cmd!(
        sh,
        "oras push {reference} --artifact-type {artifact_type} {ann_args...} {file_args...}"
    )
    .run()
    .with_context(|| format!("oras push {reference}"))?;

    Ok(())
}

/// Pull `reference` into `output_dir`. Blob filenames come from the layer
/// `org.opencontainers.image.title` annotations, which oras auto-populates on
/// push based on the local filename of each layer file.
pub fn pull(sh: &Shell, reference: &str, output_dir: &Path) -> Result<()> {
    cmd!(sh, "oras pull {reference} --output {output_dir}")
        .run()
        .with_context(|| format!("oras pull {reference}"))?;
    Ok(())
}

/// Fetch the manifest JSON for `reference`.
pub fn manifest_fetch(sh: &Shell, reference: &str) -> Result<Value> {
    let out = cmd!(sh, "oras manifest fetch {reference}")
        .read()
        .with_context(|| format!("oras manifest fetch {reference}"))?;

    let parsed: Value = serde_json::from_str(&out)
        .with_context(|| format!("parsing manifest JSON for {reference}"))?;

    Ok(parsed)
}

/// Log in to a registry. Reads the token from stdin.
#[allow(dead_code)]
pub fn login(sh: &Shell, registry: &str, username: &str, token: &str) -> Result<()> {
    cmd!(
        sh,
        "oras login {registry} -u {username} --password-stdin"
    )
    .stdin(token)
    .run()
    .with_context(|| format!("oras login {registry}"))?;
    Ok(())
}
