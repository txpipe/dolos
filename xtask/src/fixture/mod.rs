//! fixture subcommands.
//!
//! Push and pull test-fixture bundles to/from an OCI registry (GHCR by default).
//! Each tag `{network}-{subject_epoch}` is an OCI artifact with three layers
//! (seed, ground-truth, upstream fragment) stitched together — one pull per
//! test.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Subcommand;
use xshell::Shell;

use crate::config::XtaskConfig;
use crate::util::resolve_path;

pub mod oras;
pub mod pack;
pub mod pull;
pub mod push;

/// Artifact type for a test-fixture manifest.
pub const ARTIFACT_TYPE: &str = "application/vnd.txpipe.dolos.test-fixture.v1";

/// Layer media type for seed fixtures (fjall state dir).
pub const SEED_MEDIA_TYPE: &str = "application/vnd.txpipe.dolos.fixture.seed.v1.tar+zstd";

/// Layer media type for ground-truth fixtures (CSV bundle).
pub const GROUND_TRUTH_MEDIA_TYPE: &str =
    "application/vnd.txpipe.dolos.fixture.ground-truth.v1.tar+zstd";

/// Layer media type for upstream fragments (Mithril immutable chunks).
pub const UPSTREAM_MEDIA_TYPE: &str = "application/vnd.txpipe.dolos.fixture.upstream.v1.tar+zstd";

/// Current fixture schema version. Bump if the on-disk layout changes.
pub const SCHEMA_VERSION: &str = "1";

// Manifest-level annotation keys.
pub const ANN_SCHEMA_VERSION: &str = "io.txpipe.dolos.schema-version";
pub const ANN_NETWORK: &str = "io.txpipe.dolos.network";
pub const ANN_SUBJECT_EPOCH: &str = "io.txpipe.dolos.subject-epoch";
pub const ANN_SEED_EPOCH: &str = "io.txpipe.dolos.seed-epoch";
pub const ANN_UPSTREAM_START: &str = "io.txpipe.dolos.upstream-start";
pub const ANN_UPSTREAM_END: &str = "io.txpipe.dolos.upstream-end";

/// Layer filenames — also used as `org.opencontainers.image.title` annotations,
/// which is how `oras pull` decides the output filename for each blob.
pub const SEED_LAYER_FILE: &str = "seed.tar.zst";
pub const GROUND_TRUTH_LAYER_FILE: &str = "ground-truth.tar.zst";
pub const UPSTREAM_LAYER_FILE: &str = "upstream.tar.zst";

#[derive(Debug, Subcommand)]
pub enum FixtureCmd {
    /// Pack and push a test-fixture bundle to the configured registry.
    Push(push::PushArgs),

    /// Pull a test-fixture bundle from the registry and extract it locally.
    Pull(pull::PullArgs),
}

pub fn run(sh: &Shell, cmd: FixtureCmd) -> Result<()> {
    match cmd {
        FixtureCmd::Push(args) => push::run(sh, &args),
        FixtureCmd::Pull(args) => pull::run(sh, &args),
    }
}

/// Build the full registry reference `{registry}:{network}-{subject_epoch}`.
pub fn build_ref(registry: &str, network: &str, subject_epoch: u64) -> String {
    format!("{registry}:{network}-{subject_epoch}")
}

/// On-disk directory name for a seed fixture: `{network}-{epoch}`.
pub fn seed_key(network: &str, epoch: u64) -> String {
    format!("{network}-{epoch}")
}

/// On-disk directory name for a ground-truth fixture: `{network}-{epoch}`.
pub fn ground_truth_key(network: &str, epoch: u64) -> String {
    format!("{network}-{epoch}")
}

/// On-disk directory name for an upstream fragment: `{network}-{start}-{end}`.
pub fn upstream_key(network: &str, start: u64, end: u64) -> String {
    format!("{network}-{start}-{end}")
}

/// Resolve the base directory where fixtures are read from / extracted to.
///
/// Precedence: `$DOLOS_FIXTURE_DIR` env var (matches what the test reads) →
/// `xtask.toml` `fixtures.local_dir` key. The env override is how CI points
/// at a runner-local path without touching the repo's `xtask.toml`.
pub fn resolve_local_dir(repo_root: &Path, config: &XtaskConfig) -> Result<PathBuf> {
    if let Ok(v) = std::env::var("DOLOS_FIXTURE_DIR") {
        if !v.is_empty() {
            return Ok(PathBuf::from(v));
        }
    }

    config
        .fixtures
        .local_dir
        .as_ref()
        .map(|p| resolve_path(repo_root, p))
        .context(
            "fixtures.local_dir not configured (set `[fixtures] local_dir` in xtask.toml \
             or export DOLOS_FIXTURE_DIR)",
        )
}
