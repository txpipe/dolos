//! `cargo xtask compat`: forward-upgrade fixtures for the compatibility
//! harness (`tests/compat_fixtures.rs`).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use xshell::{cmd, Shell};

#[derive(Debug, Subcommand)]
pub enum Cmd {
    /// Write a fixture: a configuration, a short-chain dataset and the golden
    /// output of every supported command, under `<out>/<label>`
    Fixture(FixtureArgs),
}

#[derive(Debug, Args)]
pub struct FixtureArgs {
    /// The fixture's directory name, `v<major>.<minor>` for a release
    pub label: String,

    /// The directory the fixture is written into
    #[arg(long, default_value = dolos_testing::compat::ARCHIVE_DIR)]
    pub out: PathBuf,

    /// The `dolos` binary to record; without it, `cargo build --locked --bin
    /// dolos` builds this checkout's
    #[arg(long)]
    pub bin: Option<PathBuf>,
}

pub fn run(sh: &Shell, cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Fixture(args) => fixture(sh, &args),
    }
}

fn fixture(sh: &Shell, args: &FixtureArgs) -> Result<()> {
    let root = PathBuf::from(
        cmd!(sh, "git rev-parse --show-toplevel")
            .read()
            .context("finding the checkout")?,
    );
    sh.change_dir(&root);
    refuse_another_build(&root)?;

    let binary = match &args.bin {
        Some(binary) => binary.clone(),
        None => {
            cmd!(sh, "cargo build --locked --bin dolos").run()?;
            target_dir(&root)
                .join("debug")
                .join(format!("dolos{}", std::env::consts::EXE_SUFFIX))
        }
    };

    if !binary.is_file() {
        bail!("no dolos binary at {}", binary.display());
    }

    let commit = cmd!(sh, "git describe --always --dirty --abbrev=40")
        .read()
        .context("reading the commit")?;
    let out = crate::util::resolve_path(&root, &args.out).join(&args.label);

    dolos_testing::compat::generate(&binary, &out, &commit)
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("writing the fixture at {}", out.display()))?;

    println!("wrote {}", out.display());

    Ok(())
}

/// The goldens, the configuration tables and `config.json`'s meaning are
/// compiled into this binary, so only an xtask `cargo run` has just built
/// from the checkout is known to hold the checkout's. An installed helper
/// stays at the commit it was built from, and would archive that commit's
/// fixture under this one's name.
fn refuse_another_build(root: &Path) -> Result<()> {
    let canonical = |dir: &Path| dir.canonicalize().ok();
    let built = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);

    if built.as_deref().and_then(canonical) != canonical(&root.join("xtask")) {
        bail!(
            "this xtask was not built by `cargo run` from the checkout at {}; run `cargo run \
             --locked -p xtask -- compat fixture …` there instead",
            root.display()
        );
    }

    Ok(())
}

fn target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map_or_else(
            || root.join("target"),
            |dir| crate::util::resolve_path(root, &dir),
        )
}
