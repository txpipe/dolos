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
    let root = workspace_root();
    let binary = match &args.bin {
        Some(binary) => binary.clone(),
        None => {
            sh.change_dir(&root);
            cmd!(sh, "cargo build --locked --bin dolos").run()?;
            target_dir(&root)
                .join("debug")
                .join(format!("dolos{}", std::env::consts::EXE_SUFFIX))
        }
    };

    if !binary.is_file() {
        bail!("no dolos binary at {}", binary.display());
    }

    sh.change_dir(&root);
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

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask sits in the workspace")
        .to_path_buf()
}

fn target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map_or_else(
            || root.join("target"),
            |dir| crate::util::resolve_path(root, &dir),
        )
}
