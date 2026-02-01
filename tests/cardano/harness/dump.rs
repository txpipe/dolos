use anyhow::{Context, Result};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

/// Build the dolos binary once and return the path to it.
fn dolos_binary() -> &'static PathBuf {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let status = Command::new("cargo")
            .arg("build")
            .arg("-p")
            .arg("dolos")
            .arg("--features")
            .arg("utils")
            .stderr(Stdio::inherit())
            .status()
            .expect("running cargo build");

        assert!(status.success(), "cargo build failed");

        let output = Command::new("cargo")
            .arg("metadata")
            .arg("--format-version=1")
            .arg("--no-deps")
            .output()
            .expect("cargo metadata");

        let metadata: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parsing cargo metadata");
        let target_dir = metadata["target_directory"]
            .as_str()
            .expect("target_directory in metadata");

        PathBuf::from(target_dir).join("debug").join("dolos")
    })
}

/// Run a dolos command with CWD set to the instance directory (so relative
/// paths in dolos.toml resolve correctly).
fn run_dolos(config_path: &Path, stdout: File, args: &[&str]) -> Result<std::process::ExitStatus> {
    let instance_dir = config_path.parent().expect("config has parent dir");
    let binary = dolos_binary();

    Command::new(binary)
        .current_dir(instance_dir)
        .arg("--config")
        .arg(config_path)
        .args(args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::inherit())
        .status()
        .context("running dolos")
}

pub fn dump_eras(config_path: &Path, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing eras csv: {}", output_path.display()))?;

    let status = run_dolos(
        config_path,
        file,
        &[
            "data", "dump-state", "--namespace", "eras", "--format", "dbsync", "--count", "0",
        ],
    )?;

    if !status.success() {
        anyhow::bail!("dolos dump-state failed for eras");
    }

    Ok(())
}

pub fn dump_epochs(config_path: &Path, stop_epoch: u64, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing epochs csv: {}", output_path.display()))?;

    let stop_str = stop_epoch.to_string();
    let status = run_dolos(
        config_path,
        file,
        &[
            "data",
            "dump-logs",
            "--namespace",
            "epochs",
            "--format",
            "dbsync",
            "--epoch-start",
            "1",
            "--epoch-end",
            &stop_str,
            "--take",
            "0",
        ],
    )?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for epochs");
    }

    Ok(())
}

pub fn dump_rewards(config_path: &Path, earned_epoch: u64, output_path: &Path) -> Result<()> {
    let log_epoch_start = earned_epoch
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("epoch overflow"))?;
    let log_epoch_end = log_epoch_start
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("epoch overflow"))?;

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating reward csv dir: {}", parent.display()))?;
    }

    let file = File::create(output_path)
        .with_context(|| format!("writing rewards csv: {}", output_path.display()))?;

    let start_str = log_epoch_start.to_string();
    let end_str = log_epoch_end.to_string();
    let status = run_dolos(
        config_path,
        file,
        &[
            "data",
            "dump-logs",
            "--namespace",
            "rewards",
            "--format",
            "dbsync",
            "--epoch-start",
            &start_str,
            "--epoch-end",
            &end_str,
            "--take",
            "0",
        ],
    )?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for rewards");
    }

    Ok(())
}

pub fn dump_pparams(config_path: &Path, stop_epoch: u64, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing pparams csv: {}", output_path.display()))?;

    let stop_str = stop_epoch.to_string();
    let status = run_dolos(
        config_path,
        file,
        &[
            "data",
            "dump-logs",
            "--namespace",
            "epochs/pparams",
            "--format",
            "dbsync",
            "--epoch-start",
            "1",
            "--epoch-end",
            &stop_str,
            "--take",
            "0",
        ],
    )?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for pparams");
    }

    Ok(())
}

pub fn delegation_csv_path(dumps_dir: &Path, epoch: u64) -> PathBuf {
    dumps_dir.join(format!("delegation-{}.csv", epoch))
}

pub fn stake_csv_path(dumps_dir: &Path, epoch: u64) -> PathBuf {
    dumps_dir.join(format!("stake-{}.csv", epoch))
}
