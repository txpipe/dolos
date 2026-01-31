use anyhow::{Context, Result};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn dump_eras(config_path: &Path, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing eras csv: {}", output_path.display()))?;

    let status = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("dolos")
        .arg("--features")
        .arg("utils")
        .arg("--")
        .arg("data")
        .arg("dump-state")
        .arg("--namespace")
        .arg("eras")
        .arg("--format")
        .arg("dbsync")
        .arg("--count")
        .arg("0")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .context("running dolos dump-state for eras")?;

    if !status.success() {
        anyhow::bail!("dolos dump-state failed for eras");
    }

    Ok(())
}

pub fn dump_epochs(config_path: &Path, stop_epoch: u64, output_path: &Path) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("writing epochs csv: {}", output_path.display()))?;

    let status = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("dolos")
        .arg("--features")
        .arg("utils")
        .arg("--")
        .arg("data")
        .arg("dump-logs")
        .arg("--namespace")
        .arg("epochs")
        .arg("--format")
        .arg("dbsync")
        .arg("--epoch-start")
        .arg("1")
        .arg("--epoch-end")
        .arg(stop_epoch.to_string())
        .arg("--take")
        .arg("0")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .context("running dolos dump-logs for epochs")?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for epochs");
    }

    Ok(())
}

pub fn dump_rewards(
    config_path: &Path,
    earned_epoch: u64,
    output_path: &Path,
) -> Result<()> {
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

    let status = Command::new("cargo")
        .arg("run")
        .arg("-p")
        .arg("dolos")
        .arg("--features")
        .arg("utils")
        .arg("--")
        .arg("data")
        .arg("dump-logs")
        .arg("--namespace")
        .arg("rewards")
        .arg("--format")
        .arg("dbsync")
        .arg("--epoch-start")
        .arg(log_epoch_start.to_string())
        .arg("--epoch-end")
        .arg(log_epoch_end.to_string())
        .arg("--take")
        .arg("0")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .context("running dolos dump-logs for rewards")?;

    if !status.success() {
        anyhow::bail!("dolos dump-logs failed for rewards");
    }

    Ok(())
}

pub fn delegation_csv_path(dumps_dir: &Path, epoch: u64) -> PathBuf {
    dumps_dir.join(format!("delegation-{}.csv", epoch))
}

pub fn stake_csv_path(dumps_dir: &Path, epoch: u64) -> PathBuf {
    dumps_dir.join(format!("stake-{}.csv", epoch))
}
