use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};
use tracing::info;

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {}

/// Resolve a fully-defined chain point for WAL reset.
///
/// If the state cursor already has a hash, use it directly. Otherwise, fall back
/// to the archive tip block to obtain the hash (the state cursor from ESTART
/// commits `ChainPoint::Slot(Y)` without a hash).
fn resolve_reset_point(domain: &dolos::adapters::DomainAdapter) -> miette::Result<ChainPoint> {
    let cursor = domain
        .state
        .read_cursor()
        .into_diagnostic()
        .context("getting state cursor")?;

    let Some(cursor) = cursor else {
        return Err(miette::miette!("state has no cursor, nothing to reset to"));
    };

    if cursor.is_fully_defined() {
        info!(%cursor, "state cursor has hash, using directly");
        return Ok(cursor);
    }

    info!(%cursor, "state cursor has no hash, resolving from archive tip");

    let tip = domain
        .archive()
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?;

    let Some((slot, body)) = tip else {
        return Err(miette::miette!(
            "state cursor at slot {} has no hash and archive is empty — cannot resolve a valid point",
            cursor.slot()
        ));
    };

    let block = pallas::ledger::traverse::MultiEraBlock::decode(&body)
        .map_err(|e| miette::miette!(e.to_string()))
        .context("decoding archive tip block")?;

    let point = ChainPoint::Specific(slot, block.hash());
    info!(%point, "resolved point from archive tip");
    Ok(point)
}

#[tokio::main]
pub async fn run(config: &RootConfig, _args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let domain = crate::common::setup_domain(config)?;

    let point = resolve_reset_point(&domain)?;

    domain
        .wal()
        .reset_to(&point)
        .into_diagnostic()
        .context("resetting wal")?;

    info!(%point, "WAL reset successfully");

    Ok(())
}
