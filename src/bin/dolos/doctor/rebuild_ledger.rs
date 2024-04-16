use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

use dolos::{
    ledger::{ChainPoint, LedgerDelta},
    prelude::*,
};

use crate::common::open_data_stores;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    let (_, chain, mut ledger) = open_data_stores(config).context("opening data stores")?;

    if ledger.is_empty() {
        let byron_genesis =
            pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

        ledger
            .apply(&[dolos::ledger::compute_origin_delta(&byron_genesis)])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let cursor = ledger
        .cursor()
        .into_diagnostic()
        .context("finding ledger cursor")?;

    let remaining = chain.crawl_after(cursor.map(|x| x.0));

    for chunk in remaining.chunks(200).into_iter() {
        let deltas = chunk.map(|point| -> miette::Result<LedgerDelta> {
            let (_, hash) = point.into_diagnostic().context("crawling chain")?;

            let block = chain
                .get_block(hash)
                .into_diagnostic()
                .context("reading block")?
                .ok_or(miette::miette!("block not found"))?;

            let blockd = MultiEraBlock::decode(&block)
                .into_diagnostic()
                .context("decoding block cbor")?;

            Ok(dolos::ledger::compute_delta(&blockd))
        });

        let deltas: Vec<_> = deltas.try_collect()?;

        if let Some(last) = deltas.last() {
            if let Some(ChainPoint(slot, _)) = &last.new_position {
                debug!(slot, "importing block");
            }
        };

        ledger
            .apply(deltas.as_slice())
            .into_diagnostic()
            .context("applying ledger block")?;
    }

    Ok(())
}
