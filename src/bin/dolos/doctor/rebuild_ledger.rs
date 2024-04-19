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
        debug!("importing genesis");

        let byron_genesis =
            pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

        let delta = dolos::ledger::compute_origin_delta(&byron_genesis);

        ledger
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let cursor = ledger
        .cursor()
        .into_diagnostic()
        .context("finding ledger cursor")?;

    let remaining = chain.crawl_after(cursor.map(|x| x.0));

    for point in remaining {
        let (slot, hash) = point.into_diagnostic().context("crawling chain")?;
        debug!(slot, "importing block");

        let block = chain
            .get_block(hash)
            .into_diagnostic()
            .context("reading block")?
            .ok_or(miette::miette!("block not found"))?;

        let blockd = MultiEraBlock::decode(&block)
            .into_diagnostic()
            .context("decoding block cbor")?;

        let context = dolos::ledger::load_slice_for_block(&blockd, &ledger)
            .into_diagnostic()
            .context("loading block context")?;

        let delta = dolos::ledger::compute_delta(&blockd, context)
            .into_diagnostic()
            .context("computing ledger delta for block")?;

        ledger
            .apply(&[delta])
            .into_diagnostic()
            .context("applying ledger block")?;
    }

    Ok(())
}
