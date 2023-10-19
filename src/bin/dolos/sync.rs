use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(
    config: &super::Config,
    policy: &gasket::runtime::Policy,
    _args: &Args,
) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, chain, ledger) = crate::common::open_data_stores(config)?;

    let byron_genesis =
        pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

    dolos::sync::pipeline(&config.upstream, wal, chain, ledger, byron_genesis, policy)
        .unwrap()
        .block();

    Ok(())
}
