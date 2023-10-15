use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(
    config: &super::Config,
    policy: &gasket::runtime::Policy,
    _args: &Args,
) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    let (wal, chain, ledger) = crate::common::open_data_stores(config)?;

    let byron_genesis =
        pallas::ledger::configs::byron::from_file(&config.byron.path).map_err(Error::config)?;

    dolos::sync::pipeline(&config.upstream, wal, chain, ledger, byron_genesis, policy)
        .unwrap()
        .block();

    Ok(())
}
