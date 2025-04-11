use std::sync::Arc;

use dolos::ledger::pparams::Genesis;
use miette::{bail, Context, IntoDiagnostic};
use pallas::ledger::addresses::Address;
use tracing::warn;

use crate::init;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Amount of seconds between block production.
    #[arg(long, default_value_t = 20)]
    block_production_interval: u64,

    /// Amount of slots of history to maintain in memory
    #[arg(long, default_value_t = 20)]
    history: usize,

    /// Initial funds in the form of address=amount
    #[arg(long, value_parser = parse_address_funds)]
    initial_funds: Vec<(Address, u64)>,

    /// Port where to serve GRPC.
    #[arg(long, default_value_t = 50051)]
    grpc_port: u64,

    /// Port where to serve MiniBF.
    #[arg(long, default_value_t = 3000)]
    minibf_port: u64,

    /// Port where to serve TRP.
    #[arg(long, default_value_t = 8000)]
    trp_port: u64,
}

fn parse_address_funds(s: &str) -> miette::Result<(Address, u64)> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        bail!("Invalid initial funds")
    }

    let address = Address::from_bech32(parts[0])
        .into_diagnostic()
        .context("invalid bech32 address")?;
    let amount = parts[1]
        .parse::<u64>()
        .into_diagnostic()
        .context("invalid bech32 address")?;

    Ok((address, amount))
}

#[tokio::main]
pub async fn run(args: &Args) -> miette::Result<()> {
    let config = crate::Config {
        upstream: dolos::prelude::UpstreamConfig::Emulator(dolos::prelude::EmulatorConfig {
            block_production_interval: args.block_production_interval,
        }),
        storage: dolos::prelude::StorageConfig {
            version: dolos::prelude::StorageVersion::V1,
            path: None,
            ..Default::default()
        },
        genesis: Default::default(),
        sync: Default::default(),
        submit: Default::default(),
        serve: dolos::serve::Config {
            grpc: Some(dolos::serve::grpc::Config {
                listen_address: format!("[::]:{}", args.grpc_port),
                tls_client_ca_root: None,
                permissive_cors: Some(true),
            }),
            minibf: Some(dolos::serve::minibf::Config {
                listen_address: format!("[::]:{}", args.minibf_port).parse().unwrap(),
            }),
            ouroboros: None,
        },
        relay: Default::default(),
        retries: Default::default(),
        mithril: None,
        snapshot: None,
        logging: Default::default(),
    };

    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger, chain) = crate::common::open_data_stores(&config)?;
    let byron = serde_json::from_slice(init::include::preview::BYRON).unwrap();
    let alonzo = serde_json::from_slice(init::include::preview::ALONZO).unwrap();
    let conway = serde_json::from_slice(init::include::preview::CONWAY).unwrap();
    let mut shelley: pallas::ledger::configs::shelley::GenesisFile =
        serde_json::from_slice(init::include::preview::SHELLEY).unwrap();
    shelley.initial_funds = Some(
        args.initial_funds
            .clone()
            .into_iter()
            .map(|(address, amount)| (address.to_hex(), amount))
            .collect(),
    );

    let genesis = Arc::new(Genesis {
        byron,
        shelley,
        alonzo,
        conway,
        force_protocol: Some(9),
    });
    let mempool = dolos::mempool::Mempool::new(genesis.clone(), ledger.clone());
    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        &config.storage,
        wal.clone(),
        ledger.clone(),
        chain.clone(),
        genesis.clone(),
        mempool.clone(),
        &config.retries,
        false,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let sync = crate::common::spawn_pipeline(gasket::daemon::Daemon::new(sync), exit.clone());

    // TODO: spawn submit pipeline. Skipping for now since it's giving more trouble
    // that benefits

    // We need new file handled for the separate process.
    let serve = tokio::spawn(dolos::serve::serve(
        config.serve,
        genesis.clone(),
        wal.clone(),
        ledger.clone(),
        chain.clone(),
        mempool.clone(),
        exit.clone(),
    ));

    let relay = tokio::spawn(dolos::relay::serve(config.relay, wal.clone(), exit.clone()));

    let (_, serve, relay) = tokio::try_join!(sync, serve, relay)
        .into_diagnostic()
        .context("joining threads")?;

    serve.context("serve thread")?;
    relay.into_diagnostic().context("relay thread")?;

    warn!("shutdown complete");

    Ok(())
}
