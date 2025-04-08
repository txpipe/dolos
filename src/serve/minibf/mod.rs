use futures_util::{future::try_join, TryFutureExt};
use rocket::routes;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::chain::ChainStore;
use crate::prelude::Error;
use crate::{ledger::pparams::Genesis, mempool::Mempool, state::LedgerStore};

mod common;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
}

pub async fn serve(
    cfg: Config,
    genesis: Arc<Genesis>,
    ledger: LedgerStore,
    chain: ChainStore,
    mempool: Mempool,
    exit: CancellationToken,
) -> Result<(), Error> {
    let rocket = rocket::build()
        .configure(
            rocket::Config::figment()
                .merge(("address", cfg.listen_address.ip().to_string()))
                .merge(("port", cfg.listen_address.port()))
                .merge((
                    "shutdown",
                    rocket::config::Shutdown {
                        ctrlc: false,
                        signals: std::collections::HashSet::new(),
                        force: true,
                        ..Default::default()
                    },
                )),
        )
        .manage(genesis)
        .manage(ledger)
        .manage(chain)
        .manage(mempool)
        .mount(
            "/",
            routes![
                // Accounts
                routes::accounts::stake_address::utxos::route,
                // Addresses
                routes::addresses::address::utxos::route,
                routes::addresses::address::utxos::asset::route,
                // Blocks
                routes::blocks::latest::route,
                routes::blocks::latest::txs::route,
                routes::blocks::hash_or_number::route,
                routes::blocks::hash_or_number::addresses::route,
                routes::blocks::hash_or_number::next::route,
                routes::blocks::hash_or_number::previous::route,
                routes::blocks::hash_or_number::txs::route,
                routes::blocks::slot::slot_number::route,
                // Epoch
                routes::epochs::latest::parameters::route,
                // Submit
                routes::tx::submit::route,
                // Transactions
                routes::txs::tx_hash::cbor::route,
            ],
        )
        .ignite()
        .await
        .map_err(Error::server)?;

    let shutdown = rocket.shutdown();
    let cancellation = async {
        exit.cancelled().await;
        info!("Gracefully shuting down minibf.");
        shutdown.notify();
        Ok::<(), Error>(())
    };
    let server = async {
        rocket.launch().map_err(Error::server).await?;
        Ok(())
    };

    try_join(server, cancellation).await?;

    Ok(())
}
