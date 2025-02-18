use pallas::ledger::traverse::wellknown::GenesisValues;
use rocket::routes;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

use crate::{mempool::Mempool, state::LedgerStore, wal::redb::WalStore};

mod common;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
}

pub async fn serve(
    cfg: Config,
    genesis: GenesisValues,
    wal: WalStore,
    ledger: LedgerStore,
    mempool: Mempool,
    _exit: CancellationToken,
) -> Result<(), rocket::Error> {
    // TODO: connect cancellation token to rocket shutdown

    // let shutdown = rocket::config::Shutdown {
    //     ctrlc: false,
    //     signals: std::collections::HashSet::new(),
    //     force: true,
    //     ..Default::default()
    // };

    let _ = rocket::build()
        .configure(
            rocket::Config::figment()
                .merge(("address", cfg.listen_address.ip().to_string()))
                .merge(("port", cfg.listen_address.port())),
        )
        .manage(genesis)
        .manage(wal)
        .manage(ledger)
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
                //Epoch
                // Submit
                routes::tx::submit::route,
            ],
        )
        .launch()
        .await?;

    Ok(())
}
