use rocket::{self, get, routes};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;

use crate::state::LedgerStore;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
}

pub async fn serve(
    cfg: Config,
    ledger: LedgerStore,
    exit: CancellationToken,
) -> Result<(), rocket::Error> {
    let rocket = rocket::build()
        .configure(
            rocket::Config::figment()
                .merge(("address", cfg.listen_address.ip().to_string()))
                .merge(("port", cfg.listen_address.port())),
        )
        .state(ledger)
        .mount("/", routes![health_check])
        .launch()
        .await?;

    Ok(())
}

#[get("/health")]
fn health_check() -> &'static str {
    "OK"
}
