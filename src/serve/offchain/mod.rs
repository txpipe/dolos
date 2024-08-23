use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_util::sync::CancellationToken;
use warp::Filter as _;

use crate::prelude::*;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_address: String,
}

#[derive(Deserialize)]
struct Request {
    pub id: Option<String>,
    pub method: String,
    pub params: serde_json::Value,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn parse_request(body: serde_json::Value) -> Result<Request, ErrorResponse> {
    match serde_json::from_value(body) {
        Ok(x) => Ok(x),
        Err(x) => Err(ErrorResponse {
            error: x.to_string(),
        }),
    }
}

pub async fn serve(
    config: Config,
    runtime: crate::balius::Runtime,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let filter = warp::path::param()
        .and(warp::post())
        .and(warp::body::json())
        .map(move |worker: String, body: serde_json::Value| {
            let request = match parse_request(body) {
                Ok(x) => x,
                Err(err) => return warp::reply::json(&err),
            };

            let reply = runtime.handle_request(&worker, &request.method, request.params);

            match reply {
                Ok(x) => warp::reply::json(&x),
                Err(err) => warp::reply::json(&ErrorResponse {
                    error: err.to_string(),
                }),
            }
        });

    let address: SocketAddr = config.listen_address.parse().map_err(Error::config)?;

    let (addr, server) =
        warp::serve(filter).bind_with_graceful_shutdown(address, cancel.cancelled_owned());

    tracing::info!(%addr, "offchain request listening");

    server.await;

    Ok(())
}
