use pallas::network::miniprotocols::blockfetch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::adapters::WalAdapter;
use crate::prelude::*;

async fn send_batch(
    wal: &WalAdapter,
    s1: LogSeq,
    s2: LogSeq,
    prot: &mut blockfetch::Server,
) -> Result<(), Error> {
    let iter = wal
        .crawl_range(s1, s2)
        .map_err(Error::server)?
        .filter_apply()
        .into_blocks()
        .flatten();

    prot.send_start_batch().await.map_err(Error::server)?;

    for RawBlock { body, .. } in iter {
        prot.send_block(body.to_vec())
            .await
            .map_err(Error::server)?;
    }

    prot.send_batch_done().await.map_err(Error::server)?;

    Ok(())
}

async fn process_request(
    wal: &WalAdapter,
    req: blockfetch::BlockRequest,
    prot: &mut blockfetch::Server,
) -> Result<(), Error> {
    let (p1, p2) = req.0;

    debug!(?p1, ?p2, "processing equest");

    let s1 = wal.locate_point(&p1.into()).map_err(Error::server)?;
    let s2 = wal.locate_point(&p2.into()).map_err(Error::server)?;

    match (s1, s2) {
        (Some(s1), Some(s2)) if s1 <= s2 => send_batch(wal, s1, s2, prot).await,
        _ => prot.send_no_blocks().await.map_err(Error::server),
    }
}

pub async fn process_requests(wal: WalAdapter, mut prot: blockfetch::Server) -> Result<(), Error> {
    while let Some(req) = prot.recv_while_idle().await.map_err(Error::server)? {
        process_request(&wal, req, &mut prot).await?;
    }

    Ok(())
}

pub async fn handle_session(
    wal: WalAdapter,
    connection: blockfetch::Server,
    cancel: CancellationToken,
) -> Result<(), Error> {
    tokio::select! {
        _ = process_requests(wal, connection) => {
            info!("peer ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("protocol was cancelled");
        }
    }

    Ok(())
}
