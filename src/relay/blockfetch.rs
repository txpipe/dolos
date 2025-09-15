use pallas::network::miniprotocols::blockfetch;
use tracing::{debug, info};

use crate::prelude::*;

async fn send_batch<W: WalStore>(
    wal: &W,
    s1: ChainPoint,
    s2: ChainPoint,
    prot: &mut blockfetch::Server,
) -> Result<(), Error> {
    let iter = wal.iter_blocks(Some(s1), Some(s2)).map_err(Error::server)?;

    prot.send_start_batch().await.map_err(Error::server)?;

    for (_, body) in iter {
        prot.send_block((*body).clone())
            .await
            .map_err(Error::server)?;
    }

    prot.send_batch_done().await.map_err(Error::server)?;

    Ok(())
}

async fn process_request<W: WalStore>(
    wal: &W,
    req: blockfetch::BlockRequest,
    prot: &mut blockfetch::Server,
) -> Result<(), Error> {
    let (p1, p2) = req.0;

    debug!(?p1, ?p2, "processing equest");

    let p1 = ChainPoint::from(p1);
    let p2 = ChainPoint::from(p2);

    let ok1 = wal.contains_point(&p1).map_err(Error::server)?;
    let ok2 = wal.contains_point(&p2).map_err(Error::server)?;

    if !ok1 || !ok2 {
        return prot.send_no_blocks().await.map_err(Error::server);
    }

    if p1.slot() > p2.slot() {
        return prot.send_no_blocks().await.map_err(Error::server);
    }

    send_batch(wal, p1, p2, prot).await
}

pub async fn process_requests<W: WalStore>(
    wal: W,
    mut prot: blockfetch::Server,
) -> Result<(), Error> {
    while let Some(req) = prot.recv_while_idle().await.map_err(Error::server)? {
        process_request(&wal, req, &mut prot).await?;
    }

    Ok(())
}

pub async fn handle_session<W: WalStore, C: CancelToken>(
    wal: W,
    connection: blockfetch::Server,
    cancel: C,
) -> Result<(), ServeError> {
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
