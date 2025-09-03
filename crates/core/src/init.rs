use tracing::warn;

use crate::{ArchiveStore, ChainPoint, Domain, DomainError, StateStore, WalStore};

fn check_catch_up(
    wal: Option<ChainPoint>,
    archive: Option<ChainPoint>,
    ledger: Option<ChainPoint>,
) {
    if let Some(wal) = wal {
        if let Some(archive) = archive {
            if wal > archive {
                warn!(%archive, %wal,"catch up needed, wal is ahead of archive");
            }
        }

        if let Some(ledger) = ledger {
            if wal > ledger {
                warn!(%ledger, %wal, "catch up needed, wal is ahead of ledger");
            }
        }
    }
}

pub fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    domain.wal().ensure_initialized()?;

    let wal = domain.wal().find_tip()?.map(|(point, _)| point);

    let archive = domain
        .archive()
        .get_tip()?
        .map(|(slot, _)| ChainPoint::Slot(slot));

    let ledger = domain.state().cursor()?;

    check_catch_up(wal, archive, ledger);

    Ok(())
}
