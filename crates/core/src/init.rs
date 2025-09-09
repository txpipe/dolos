use tracing::warn;

use crate::{ArchiveStore, ChainPoint, Domain, DomainError, State3Store, StateStore, WalStore};

fn check_catch_up(
    wal: ChainPoint,
    archive: Option<ChainPoint>,
    state: Option<ChainPoint>,
    utxoset: Option<ChainPoint>,
) {
    if let Some(archive) = archive {
        if wal > archive {
            warn!(%archive, %wal,"catch up needed, wal is ahead of archive");
        }
    }

    if let Some(state) = state {
        if wal > state {
            warn!(%state, %wal, "catch up needed, wal is ahead of state");
        }
    }

    if let Some(utxoset) = utxoset {
        if wal > utxoset {
            warn!(%utxoset, %wal, "catch up needed, wal is ahead of utxoset");
        }
    }
}

pub fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    domain.wal().ensure_initialized()?;

    let wal = domain.wal().find_tip()?.map(|(point, _)| point);

    let Some(wal) = wal else {
        return Err(DomainError::WalIsEmpty);
    };

    let archive = domain
        .archive()
        .get_tip()?
        .map(|(slot, _)| ChainPoint::Slot(slot));

    let state = domain
        .state3()
        .read_cursor()?
        .map(|slot| ChainPoint::Slot(slot));

    let utxoset = domain.state().cursor()?;

    check_catch_up(wal, archive, state, utxoset);

    Ok(())
}
