use tracing::warn;

use crate::{ArchiveStore, ChainLogic, ChainPoint, Domain, DomainError, StateStore, WalStore};

pub fn catch_up<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let wal = domain
        .wal()
        .find_tip()?
        .map(|(point, _)| point)
        .ok_or(DomainError::WalIsEmpty)?;

    let state = domain.state().read_cursor()?.unwrap_or(ChainPoint::Origin);

    let archive = domain
        .archive()
        .get_tip()?
        .map(|(slot, _)| ChainPoint::Slot(slot))
        .unwrap_or(ChainPoint::Origin);

    if wal > archive {
        warn!(%archive, %wal,"catch up needed, wal is ahead of archive");
    }

    if wal > state {
        warn!(%state, %wal, "catch up needed, wal is ahead of state");
    }

    Ok(())
}

fn ensure_wal<D: Domain>(domain: &D, at: &ChainPoint) -> Result<(), DomainError> {
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);

    if let Some(wal) = wal {
        if wal.slot() < at.slot() {
            domain.wal().reset_to(&at)?;
        }
    } else {
        domain.wal().reset_to(&at)?;
    }

    Ok(())
}

fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let state = domain.state().read_cursor()?.unwrap_or(ChainPoint::Origin);

    ensure_wal(domain, &state)?;

    Ok(())
}

fn is_empty<D: Domain>(domain: &D) -> Result<bool, DomainError> {
    let out = domain.state().read_cursor()?.is_none();

    Ok(out)
}

pub fn ensure_bootstrap<D: Domain>(domain: &D) -> Result<(), DomainError> {
    if !is_empty(domain)? {
        tracing::debug!("skipping bootstrap, data is not empty");
        return Ok(());
    }

    domain.chain().bootstrap(domain)?;

    domain.wal().reset_to(&ChainPoint::Origin)?;

    Ok(())
}

pub fn ensure_initialized<D: Domain>(domain: &D) -> Result<(), DomainError> {
    ensure_bootstrap(domain)?;

    check_integrity(domain)?;

    catch_up(domain)?;

    Ok(())
}
