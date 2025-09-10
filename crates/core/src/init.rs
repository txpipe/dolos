use tracing::warn;

use crate::{
    ArchiveStore, ChainLogic, ChainPoint, Domain, DomainError, State3Store, StateStore, WalStore,
};

pub fn catch_up<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let wal = domain
        .wal()
        .find_tip()?
        .map(|(point, _)| point)
        .ok_or(DomainError::WalIsEmpty)?;

    let state = domain
        .state3()
        .read_cursor()?
        .map(|slot| ChainPoint::Slot(slot))
        .unwrap_or(ChainPoint::Origin);

    let utxoset = domain.state().cursor()?.unwrap_or(ChainPoint::Origin);

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

    if wal > utxoset {
        warn!(%utxoset, %wal, "catch up needed, wal is ahead of utxoset");
    }

    Ok(())
}

fn ensure_wal<D: Domain>(domain: &D, at: &ChainPoint) -> Result<ChainPoint, DomainError> {
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);

    if let Some(wal) = wal {
        return Ok(wal);
    }

    domain.wal().reset_to(&at)?;

    Ok(at.clone())
}

fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let state = domain
        .state3()
        .read_cursor()?
        .map(|slot| ChainPoint::Slot(slot))
        .unwrap_or(ChainPoint::Origin);

    let wal = ensure_wal(domain, &state)?;

    if wal.slot() < state.slot() {
        return Err(DomainError::WalIsBehindState(wal.slot(), state.slot()));
    }

    Ok(())
}

fn is_empty<D: Domain>(domain: &D) -> Result<bool, DomainError> {
    let utxoset = domain.state().is_empty()?;
    let state = domain.state3().read_cursor()?.is_none();

    if utxoset != state {
        return Err(DomainError::InconsistentState(
            "utxoset and state are inconsistent".to_string(),
        ));
    }

    Ok(utxoset && state)
}

pub fn ensure_bootstrap<D: Domain>(domain: &D) -> Result<(), DomainError> {
    if !is_empty(domain)? {
        dbg!("skipping bootstrap, data is not empty");
        tracing::debug!("skipping bootstrap, data is not empty");
        return Ok(());
    }

    domain.chain().bootstrap(domain)?;

    dbg!("chain bootstrap passed");

    domain.wal().reset_to(&ChainPoint::Origin)?;

    dbg!("wal reset to origin");

    Ok(())
}

pub fn ensure_initialized<D: Domain>(domain: &D) -> Result<(), DomainError> {
    ensure_bootstrap(domain)?;

    dbg!("bootstrap passed");

    check_integrity(domain)?;

    dbg!("integrity check passed");

    catch_up(domain)?;

    dbg!("initialized");

    Ok(())
}
