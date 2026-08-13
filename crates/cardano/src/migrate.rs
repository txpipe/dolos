//! One-time state migrations applied at node initialization.
//!
//! These self-heal stores bootstrapped before an invariant existed, the
//! same way `check_wal_in_sync_with_state` self-heals an empty WAL. Each
//! migration is idempotent and keyed on the data shape itself (no store
//! version stamp exists), so running them on every startup is a no-op
//! after the first time.

use dolos_core::{ChainError, Domain, StateStore as _, StateWriter as _};
use tracing::warn;

use crate::{load_era_summary, read_singleton, GovState, SingletonEntity as _};

/// CARDANO-006: create the governance singleton on stores bootstrapped
/// before its existence invariant (genesis now seeds the row; older
/// stores and published snapshots miss it).
///
/// The enact-state fields stay unset — for a store already past the
/// Chang boundary the constitution, committee and cert history since the
/// fork were never recorded, and only a fresh sync recovers them. Only
/// `active_since` is derived, from the era summary.
///
/// Must run before WAL catch-up: replayed governance deltas expect the
/// row to exist. No-op on fresh stores (no cursor yet — genesis
/// bootstrap seeds the row) and on stores that already carry the row.
/// Returns whether a migration write happened.
pub(crate) fn ensure_gov_singleton<D: Domain>(state: &D::State) -> Result<bool, ChainError> {
    if state.read_cursor()?.is_none() {
        return Ok(false);
    }

    if read_singleton::<D, GovState>(state)?.is_some() {
        return Ok(false);
    }

    let summary = load_era_summary::<D>(state)?;

    let gov = GovState {
        active_since: summary.first_conway_epoch(),
        ..Default::default()
    };

    warn!(
        active_since = ?gov.active_since,
        "CARDANO-006: governance singleton missing on bootstrapped store; creating it (enact-state unknown)"
    );

    let writer = state.start_writer()?;
    writer.write_entity_typed(&GovState::singleton_key(), &gov)?;
    writer.commit()?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use dolos_core::{Domain as _, StateStore as _};
    use dolos_testing::toy_domain::ToyDomain;
    use std::sync::Arc;

    use crate::FixedNamespace as _;

    use super::*;

    fn read_gov(domain: &ToyDomain) -> Option<GovState> {
        domain
            .state()
            .read_entity_typed::<GovState>(GovState::NS, &GovState::singleton_key())
            .unwrap()
    }

    fn delete_gov(domain: &ToyDomain) {
        let writer = domain.state().start_writer().unwrap();
        writer
            .delete_entity(GovState::NS, &GovState::singleton_key())
            .unwrap();
        writer.commit().unwrap();
    }

    #[test]
    fn migration_recreates_missing_row_on_conway_store() {
        // devnet force-starts at protocol 9, so its era summary is Conway
        // from epoch 0
        let domain = ToyDomain::new(None, None);
        delete_gov(&domain);

        let migrated = ensure_gov_singleton::<ToyDomain>(domain.state()).unwrap();

        assert!(migrated);
        let gov = read_gov(&domain).expect("migration created the row");
        assert_eq!(gov.active_since, Some(0));
        assert_eq!(gov.constitution, None);
        assert_eq!(gov.committee, None);
    }

    #[test]
    fn migration_creates_inactive_row_on_pre_conway_store() {
        let mut genesis = crate::include::devnet::load();
        genesis.force_protocol = Some(8);
        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);
        delete_gov(&domain);

        let migrated = ensure_gov_singleton::<ToyDomain>(domain.state()).unwrap();

        assert!(migrated);
        let gov = read_gov(&domain).expect("migration created the row");
        assert_eq!(gov, GovState::default());
    }

    #[test]
    fn migration_is_noop_when_row_exists() {
        let domain = ToyDomain::new(None, None);
        let before = read_gov(&domain).expect("bootstrap seeds the row");

        let migrated = ensure_gov_singleton::<ToyDomain>(domain.state()).unwrap();

        assert!(!migrated);
        assert_eq!(read_gov(&domain), Some(before));
    }

    #[test]
    fn migration_is_noop_before_genesis() {
        // a store with no cursor: genesis hasn't run, bootstrap will seed
        let state = dolos_testing::toy_domain::empty_state_store();

        let migrated = ensure_gov_singleton::<ToyDomain>(&state).unwrap();

        assert!(!migrated);
        assert_eq!(
            state
                .read_entity_typed::<GovState>(GovState::NS, &GovState::singleton_key())
                .unwrap(),
            None
        );
    }
}
