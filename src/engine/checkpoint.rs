use dolos_core::{BlockSlot, ChainPoint, StateError, StateStore, WalError, WalStore};

#[derive(Debug, thiserror::Error)]
pub(super) enum CheckpointError {
    #[error("reading replay position")]
    ReadState(#[source] StateError),
    #[error("reading replay checkpoint")]
    ReadWal(#[source] WalError),
    #[error("state cursor at slot {slot} has no block hash")]
    UnanchoredState { slot: BlockSlot },
    #[error("checkpoint {wal} and state {state} disagree")]
    Diverged { wal: ChainPoint, state: ChainPoint },
    #[error("checkpoint {wal} exists but state has no cursor")]
    MissingState { wal: ChainPoint },
    #[error("persisting replay checkpoint")]
    ResetWal(#[source] WalError),
}

pub(super) fn reconcile<S: StateStore, W: WalStore>(
    state: &S,
    wal: &W,
) -> Result<(), CheckpointError> {
    let position = state.read_cursor().map_err(CheckpointError::ReadState)?;
    let tip = wal
        .find_tip()
        .map_err(CheckpointError::ReadWal)?
        .map(|(point, _)| point);

    if let Some(position) = position.as_ref() {
        if !position.is_fully_defined() {
            return Err(CheckpointError::UnanchoredState {
                slot: position.slot(),
            });
        }
    }

    match (tip, position) {
        (None, None) => Ok(()),
        (Some(ChainPoint::Origin), None) => Ok(()),
        (Some(wal), None) => Err(CheckpointError::MissingState { wal }),
        (Some(wal), Some(state)) if wal == state => Ok(()),
        (Some(wal), Some(state)) if wal.slot() == state.slot() => {
            Err(CheckpointError::Diverged { wal, state })
        }
        (Some(wal), Some(state)) if wal.slot() > state.slot() => Ok(()),
        (_, Some(position)) => wal.reset_to(&position).map_err(CheckpointError::ResetWal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::{Domain as _, StateWriter as _};
    use dolos_testing::toy_domain::ToyDomain;

    fn set_position(domain: &ToyDomain, position: ChainPoint) {
        let writer = domain.state().start_writer().unwrap();
        writer.set_cursor(position).unwrap();
        writer.commit().unwrap();
    }

    fn tip(domain: &ToyDomain) -> Option<ChainPoint> {
        domain.wal().find_tip().unwrap().map(|(point, _)| point)
    }

    #[test]
    fn missing_or_stale_checkpoint_is_reconciled_without_moving_state() {
        for previous in [None, Some(ChainPoint::Specific(5, [1; 32].into()))] {
            let domain = ToyDomain::new(None, None);
            if let Some(previous) = previous {
                domain.wal().reset_to(&previous).unwrap();
            }
            let position = ChainPoint::Specific(10, [2; 32].into());
            set_position(&domain, position.clone());
            reconcile(domain.state(), domain.wal()).unwrap();
            assert_eq!(tip(&domain), Some(position.clone()));
            assert_eq!(
                domain.state().read_cursor().unwrap(),
                Some(position.clone())
            );
            reconcile(domain.state(), domain.wal()).unwrap();
            assert_eq!(tip(&domain), Some(position));
        }
    }

    #[test]
    fn later_checkpoint_is_left_for_normal_bootstrap() {
        let domain = ToyDomain::new(None, None);
        let state = ChainPoint::Specific(5, [1; 32].into());
        let checkpoint = ChainPoint::Specific(10, [2; 32].into());
        set_position(&domain, state.clone());
        domain.wal().reset_to(&checkpoint).unwrap();
        reconcile(domain.state(), domain.wal()).unwrap();
        assert_eq!(tip(&domain), Some(checkpoint));
        assert_eq!(domain.state().read_cursor().unwrap(), Some(state));
    }

    #[test]
    fn divergent_and_unanchored_positions_are_not_overwritten() {
        let domain = ToyDomain::new(None, None);
        let checkpoint = ChainPoint::Specific(10, [2; 32].into());
        domain.wal().reset_to(&checkpoint).unwrap();
        set_position(&domain, ChainPoint::Specific(10, [3; 32].into()));
        assert!(matches!(
            reconcile(domain.state(), domain.wal()),
            Err(CheckpointError::Diverged { .. })
        ));
        assert_eq!(tip(&domain), Some(checkpoint.clone()));
        set_position(&domain, ChainPoint::Slot(11));
        assert!(matches!(
            reconcile(domain.state(), domain.wal()),
            Err(CheckpointError::UnanchoredState { .. })
        ));
        assert_eq!(tip(&domain), Some(checkpoint));
    }

    #[test]
    fn missing_state_is_only_allowed_for_a_fresh_or_origin_checkpoint() {
        let state = dolos_core::builtin::MemoryStateStore::new();
        let wal = dolos_redb3::wal::RedbWalStore::<dolos_cardano::CardanoDelta>::memory().unwrap();
        reconcile(&state, &wal).unwrap();
        wal.reset_to(&ChainPoint::Origin).unwrap();
        reconcile(&state, &wal).unwrap();
        wal.reset_to(&ChainPoint::Specific(10, [2; 32].into()))
            .unwrap();
        assert!(matches!(
            reconcile(&state, &wal),
            Err(CheckpointError::MissingState { .. })
        ));
    }
}
