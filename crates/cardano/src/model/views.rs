//! The state of the materialized views (`"views"` namespace): whether they
//! hold every block the ledger holds.
//!
//! A single row at the empty key, absent until a node first rolls a batch
//! with `sync.materialized_views` off. Absence means the views are complete,
//! which is what every store built before the switch existed has. The first
//! batch rolled without the views writes the slot they stopped at, through
//! the WAL like any other delta, and nothing moves it forward again: the
//! blocks rolled meanwhile are missing from the views for good, so turning
//! the switch on later does not revive them — the roll keeps skipping them
//! and the endpoints keep refusing — until the node bootstraps again from a
//! stele with complete views, or rebuilds its state. The row is ledger state,
//! so a stele carries it and a node restored from one inherits the truth
//! about the views it restored.

use dolos_core::{BlockSlot, ChainError, Domain, EntityDelta, NsKey, StateStore as _};
use pallas::codec::minicbor::{self, Decode, Encode};
use serde::{Deserialize, Serialize};

use super::SingletonEntity;

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub struct ViewsState {
    /// The first slot the views missed, once a batch rolled without them.
    #[n(0)]
    pub stale_since: Option<BlockSlot>,
}

entity_boilerplate!(ViewsState, "views", state = ());

impl SingletonEntity for ViewsState {}

impl ViewsState {
    /// Whether the views hold every block the ledger holds.
    pub fn is_complete(&self) -> bool {
        self.stale_since.is_none()
    }
}

/// Whether the views hold every block the ledger holds, as the store says.
pub fn views_complete<D: Domain>(state: &D::State) -> Result<bool, ChainError> {
    let views = state.read_entity_typed::<ViewsState>(&())?;

    Ok(match views {
        Some(views) => views.is_complete(),
        None => true,
    })
}

/// Whether this node materializes the views: the switch is on, and the views
/// never fell behind the ledger.
pub fn views_materialized<D: Domain>(domain: &D) -> Result<bool, ChainError> {
    if !domain.sync_config().materialized_views() {
        return Ok(false);
    }

    views_complete::<D>(domain.state())
}

/// Record the slot the views stopped at.
///
/// Emitted once, by the first batch a node rolls without the views; a row that
/// already names a slot is left alone, since the views have been missing
/// blocks since then.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkViewsStale {
    pub since: BlockSlot,

    // undo: the row as it was, `None` for no row
    prev: Option<Option<BlockSlot>>,
}

impl MarkViewsStale {
    pub fn new(since: BlockSlot) -> Self {
        Self { since, prev: None }
    }
}

impl EntityDelta for MarkViewsStale {
    type Entity = ViewsState;

    fn key(&self) -> NsKey {
        ViewsState::ns_key()
    }

    fn apply(&mut self, entity: &mut Option<ViewsState>) {
        self.prev = entity.as_ref().map(|views| views.stale_since);

        let views = entity.get_or_insert_with(ViewsState::default);

        if views.stale_since.is_none() {
            views.stale_since = Some(self.since);
        }
    }

    fn undo(&self, entity: &mut Option<ViewsState>) {
        *entity = self.prev.map(|stale_since| ViewsState { stale_since });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_mark_sticks_and_undoes_to_what_was_there() {
        let mut row = None;

        let mut first = MarkViewsStale::new(100);
        first.apply(&mut row);
        assert_eq!(
            row,
            Some(ViewsState {
                stale_since: Some(100)
            })
        );

        let mut later = MarkViewsStale::new(200);
        later.apply(&mut row);
        assert_eq!(
            row,
            Some(ViewsState {
                stale_since: Some(100)
            })
        );

        later.undo(&mut row);
        assert_eq!(
            row,
            Some(ViewsState {
                stale_since: Some(100)
            })
        );

        first.undo(&mut row);
        assert_eq!(row, None);
    }
}
