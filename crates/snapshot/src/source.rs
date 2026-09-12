//! Read-only access to the Dolos snapshot profile.

use dolos_core::{ArchiveStore, ChainPoint, StateStore};
use stelae::progress::Observer;

use crate::{
    export::{self, Plan},
    publisher::Publisher,
    registry::{Preview, Published},
    Error, RetainedEpochs,
};

/// Profile operations available without exposing mutable storage handles.
pub trait SnapshotSource {
    fn committed_position(&self) -> Result<Option<ChainPoint>, Error>;
    fn epoch(&self) -> Result<Option<u64>, Error>;
    fn plan(&self, network_magic: u64, retained: RetainedEpochs) -> Result<Plan, Error>;
    fn preview(&self, publisher: &Publisher, plan: &Plan) -> Result<Preview, Error>;
    fn publish(
        &self,
        publisher: &Publisher,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Published, Error>;
}

/// A borrowed profile view. The stores remain private and cannot escape it.
pub struct StoreSnapshot<'a, A, S> {
    archive: &'a A,
    state: &'a S,
}

impl<'a, A: ArchiveStore, S: StateStore> StoreSnapshot<'a, A, S> {
    pub fn new(archive: &'a A, state: &'a S) -> Self {
        Self { archive, state }
    }
}

impl<A: ArchiveStore, S: StateStore> SnapshotSource for StoreSnapshot<'_, A, S> {
    fn committed_position(&self) -> Result<Option<ChainPoint>, Error> {
        Ok(self.state.read_cursor()?)
    }

    fn epoch(&self) -> Result<Option<u64>, Error> {
        let Some(position) = self.committed_position()? else {
            return Ok(None);
        };
        if !position.is_fully_defined() {
            return Err(Error::UnanchoredPoint(format!(
                "cursor at slot {} has no block hash",
                position.slot()
            )));
        }
        let summary = dolos_cardano::eras::load_chain_summary_from_state(self.state)?;
        Ok(Some(summary.slot_epoch(position.slot()).0))
    }

    fn plan(&self, network_magic: u64, retained: RetainedEpochs) -> Result<Plan, Error> {
        export::plan(self.state, network_magic, retained)
    }

    fn preview(&self, publisher: &Publisher, plan: &Plan) -> Result<Preview, Error> {
        publisher.preview(plan, self.archive)
    }

    fn publish(
        &self,
        publisher: &Publisher,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Published, Error> {
        publisher.publish(plan, self.archive, self.state, observer)
    }
}
