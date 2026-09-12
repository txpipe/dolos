//! Headless access to the Dolos snapshot profile.
//!
//! A host owns storage lifecycle and application policy. This module gives it
//! one read-only view for planning, directory publication, digest reproduction
//! and verification without exposing the underlying store handles.

use std::{num::NonZeroUsize, path::Path};

use dolos_core::{ArchiveStore, ChainPoint, StateStore};
use stelae::progress::Observer;

use crate::{
    export::{self, Document, Plan, Predecessor},
    inscription::Inscription,
    planning::{self, EpochRange},
    publisher::Publisher,
    registry::{Preview, Published},
    Error, RetainedEpochs,
};

/// The selection shared by publication, digest and reproduction.
///
/// All fields are optional so [`Selection::default`] preserves the profile's
/// measured defaults and selects every epoch available at the current cursor.
#[derive(Debug, Clone, Copy, Default)]
pub struct Selection {
    pub epochs: Option<EpochRange>,
    pub index_band: Option<NonZeroUsize>,
    pub producers: Option<NonZeroUsize>,
}

impl Selection {
    /// Apply every selection knob through the profile's canonical planning
    /// functions. The order is shared by all hosts and commands.
    pub fn apply(self, plan: Plan) -> Plan {
        let plan = planning::restrict(plan, self.epochs);
        let plan = planning::banded(plan, self.index_band);
        planning::produced(plan, self.producers)
    }
}

/// Profile operations available without exposing mutable storage handles.
pub trait SnapshotSource {
    fn committed_position(&self) -> Result<Option<ChainPoint>, Error>;
    fn epoch(&self) -> Result<Option<u64>, Error>;
    fn plan(&self, network_magic: u64, retained: RetainedEpochs) -> Result<Plan, Error>;

    /// Build and select a plan in the same way for every snapshot operation.
    fn selected_plan(
        &self,
        network_magic: u64,
        retained: RetainedEpochs,
        selection: Selection,
    ) -> Result<Plan, Error> {
        Ok(selection.apply(self.plan(network_magic, retained)?))
    }

    /// Write a directory stele through the same profile encoder publication
    /// and registry publication use.
    fn publish_directory(
        &self,
        destination: &Path,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Inscription, Error>;

    /// Reproduce a canonical inscription without writing it.
    fn digest_document(&self, plan: &Plan, previous: &dyn Predecessor) -> Result<Document, Error>;

    /// Rebuild every layer and compare it with a published inscription.
    fn verify_reproduction(
        &self,
        published: &Inscription,
        plan: &Plan,
    ) -> Result<Inscription, Error>;

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

    fn publish_directory(
        &self,
        destination: &Path,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Inscription, Error> {
        export::publish(destination, plan, self.archive, self.state, None, observer)
    }

    fn digest_document(&self, plan: &Plan, previous: &dyn Predecessor) -> Result<Document, Error> {
        export::digest_document(plan, self.archive, self.state, previous)
    }

    fn verify_reproduction(
        &self,
        published: &Inscription,
        plan: &Plan,
    ) -> Result<Inscription, Error> {
        export::verify_reproduction(published, plan, self.archive, self.state, None)
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
