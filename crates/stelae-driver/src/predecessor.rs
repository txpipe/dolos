//! The publish a publish follows, and what it may carry forward from it.
//!
//! The seam an export walks against: one trait, and the two implementations
//! that need nothing but a document. The transport-shaped one — the publish
//! this one follows *in a repository* — is [`crate::publish::Chained`].

use stelae::inscription::{HistoryEntry, LayerDescriptor};

use crate::Error;

/// The publish this one follows.
///
/// Two questions, one concept: what a new inscription attests about the steles
/// before it, and which of their layers it may carry forward rather than build
/// again. Both are answers only the previous publish has, and holding them
/// together is what lets a publisher rebuild everything while still chaining —
/// the `--rebuild` case, which suppresses [`Predecessor::adopt`] and leaves
/// [`Predecessor::history`] exactly as it was.
///
/// The publish this one follows can be **this publish, interrupted**, which is
/// what [`Predecessor::landed`] is for: a stele that never got sealed left
/// layers behind that a restart may carry forward on exactly the terms a
/// predecessor's do. Nothing here decides where that is written down — that is
/// the implementor's, as `adopt` already is.
///
/// `Sync`, because an export drives its layer producers from a pool of
/// threads and each producer asks these questions for its own layers. An
/// implementor that keeps state — an adoption counter, a resumption record —
/// keeps it behind its own synchronization.
pub trait Predecessor: Sync {
    /// The history the new inscription carries: every prior publication,
    /// contiguous and ascending, ending at `sequence - 1`.
    ///
    /// Assembling it is the implementor's business, and the protocol holds it
    /// to the invariant when the document is validated
    /// (`stelae::inscription`).
    fn history(&self) -> &[HistoryEntry];

    /// The descriptor to adopt for a layer of `kind` at `scope`, or `None` to
    /// build it from the stores.
    ///
    /// An implementation that answers `Some` **has already arranged for the
    /// transport to carry the layer's blob**; all that is left for an export
    /// is to not walk the store. That ordering is why this returns a descriptor
    /// rather than a boolean: the answer and the arrangement are one act, and
    /// an export that reused a descriptor whose blob nothing carried would
    /// publish a manifest with a hole in it.
    ///
    /// The default reuses nothing, which is what makes [`First`] one line and
    /// what a transport with no notion of "already there" — a directory —
    /// wants.
    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let _ = (kind, scope);

        Ok(None)
    }

    /// Whether a layer of `kind` at `scope` would be carried forward rather
    /// than built — [`Predecessor::adopt`]'s question, asked without arranging
    /// anything.
    ///
    /// Separate because `adopt` *acts*: it puts the blob in the transport and
    /// counts the layer as reused. Forecasting how many layers a publish will
    /// write has to ask the same question without taking either step, and a
    /// forecast that called `adopt` would double-count every layer it looked
    /// at.
    ///
    /// It may answer `true` where `adopt` will later answer `None`, in exactly
    /// one case: a layer an interrupted publish recorded, whose blob the
    /// repository has since dropped. That is only discovered by reaching for
    /// it, which is the step this deliberately does not take — so this is the
    /// honest forecast and `adopt` is the outcome.
    ///
    /// The default reuses nothing, matching [`Predecessor::adopt`]'s.
    fn carried_forward(&self, kind: &str, scope: &serde_json::Value) -> Result<bool, Error> {
        let _ = (kind, scope);

        Ok(false)
    }

    /// Note that `descriptor`'s layer is in the transport and will be in the
    /// manifest, whether it was built here or adopted.
    ///
    /// Called once per epoch layer, the moment it lands, so an implementor
    /// writing it down leaves a record that means "this layer is up" rather
    /// than "this layer was attempted" — the same boundary
    /// a restore's checkpoint records on its side. Layers land from
    /// concurrent producers, so calls interleave; the record is keyed by kind
    /// and scope, never by arrival order. The state shards are deliberately
    /// never offered: they describe a moving tip, and a restart must rebuild
    /// them.
    ///
    /// A failure here **fails the publish**. Recording is not a courtesy: a
    /// record that silently stopped being written would cost the hours it
    /// exists to save, at the moment nobody is watching.
    ///
    /// The default does nothing, which is what a publish with no host behind it
    /// — a directory, a reproduction — wants.
    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), Error> {
        let _ = descriptor;

        Ok(())
    }
}

/// The first stele of a repository: no history, nothing to inherit.
///
/// The protocol permits an empty history at any sequence, so this is not only
/// the very first publish — it is every publish into a directory, which has no
/// way to be asked what it already holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct First;

impl Predecessor for First {
    fn history(&self) -> &[HistoryEntry] {
        &[]
    }
}
