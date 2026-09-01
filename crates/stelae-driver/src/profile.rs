//! What the lifecycle has to ask a profile.

use stelae::inscription::Inscription;

use crate::Error;

/// The questions the publish and restore lifecycle asks of a profile.
///
/// [`stelae::Profile`] answers what the *protocol* needs — naming, kinds, tags,
/// the record ceiling — and deliberately has no hook for anything
/// dataset-shaped. The lifecycle needs a little more than that and strictly
/// less than a dataset: which kinds an epoch produces, which kinds carry the
/// tip, whether a layer may be carried forward, and whether two documents
/// describe the same dataset at all. None of those answers reaches into a
/// store, a chain or a node, which is why they can live on a companion trait
/// here rather than growing the protocol's.
///
/// Everything crossing this boundary in either direction is opaque: a `scope`
/// and a `position` are [`serde_json::Value`], composed by the profile and
/// never composed here. The driver reads a document's shape only through the
/// implementor.
pub trait DriverProfile: stelae::Profile {
    /// The kinds a closed window always produces a layer for, plus the sparse
    /// ones it may.
    ///
    /// The set a publish enumerates when it asks what it could carry forward
    /// rather than build.
    fn epoch_kinds(&self) -> &[&str];

    /// The subset of [`DriverProfile::epoch_kinds`] a window produces
    /// unconditionally.
    ///
    /// Split from the whole because the layer arithmetic is made of the two
    /// arities: the dense kinds multiply out by the number of windows, and the
    /// sparse ones have to be counted against the data.
    fn dense_epoch_kinds(&self) -> &[&str];

    /// Whether `kind` carries the dataset's tip rather than one window of its
    /// history.
    ///
    /// Kind classification like [`DriverProfile::epoch_kinds`], and asked for
    /// one reason: the staging arithmetic sizes the two halves of a stele
    /// differently. A tip is rewritten whole by every publish, so all of it is
    /// staged together and every such layer sums; anything else is staged a few
    /// at a time and only the largest few count. [`is_inheritable`] cannot
    /// stand in for this — it answers a different question, and a stele has
    /// layers that are neither inheritable nor tip.
    ///
    /// [`is_inheritable`]: DriverProfile::is_inheritable
    fn is_state_kind(&self, kind: &str) -> bool;

    /// Whether a layer of `kind` at `scope` may be carried forward from an
    /// earlier publish rather than built again.
    ///
    /// A question about the scope as well as the kind, and the one rule three
    /// callers share: the predecessor's manifest, an interrupted publish's
    /// record, and the note a landed layer leaves.
    fn is_inheritable(&self, kind: &str, scope: &serde_json::Value) -> bool;

    /// Refuse a predecessor that describes a different dataset than the one
    /// being published.
    ///
    /// `previous` is the stele being chained to; `position` is the document the
    /// new stele will carry. Both halves are the profile's own shape, so what
    /// "the same dataset" means is the profile's to decide — the driver only
    /// knows that a repository holding two of them is a fault, and refuses
    /// before anything is built.
    fn check_same_dataset(
        &self,
        previous: &Inscription,
        position: &serde_json::Value,
    ) -> Result<(), Error>;
}
