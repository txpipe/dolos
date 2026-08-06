//! Publishing a Dolos stele into an OCI repository.
//!
//! [`crate::export`] knows how to walk a store set into any transport, and
//! `stelae::oci` knows how to move bytes into a registry. This module is the
//! part that only exists once a publisher is standing in front of a
//! *repository*: what the new stele says about the ones already in it, and what
//! it may take from them.
//!
//! ## A publish is chained, or it is refused
//!
//! Every stele carries a `history`: one entry per prior publication,
//! contiguous, ending at `sequence - 1`. A publish therefore reads the
//! repository's moving tag before it writes anything, and there are exactly
//! three outcomes — the repository is empty and this stele starts a chain, the
//! latest stele is this one's predecessor and the chain extends, or **the
//! publish is refused**. A publisher that skipped an epoch has an operational
//! fault, and silently starting a second chain in one repository is precisely
//! what the field exists to prevent. There is no flag here that overrides that.
//!
//! ## A reused layer is attested without being reproduced
//!
//! The layers of an epoch that has closed cannot change, so a publish that
//! already published them has no reason to build them again: it points its
//! manifest at the blobs the previous manifest pointed at and never opens the
//! store. That is worth a plain sentence, because it changes what a publish
//! asserts. The new inscription describes those layers, and this node did not
//! read a single byte of them out of its own stores to check. Three things make
//! the trade defensible and all three are load-bearing:
//!
//! - **Scope equality is the entire test** — for an epoch layer that is
//!   `epoch`, `startSlot` and `endSlot`, all three. It is what makes a
//!   previously *clamped* final epoch — one whose window stopped at the cursor
//!   rather than at the epoch boundary — correctly fail to match the same epoch
//!   published later in full, and be rebuilt.
//! - **The blob is checked to still exist**, one `HEAD` per layer, inside
//!   `Registry::adopt_layer`. ADR-004 argues epoch blobs stay referenced by
//!   later manifests and so survive garbage collection; a descriptor pointing
//!   at a blob that is gone is a stele nobody can restore, and the argument is
//!   not a substitute for the check.
//! - **A publisher can always choose to reproduce**, with `rebuild`. It
//!   suppresses inheritance and nothing else — the history still chains,
//!   because rebuilding what you published is not the same act as forgetting
//!   that you published it.
//!
//! State shards are never inherited; [`crate::export`] says why, and the second
//! of its two reasons is the one that matters.
//!
//! ## Two consequences a publisher should hear from the documentation
//!
//! **A repository's identity is path-dependent.** `history` is inside the
//! canonical document, so a stele's digest depends on the chain it extends.
//! Two repositories that were published into differently — even once, even by
//! the same node from the same stores — never agree on a digest again at any
//! later sequence. That is deliberate: it is what makes the newest inscription
//! an attestation of every earlier one rather than a snapshot of a moment.
//! Reproducibility lives one level down, in the layers: same stores and same
//! predecessor gives the same digest, which is what the `rebuild` comparison
//! checks.
//!
//! **A publish that dies half way leaves blobs behind.** Layers upload one at a
//! time and the manifest is written last, so an interrupted publish leaves
//! uploaded blobs that no manifest references. They are harmless — a registry's
//! garbage collection reclaims exactly that — and the moving tag still resolves
//! to the previous stele, which is a stele, and restores. Publishing again
//! re-uploads nothing it already sent, because the blob check finds them.

use std::{cell::Cell, collections::BTreeMap};

use dolos_core::{ArchiveStore, IndexStore, StateStore};
use stelae::{
    inscription::{HistoryEntry, Inscription, LayerDescriptor},
    oci::{Options, Registry, Stele, Transfer},
    Digest, SteleReader as _,
};

use crate::{
    export::{self, Plan, Predecessor},
    layers::digests,
    DolosProfile, Error, Scope as _, EPOCH_KINDS, STATE_SHARDS,
};

/// Open a repository in a registry.
///
/// Here rather than at the call site so the `dolos` binary keeps never naming
/// the protocol crate — the same property [`crate::export::publish`] and
/// [`crate::restore::restore_dir`] hold for a directory.
///
/// `insecure` speaks plaintext HTTP. It is for a registry on a loopback address
/// or a mirror inside a cluster, and for nothing that is reachable from outside
/// one.
///
/// **Never call any of this from inside an async context.** The transport owns
/// a current-thread runtime and enters it with `block_on`; `stelae::oci`'s
/// module documentation states the rule and the reason.
pub fn open(
    host: impl Into<String>,
    repository: impl Into<String>,
    insecure: bool,
) -> Result<Registry, Error> {
    Ok(Registry::open(
        host,
        repository,
        Options {
            insecure,
            scratch_dir: None,
        },
    )?)
}

/// What a publish into a repository did.
#[derive(Debug, Clone)]
pub struct Published {
    pub inscription: Inscription,
    /// The stele's identity: sha256 of the canonical inscription.
    pub identity: Digest,
    /// Layers this publish read out of the stores and compressed.
    pub layers_built: usize,
    /// Layers it inherited from the stele before it, and never built.
    pub layers_reused: usize,
    /// What the transport moved, as it counted it.
    pub transfer: Transfer,
}

/// What a publish into a repository *would* do.
///
/// The `--dry-run` half, and it answers the question a publisher wants before
/// committing hours: how much of this is new. It reports what the scopes
/// permit and deliberately spends no `HEAD` per layer — the publish itself
/// verifies every blob it inherits, and a dry run that promised a reuse the
/// publish then refused would be worse than one that does not promise.
#[derive(Debug, Clone)]
pub struct Preview {
    /// The sequence this publish would write.
    pub sequence: u64,
    /// The stele it would chain to, if the repository holds one.
    pub predecessor: Option<(u64, Digest)>,
    /// How many entries the new `history` would carry.
    pub history: usize,
    pub layers_reused: usize,
    pub layers_built: usize,
}

/// Publish `plan` into `registry`, chained to whatever is already there.
///
/// Reads the repository's moving tag first: an absent one starts a history, a
/// predecessor at `sequence - 1` extends it, and anything else is refused
/// before a single layer is built. `rebuild` reproduces every layer instead of
/// inheriting the ones whose scope is unchanged — see the module
/// documentation.
pub fn publish<A, S, I>(
    registry: &Registry,
    plan: &Plan,
    archive: &A,
    state: &S,
    indexes: &I,
    digest_records: Option<&[digests::ImmutableDigests]>,
    rebuild: bool,
) -> Result<Published, Error>
where
    A: ArchiveStore,
    S: StateStore,
    I: IndexStore,
{
    let latest = registry.latest(&DolosProfile)?;
    let previous = Chained::new(latest.as_ref(), registry, plan, rebuild)?;

    // Reset before the export, so what comes back is this publish's cost and
    // not a total carried over from an earlier one through the same transport.
    registry.take_transfer();

    let inscription = export::export(
        registry,
        plan,
        archive,
        state,
        indexes,
        digest_records,
        &previous,
    )?;

    let identity = inscription.digest()?;
    let layers_reused = previous.adopted.get();

    Ok(Published {
        layers_built: inscription.layers.len() - layers_reused,
        layers_reused,
        transfer: registry.transfer(),
        identity,
        inscription,
    })
}

/// What [`publish`] would do, without writing anything.
pub fn preview(registry: &Registry, plan: &Plan, rebuild: bool) -> Result<Preview, Error> {
    let latest = registry.latest(&DolosProfile)?;
    let previous = Chained::new(latest.as_ref(), registry, plan, rebuild)?;

    // Every epoch selected contributes one layer per epoch kind, and the state
    // tip contributes its sixteen shards however the epochs were restricted.
    let total = plan.epochs.len() * EPOCH_KINDS.len() + STATE_SHARDS as usize;

    // The same lookup `export` will make, through the same `layer_spec`, so a
    // preview and the publish that follows it cannot disagree about which
    // layers are inherited. A failure here is the scope refusing a kind it does
    // not describe, and it propagates rather than counting as a miss: a dry run
    // that quietly reported "build it" for a layer nobody could build would be
    // the one number a publisher trusts, wrong.
    let mut layers_reused = 0;

    for window in &plan.epochs {
        let scope = window.scope(plan.network.magic());

        for kind in EPOCH_KINDS {
            let spec = scope.layer_spec(kind)?;

            if previous.inheritable(kind, &spec.scope)?.is_some() {
                layers_reused += 1;
            }
        }
    }

    Ok(Preview {
        sequence: plan.sequence,
        predecessor: previous.predecessor,
        history: previous.history.len(),
        layers_built: total - layers_reused,
        layers_reused,
    })
}

/// The stele this publish follows, in a repository.
///
/// Holds the history it hands to the new inscription and the table of layers it
/// is willing to let the new stele inherit, keyed by the pair that decides it:
/// the layer's kind and the canonical encoding of its profile-owned scope.
struct Chained<'a> {
    registry: &'a Registry,
    source: Option<&'a Stele>,
    predecessor: Option<(u64, Digest)>,
    history: Vec<HistoryEntry>,
    inheritable: BTreeMap<(String, String), LayerDescriptor>,
    adopted: Cell<usize>,
}

impl<'a> Chained<'a> {
    fn new(
        latest: Option<&'a Stele>,
        registry: &'a Registry,
        plan: &Plan,
        rebuild: bool,
    ) -> Result<Self, Error> {
        let inscription = latest.map(|stele| stele.read_inscription()).transpose()?;

        if let Some(previous) = &inscription {
            same_network(previous, plan)?;
        }

        let history = history_for(inscription.as_ref(), plan.sequence)?;

        let predecessor = inscription
            .as_ref()
            .map(|previous| Ok::<_, Error>((previous.sequence, previous.digest()?)))
            .transpose()?;

        // Built only when it can be used. `rebuild` is the publisher choosing
        // to reproduce rather than inherit, and it stops here rather than at
        // `adopt` so that nothing downstream has to remember it was set.
        let inheritable = match (rebuild, &inscription) {
            (false, Some(previous)) => inheritable_layers(previous)?,
            _ => BTreeMap::new(),
        };

        Ok(Self {
            registry,
            source: latest.filter(|_| !rebuild),
            predecessor,
            history,
            inheritable,
            adopted: Cell::new(0),
        })
    }

    fn inheritable(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<&LayerDescriptor>, Error> {
        Ok(self.inheritable.get(&key(kind, scope)?))
    }
}

impl Predecessor for Chained<'_> {
    fn history(&self) -> &[HistoryEntry] {
        &self.history
    }

    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let (Some(source), Some(descriptor)) = (self.source, self.inheritable(kind, scope)?) else {
            return Ok(None);
        };

        // The arrangement and the answer are one act: by the time this returns
        // a descriptor, the transport is already carrying the blob, and the
        // `HEAD` that proves the registry still has it has already happened.
        self.registry.adopt_layer(source, descriptor.clone())?;
        self.adopted.set(self.adopted.get() + 1);

        Ok(Some(descriptor.clone()))
    }
}

/// The layers a new stele may inherit from `previous`, keyed by kind and scope.
///
/// Only the epoch kinds are in it. A state shard is the tip and changes every
/// publish, and — independently — its descriptor scope names no epoch, so scope
/// equality could not tell one publish's shard from another's. `digests` has no
/// source in this slice.
///
/// Two layers of one kind claiming one scope with different identities is a
/// refusal rather than a first-wins: it means the stele being chained to
/// describes the same window twice and disagrees with itself about what is in
/// it, and inheriting either answer would publish that disagreement forward.
fn inheritable_layers(
    previous: &Inscription,
) -> Result<BTreeMap<(String, String), LayerDescriptor>, Error> {
    let mut inheritable = BTreeMap::new();

    for layer in &previous.layers {
        if !EPOCH_KINDS.contains(&layer.kind.as_str()) {
            continue;
        }

        let key = key(&layer.kind, &layer.scope)?;

        if let Some(existing) = inheritable.get(&key) {
            let existing: &LayerDescriptor = existing;

            if existing.diff_id != layer.diff_id {
                return Err(Error::malformed_inscription(
                    format!("layers[{}]", layer.kind),
                    format!(
                        "sequence {} describes {} twice at one scope, as {} and as {}",
                        previous.sequence, layer.kind, existing.diff_id, layer.diff_id,
                    ),
                ));
            }

            continue;
        }

        inheritable.insert(key, layer.clone());
    }

    Ok(inheritable)
}

/// The table key: a layer's kind and the canonical encoding of its scope.
///
/// Canonical rather than `serde_json::Value` equality, so that two scopes are
/// the same key exactly when they are the same bytes inside the canonical
/// document — which is the only sense of "the same scope" the protocol has.
fn key(kind: &str, scope: &serde_json::Value) -> Result<(String, String), Error> {
    let canonical = stelae::inscription::canonical_json(scope)?;

    let canonical = String::from_utf8(canonical)
        .map_err(|e| Error::malformed_inscription("layer scope", e.to_string()))?;

    Ok((kind.to_owned(), canonical))
}

/// The history a stele at `sequence` carries when it follows `previous`.
///
/// The three legal readings of a repository, and the one refusal:
///
/// - **nothing there** — an empty history, which the protocol permits at any
///   sequence. The first stele of a repository carries no history, and so does
///   a publisher deliberately starting a new one at epoch 500;
/// - **the stele before this one** — the old history plus an entry naming it.
///   Contiguous by construction, so the protocol's invariant passes rather than
///   being relied upon;
/// - **anything else** — refused, naming both sequences. A gap means a
///   publisher skipped epochs, an equal sequence means it is republishing one,
///   and a higher one means the repository is ahead of this node. All three are
///   operational faults with different fixes, so the message says which.
///
/// Whether a deliberate gap ever gets a policy is not this function's to
/// invent; there is no flag here that overrides the refusal.
fn history_for(previous: Option<&Inscription>, sequence: u64) -> Result<Vec<HistoryEntry>, Error> {
    let Some(previous) = previous else {
        return Ok(Vec::new());
    };

    let latest = previous.sequence;

    let reason = match latest.checked_add(1) {
        Some(next) if next == sequence => {
            let mut history = previous.history.clone();

            history.push(HistoryEntry {
                sequence: latest,
                inscription_digest: previous.digest()?,
            });

            return Ok(history);
        }
        _ if latest >= sequence => {
            "this stele is at or behind the repository's latest; a republish would restart the \
             chain rather than extend it"
        }
        _ => {
            "a publish must follow the repository's latest stele, and this one would leave a gap \
             no later stele could close"
        }
    };

    Err(Error::HistoryBreak {
        latest,
        publishing: sequence,
        reason,
    })
}

/// Refuse a predecessor from another chain.
///
/// A repository holding two networks' steles is an operator fault, and the
/// check costs nothing: the previous stele's `position` already names its
/// network, and reading it is the same function a restore uses.
fn same_network(previous: &Inscription, plan: &Plan) -> Result<(), Error> {
    let found = crate::read_position(&previous.position)?.network;

    if found.magic() != plan.network.magic() {
        return Err(Error::NetworkMismatch {
            expected: plan.network.magic(),
            found: found.magic(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::Profile as _;

    use super::*;

    fn inscription(sequence: u64, history: Vec<HistoryEntry>) -> Inscription {
        let mut inscription = Inscription::new(
            &DolosProfile,
            sequence,
            json!({"epoch": sequence.saturating_sub(1)}),
            crate::parameters(),
            crate::compression(),
        );

        inscription.history = history;
        inscription
    }

    fn entry(sequence: u64) -> HistoryEntry {
        HistoryEntry {
            sequence,
            inscription_digest: Digest::compute(sequence.to_be_bytes()),
        }
    }

    /// The first stele of a repository carries no history, at any sequence.
    #[test]
    fn an_empty_repository_starts_a_history() {
        assert!(history_for(None, 0).unwrap().is_empty());
        assert!(history_for(None, 500).unwrap().is_empty());
    }

    #[test]
    fn a_publish_that_follows_latest_extends_the_chain() {
        let previous = inscription(3, vec![entry(1), entry(2)]);

        let history = history_for(Some(&previous), 4).unwrap();

        assert_eq!(
            history.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the old history plus an entry naming the stele it came from"
        );

        assert_eq!(history[2].inscription_digest, previous.digest().unwrap());

        // The invariant holds by construction rather than by inspection: a
        // document built on this history validates.
        inscription(4, history).validate().unwrap();
    }

    /// All three refusals name both sequences, because which of the three it is
    /// decides what the publisher does about it.
    #[test]
    fn a_publish_that_does_not_follow_latest_is_refused() {
        let previous = inscription(497, vec![]);

        for publishing in [500, 497, 496] {
            let err = history_for(Some(&previous), publishing).unwrap_err();
            let message = err.to_string();

            assert!(
                matches!(err, Error::HistoryBreak { .. }),
                "{publishing}: {err:?}"
            );

            assert!(message.contains("497"), "{publishing}: {message}");
            assert!(
                message.contains(&publishing.to_string()),
                "{publishing}: {message}"
            );
        }
    }

    #[test]
    fn a_gap_and_a_republish_are_told_apart() {
        let previous = inscription(497, vec![]);

        assert!(history_for(Some(&previous), 500)
            .unwrap_err()
            .to_string()
            .contains("gap"));

        assert!(history_for(Some(&previous), 497)
            .unwrap_err()
            .to_string()
            .contains("republish"));

        assert!(history_for(Some(&previous), 496)
            .unwrap_err()
            .to_string()
            .contains("republish"));
    }

    /// Only the epoch kinds are inheritable, and a state shard is excluded by
    /// this table rather than by the caller remembering to.
    #[test]
    fn only_epoch_layers_are_inheritable() {
        let mut previous = inscription(3, vec![]);

        previous.layers = vec![
            layer(
                crate::BLOCKS,
                json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
                1,
            ),
            layer(crate::STATE, json!({"shard": 0}), 2),
            layer(crate::DIGESTS, json!({"lastImmutable": 7}), 3),
        ];

        let table = inheritable_layers(&previous).unwrap();

        assert_eq!(table.len(), 1);
        assert!(table.contains_key(
            &key(
                crate::BLOCKS,
                &json!({"epoch": 2, "startSlot": 200, "endSlot": 299})
            )
            .unwrap()
        ));
    }

    /// The clamp rule, as a table lookup: a window that stopped at the cursor
    /// is a different scope from the same epoch published in full, so it is a
    /// miss and the layer is rebuilt.
    #[test]
    fn a_clamped_window_does_not_match_the_full_epoch() {
        let mut previous = inscription(3, vec![]);

        previous.layers = vec![layer(
            crate::BLOCKS,
            json!({"epoch": 2, "startSlot": 200, "endSlot": 250}),
            1,
        )];

        let table = inheritable_layers(&previous).unwrap();

        let full = key(
            crate::BLOCKS,
            &json!({"epoch": 2, "startSlot": 200, "endSlot": 299}),
        )
        .unwrap();

        assert!(!table.contains_key(&full));
    }

    #[test]
    fn one_scope_described_twice_two_ways_is_refused() {
        let mut previous = inscription(3, vec![]);
        let scope = json!({"epoch": 2, "startSlot": 200, "endSlot": 299});

        previous.layers = vec![
            layer(crate::BLOCKS, scope.clone(), 1),
            layer(crate::BLOCKS, scope, 2),
        ];

        let err = inheritable_layers(&previous).unwrap_err();

        assert!(matches!(err, Error::MalformedInscription { .. }), "{err:?}");
    }

    fn layer(kind: &str, scope: serde_json::Value, identity: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: DolosProfile.layer_media_type(kind).unwrap(),
            diff_id: Digest::compute([identity]),
            records: 1,
            uncompressed_size: 1,
            scope,
        }
    }
}
