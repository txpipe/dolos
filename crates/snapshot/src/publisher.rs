//! Publishing into an OCI repository, as a sequence of steps a command drives.
//!
//! [`registry`](crate::registry) is the lifecycle: it opens a transport, reads
//! the moving tag, chains onto a predecessor and moves blobs. What sits here is
//! the layer above it — the order those calls go in, where a node's credentials
//! and staging directory come from, and what each reading of the repository
//! *means* for a publish that is about to start.
//!
//! ## Why the steps are separate calls
//!
//! Because a command has something to say between them. It reports where the
//! node stands before the staging volume is sized, prints a dry run's counts
//! and stops, and opens a progress renderer only once bytes are actually going
//! to move — a renderer built any earlier draws an empty bar under a dry run's
//! report. One call that did all of it would have to take a callback per line;
//! four calls in an order a reader can see are the cheaper shape.

use std::path::PathBuf;

use dolos_core::{config::RootConfig, ArchiveStore, IndexStore, StateStore};
use stelae::progress::Observer;
use stelae_driver::Standing;

use crate::{
    export::Plan,
    node,
    registry::{self, Auth, Preview, Published, Publishing, Registry, Repository, Tuning},
    DolosProfile, Error,
};

/// The repository arm's settings, freed of the CLI that spelled them.
///
/// Factored so `snapshot backfill` publishes through exactly the code path
/// `snapshot publish --repo` does — same standing check, same preflight, same
/// chained predecessor, same report — rather than a second telling of it that
/// would drift.
pub struct RepositoryPublish<'a> {
    /// The repository to publish into.
    pub repo: &'a Repository,

    /// Talk plaintext HTTP rather than HTTPS.
    pub insecure: bool,

    /// Where to stage layers; `None` takes `<storage.path>/scratch`.
    pub scratch_dir: Option<&'a std::path::Path>,

    /// Build every layer instead of carrying forward published ones.
    pub rebuild: bool,

    /// Report what would be written and stop.
    pub dry_run: bool,

    /// Fail when the repository is already at this node's sequence.
    pub require_new: bool,

    /// How the publish moves: concurrency, and the carried-layer check.
    pub tuning: Tuning,
}

/// What a publish should do, from where the node stands against the repository.
///
/// Three of the four readings [`Standing`] can take are terminal, and two of
/// those are refusals — so this is the shape that is left once the policy has
/// had its say: two ways to carry on, and one way to stop having done nothing
/// wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing is published; this stele starts the chain.
    First,

    /// The repository's latest stele is `latest`, and this one extends it.
    After { latest: u64 },

    /// The repository has already reached this node, and the sentence that says
    /// so. A job on a timer that runs more often than epochs close arrives here
    /// every time it runs, and that is not a failure: a command reports this
    /// and exits zero. The message is carried rather than composed by the
    /// caller because `--require-new` refuses with the same one, and two
    /// spellings of it would drift.
    Nothing(String),
}

impl Next {
    /// Read a standing into what the publish should do.
    ///
    /// The two refusals are the policy this exists for:
    ///
    /// - **`--require-new` over an up-to-date repository** — the ordinary case
    ///   made an error, for an operator who ran this expecting a stele.
    /// - **the node further ahead than one sequence** — refused, and the
    ///   refusal stands: whether a deliberate gap ever gets a policy is not a
    ///   command's to invent. The message names the distance alongside both
    ///   sequences, so "the publisher has been down for a day" and "the
    ///   publisher has been down for a month" do not read the same.
    pub fn read(standing: Standing, sequence: u64, require_new: bool) -> Result<Self, Error> {
        match standing {
            Standing::Empty => Ok(Self::First),
            Standing::Next { latest } => Ok(Self::After { latest }),
            Standing::UpToDate { latest } => {
                let message = format!(
                    "nothing to publish: this repository is at sequence {latest} and this node is \
                     at sequence {sequence}",
                );

                match require_new {
                    true => Err(Error::NothingToPublish(message)),
                    false => Ok(Self::Nothing(message)),
                }
            }
            Standing::Ahead { latest, distance } => Err(Error::PublishWouldGap {
                latest,
                sequence,
                distance,
            }),
        }
    }
}

/// An opened repository and everything a node knows about publishing into it.
pub struct Publisher {
    registry: Registry,
    record_path: PathBuf,
    rebuild: bool,
}

impl Publisher {
    /// Open the repository, staging where this node's configuration says.
    ///
    /// `auth` is resolved by the caller through [`node::registry_auth`], the
    /// way every command that opens a repository resolves it — a node's
    /// identity is one question with one answer, asked before a transport
    /// exists.
    ///
    /// **Never call this from inside an async context.** See
    /// [`registry::open`].
    pub fn open(
        config: &RootConfig,
        publish: &RepositoryPublish<'_>,
        auth: Auth,
    ) -> Result<Self, Error> {
        let scratch = node::scratch_dir(&config.storage, publish.scratch_dir);

        let registry = registry::open(
            publish.repo,
            publish.insecure,
            auth,
            scratch,
            publish.tuning,
        )?;

        Ok(Self {
            registry,
            // The resumption record sits beside the stores, so an interrupted
            // publish restarted against this repository carries forward the
            // epoch layers it already uploaded instead of rebuilding them.
            // `--rebuild` starts it over along with everything else.
            record_path: registry::record_path_in(&config.storage.path),
            rebuild: publish.rebuild,
        })
    }

    /// Read where this node stands against the repository.
    ///
    /// A read of the latest manifest and the one network call a publish makes
    /// before it commits to anything, so running it again is free of
    /// consequence — which is why the transient retry wraps it here rather than
    /// anywhere later.
    ///
    /// `&|| false` for the abort predicate, `snapshot backfill`'s driver
    /// included: the publish that follows this read observes no cancellation
    /// for as long as it runs, so a predicate threaded in here would cut a
    /// shutdown's wait by the backoff and leave the minutes on either side of
    /// it untouched.
    pub fn standing(&self, plan: &Plan) -> Result<Standing, Error> {
        stelae_driver::retry::transient("reading the repository's latest stele", &|| false, || {
            registry::standing(&self.registry, plan)
        })
    }

    /// Size the staging directory against what the publish would put on it.
    ///
    /// Run before anything is built, and before a dry run too: a publisher
    /// asking what a publish would do wants the same answer the publish gives.
    /// A dry run that reported the layers and said nothing about the volume
    /// they would be staged on would be the one rehearsal a publisher does,
    /// missing the failure it exists to find. After
    /// [`Publisher::standing`], because a repository holding another
    /// network's chain is refused there and sizing against it would be
    /// sizing against the wrong stele.
    pub fn preflight(&self) -> Result<(), Error> {
        Ok(registry::preflight(&self.registry, &DolosProfile)?)
    }

    /// What the publish would do, without writing anything.
    pub fn preview<A: ArchiveStore>(&self, plan: &Plan, archive: &A) -> Result<Preview, Error> {
        // `None` here and `None` at [`Publisher::publish`] are one decision: a
        // dry run describes the publish that follows it, so the two calls are
        // handed the same digest records or the number is about something else.
        registry::preview(self.publishing(), plan, archive, None)
    }

    /// Publish, chained to whatever is already in the repository.
    pub fn publish<A, S, I>(
        &self,
        plan: &Plan,
        archive: &A,
        state: &S,
        indexes: &I,
        observer: &Observer,
    ) -> Result<Published, Error>
    where
        A: ArchiveStore,
        S: StateStore,
        I: IndexStore,
    {
        registry::publish(
            self.publishing(),
            plan,
            archive,
            state,
            indexes,
            None,
            observer,
        )
    }

    fn publishing(&self) -> Publishing<'_> {
        Publishing::new(&self.registry)
            .recording_in(self.record_path.clone())
            .rebuilding(self.rebuild)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four readings, mapped one-to-one onto what a command does with them.
    ///
    /// The pair that used to live inside the CLI's `standing()`, and the pair
    /// that is easiest to get wrong: an up-to-date repository is exit zero
    /// unless `--require-new` says otherwise, and a gap is always a refusal.
    #[test]
    fn the_readings_map_onto_carrying_on_stopping_and_refusing() {
        assert_eq!(
            Next::read(Standing::Empty, 500, false).unwrap(),
            Next::First
        );

        assert_eq!(
            Next::read(Standing::Next { latest: 499 }, 500, false).unwrap(),
            Next::After { latest: 499 }
        );

        let Ok(Next::Nothing(message)) = Next::read(Standing::UpToDate { latest: 500 }, 500, false)
        else {
            panic!("an up-to-date repository is not a failure without --require-new");
        };

        assert_eq!(
            message,
            "nothing to publish: this repository is at sequence 500 and this node is at sequence \
             500",
        );

        // The same sentence, refused.
        let refused = Next::read(Standing::UpToDate { latest: 500 }, 500, true).unwrap_err();
        assert_eq!(refused.to_string(), message);

        let gap = Next::read(
            Standing::Ahead {
                latest: 497,
                distance: 3,
            },
            500,
            false,
        )
        .unwrap_err();

        assert_eq!(
            gap.to_string(),
            "this repository's latest stele is sequence 497 and this node is at sequence 500, 3 \
             sequences ahead: a publish must follow the repository's latest stele, and this one \
             would leave a gap no later stele could close",
        );
    }
}
