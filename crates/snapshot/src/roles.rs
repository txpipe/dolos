//! Which array of an inscription a layer belongs to (decision 0080).
//!
//! An inscription lists its layers in two arrays: `diff`, which every later
//! snapshot inherits, and `tip`, which belongs to its own sequence alone. A
//! layer goes to `diff` once its content is final; until then each snapshot
//! carries its current form in its tip. This module is that rule for this
//! profile, and the one table it reads, [`ANCHORS`]. [`Listing`] is what one
//! snapshot lists under it, which is what export builds.

use dolos_cardano::{
    eras::ChainSummary,
    periods::{is_sealable, newly_sealable},
};

use crate::{is_state_kind, Error, BLOCKS, DENSE_EPOCH_KINDS, ENTITY_KINDS, INDEXES, LOG_KINDS};

/// The array of an inscription a layer is listed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Final: listed once, by the first snapshot that holds it, and inherited
    /// by every later one.
    Diff,
    /// The current form of a layer that can still change, listed by every
    /// snapshot that holds it.
    Tip,
}

/// When the layer of one kind for window `e` can first exist and when it is
/// final.
///
/// Both are sequences: the snapshot at sequence N is cut at the boundary that
/// opens epoch N, so `e + 1` is the first snapshot after window `e` closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Anchor {
    /// Possible from the snapshot at `e + first`, final from `e + settled`.
    After { first: u64, settled: u64 },
    /// Written whole by the seal at the boundary that seals window `e`, which
    /// [`Rule::seal`] names.
    Seal,
    /// Listed only by the snapshot at `e`, in its tip, and by no diff: what is
    /// true only at the cut. The boundary window's `blocks` (decision 0080,
    /// points 11 and 13); a closed epoch's blocks are the blocks profile's.
    Cut,
}

impl Anchor {
    /// The ordinary epoch kind: its boundary sliver at `e`, then final once
    /// the window closes.
    const CLOSED: Self = Self::After {
        first: 0,
        settled: 1,
    };

    /// Final from the first snapshot that holds it.
    const fn at(offset: u64) -> Self {
        Self::After {
            first: offset,
            settled: offset,
        }
    }
}

/// Every epoch kind and its [`Anchor`], in inscription order: the dense kinds,
/// then the log kinds (`PROFILE.md` "Log anchors" is this table in prose),
/// then the entity kinds. Every log row keys at `epoch_start(e)`.
pub const ANCHORS: [(&str, Anchor);
    DENSE_EPOCH_KINDS.len() + LOG_KINDS.len() + ENTITY_KINDS.len()] = [
    (BLOCKS, Anchor::Cut),
    (INDEXES, Anchor::CLOSED),
    ("log-account-address-epochs", Anchor::Seal),
    // ESTART opening e, stake leg alone; EWRAP closing e + 1 rewrites them in
    // full under the same keys
    (
        "log-account-epochs",
        Anchor::After {
            first: 0,
            settled: 2,
        },
    ),
    ("log-account-stats", Anchor::Seal),
    ("log-address-stats", Anchor::Seal),
    ("log-asset-mint-chunks", Anchor::Seal),
    // EWRAP closing e, the distribution it completes
    ("log-drep-epochs", Anchor::at(1)),
    // ESTART opening e + 1 moves the final rows
    ("log-dreps", Anchor::at(1)),
    // EWRAP closing e
    ("log-epochs", Anchor::at(1)),
    ("log-first-mint-chunks", Anchor::Seal),
    ("log-first-script-chunks", Anchor::Seal),
    ("log-payment-address-epochs", Anchor::Seal),
    ("log-policy-first-mint-chunks", Anchor::Seal),
    // RUPD during e + 1, settled by EWRAP closing e + 1 on what it paid
    ("log-pool-epochs", Anchor::at(2)),
    ("log-pools", Anchor::at(1)),
    ("log-proposals", Anchor::at(1)),
    ("entity-asset", Anchor::Seal),
    ("entity-script", Anchor::Seal),
    ("entity-stake", Anchor::Seal),
];

/// The [`Anchor`] of an epoch kind, or `None` where `kind` is not one.
pub fn anchor_for(kind: &str) -> Option<Anchor> {
    ANCHORS
        .into_iter()
        .find(|(known, _)| *known == kind)
        .map(|(_, anchor)| anchor)
}

/// The array of a layer the snapshot at `sequence` writes, without the chain's
/// epoch geometry.
///
/// [`Rule::role`] for a layer known to exist: a seal-anchored layer is written
/// only by the seal that makes it final, so a written one is in the diff.
/// What the driver asks of the layers it records and resumes.
pub fn written_role(kind: &str, window: Option<u64>, sequence: u64) -> Result<Role, Error> {
    let mismatch = |scope| Error::ScopeMismatch {
        kind: kind.to_owned(),
        scope,
    };

    if is_state_kind(kind) {
        return match window {
            None => Ok(Role::Tip),
            Some(_) => Err(mismatch("an epoch in a state scope")),
        };
    }

    let anchor = anchor_for(kind).ok_or_else(|| Error::Unassigned(kind.to_owned()))?;
    let epoch = window.ok_or_else(|| mismatch("a scope without an epoch"))?;

    match anchor {
        Anchor::Seal => Ok(Role::Diff),
        Anchor::Cut if sequence == epoch => Ok(Role::Tip),
        Anchor::Cut => Err(Error::Unlisted {
            kind: kind.to_owned(),
            window: epoch,
            sequence,
        }),
        Anchor::After { first, settled } => match sequence {
            sequence if sequence < epoch + first => Err(Error::Unlisted {
                kind: kind.to_owned(),
                window: epoch,
                sequence,
            }),
            sequence if sequence < epoch + settled => Ok(Role::Tip),
            _ => Ok(Role::Diff),
        },
    }
}

/// What the snapshot at one sequence lists: its tip, and in its diff only the
/// layers that become final there.
///
/// A snapshot that starts its lineage lists in its diff every layer final by
/// its cut instead (decision 0080, point 3).
#[derive(Debug, Clone, Copy)]
pub struct Listing<'a> {
    rule: Rule<'a>,
    sequence: u64,
    starts: bool,
}

impl<'a> Listing<'a> {
    /// The listing of the snapshot at `sequence`; `starts` when it has no
    /// predecessor.
    pub fn new(rule: Rule<'a>, sequence: u64, starts: bool) -> Self {
        Self {
            rule,
            sequence,
            starts,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Whether this snapshot starts its lineage.
    pub fn starts(&self) -> bool {
        self.starts
    }

    /// The array this snapshot lists a layer of `kind` for `window` in, or
    /// `None` where it lists none: the rule holds no such layer yet, or an
    /// earlier diff of the lineage already listed it.
    pub fn role(&self, kind: &str, window: Option<u64>) -> Result<Option<Role>, Error> {
        let role = self.rule.role(kind, window, self.sequence)?;

        Ok(match role {
            Some(Role::Diff)
                if !self.starts && self.rule.first_listed(kind, window)? != Some(self.sequence) =>
            {
                None
            }
            role => role,
        })
    }

    /// Whether this snapshot lists a layer of `kind` for `window`.
    pub fn lists(&self, kind: &str, window: u64) -> Result<bool, Error> {
        Ok(self.role(kind, Some(window))?.is_some())
    }
}

/// The rule over one chain: its epoch bounds and its genesis stability window,
/// `3k/f`, which together say which boundary seals an epoch.
#[derive(Debug, Clone, Copy)]
pub struct Rule<'a> {
    summary: &'a ChainSummary,
    mutable_slots: u64,
}

impl<'a> Rule<'a> {
    pub fn new(summary: &'a ChainSummary, mutable_slots: u64) -> Self {
        Self {
            summary,
            mutable_slots,
        }
    }

    /// The boundary whose seal writes `epoch`'s period logs and entity
    /// entries: the first boundary that `epoch`'s last slot is more than a
    /// stability window behind.
    pub fn seal(&self, epoch: u64) -> u64 {
        (epoch + 1..)
            .find(|boundary| is_sealable(self.summary, self.mutable_slots, epoch, *boundary))
            .expect("every epoch is sealed by some later boundary")
    }

    /// The epochs the seal at `boundary` writes and no earlier seal did: the
    /// windows whose period logs and entity layers the snapshot at `boundary`
    /// first holds.
    pub fn sealed_at(&self, boundary: u64) -> Vec<u64> {
        newly_sealable(self.summary, self.mutable_slots, boundary)
    }

    /// The array the rule puts a layer of `kind` in at `sequence`, or `None`
    /// where it places no such layer in that snapshot. `window` is the epoch
    /// the layer's scope names, and `None` for a state shard. A kind with no
    /// array is [`Error::Unassigned`], and a scope of the wrong shape
    /// [`Error::ScopeMismatch`].
    ///
    /// Neither this nor [`Rule::first_listed`] sees records: a log or entity
    /// layer exists only where its window holds one, so an answer says where
    /// such a layer goes, not that it exists.
    pub fn role(
        &self,
        kind: &str,
        window: Option<u64>,
        sequence: u64,
    ) -> Result<Option<Role>, Error> {
        Ok(match self.span(kind, window)? {
            Span::State => Some(Role::Tip),
            Span::Cut(epoch) => (sequence == epoch).then_some(Role::Tip),
            Span::Window { first, settled } => match sequence {
                sequence if sequence < first => None,
                sequence if sequence < settled => Some(Role::Tip),
                _ => Some(Role::Diff),
            },
        })
    }

    /// The sequence whose `diff` would first list a layer of `kind` for
    /// `window`, or `None` for a layer no `diff` lists: a state shard, or the
    /// boundary window's blocks.
    pub fn first_listed(&self, kind: &str, window: Option<u64>) -> Result<Option<u64>, Error> {
        Ok(match self.span(kind, window)? {
            Span::Window { settled, .. } => Some(settled),
            Span::State | Span::Cut(_) => None,
        })
    }

    /// Which snapshots hold a layer of `kind` for `window`.
    fn span(&self, kind: &str, window: Option<u64>) -> Result<Span, Error> {
        let mismatch = |scope| Error::ScopeMismatch {
            kind: kind.to_owned(),
            scope,
        };

        if is_state_kind(kind) {
            // a state scope that names an epoch is a retained dump, which
            // decision 0080 removes
            return match window {
                None => Ok(Span::State),
                Some(_) => Err(mismatch("an epoch in a state scope")),
            };
        }

        let anchor = anchor_for(kind).ok_or_else(|| Error::Unassigned(kind.to_owned()))?;
        let epoch = window.ok_or_else(|| mismatch("a scope without an epoch"))?;

        Ok(match anchor {
            Anchor::After { first, settled } => Span::Window {
                first: epoch + first,
                settled: epoch + settled,
            },
            Anchor::Seal => {
                let seal = self.seal(epoch);
                Span::Window {
                    first: seal,
                    settled: seal,
                }
            }
            Anchor::Cut => Span::Cut(epoch),
        })
    }
}

/// The snapshots that hold one layer.
enum Span {
    /// Every snapshot's tip: a state shard.
    State,
    /// The tip of the snapshot at this sequence alone.
    Cut(u64),
    /// From `first`, in the tip until `settled` and in the diff after.
    Window { first: u64, settled: u64 },
}

#[cfg(test)]
mod tests {
    use dolos_cardano::{model::EraState, EraBoundary};

    use super::*;
    use crate::{EPOCH_KINDS, KINDS, STATE_KIND_NAMES};

    /// Mainnet's eras: 208 Byron epochs of 21,600 slots, then 432,000-slot
    /// epochs, with `3k/f` of its genesis.
    fn mainnet() -> (ChainSummary, u64) {
        let byron = EraState {
            start: EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 1_506_203_091,
            },
            end: Some(EraBoundary {
                epoch: 208,
                slot: 4_492_800,
                timestamp: 1_596_059_091,
            }),
            epoch_length: 21_600,
            slot_length: 20,
            protocol: 0,
        };

        let shelley = EraState {
            start: byron.end.clone().unwrap(),
            end: None,
            epoch_length: 432_000,
            slot_length: 1,
            protocol: 2,
        };

        let mut summary = ChainSummary::default();
        summary.append_era(0, byron);
        summary.append_era(2, shelley);

        (summary, stability_window(2160))
    }

    /// Preview's single era of 86,400-slot epochs from slot 0, with `3k/f` of
    /// its genesis.
    fn preview() -> (ChainSummary, u64) {
        let era = EraState {
            start: EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 1_666_656_000,
            },
            end: None,
            epoch_length: 86_400,
            slot_length: 1,
            protocol: 6,
        };

        let mut summary = ChainSummary::default();
        summary.append_era(6, era);

        (summary, stability_window(432))
    }

    /// `3k/f` at both networks' active slot coefficient, 0.05.
    fn stability_window(k: u64) -> u64 {
        3 * k * 20
    }

    fn networks() -> [(&'static str, ChainSummary, u64); 2] {
        let (mainnet, mainnet_window) = mainnet();
        let (preview, preview_window) = preview();

        [
            ("mainnet", mainnet, mainnet_window),
            ("preview", preview, preview_window),
        ]
    }

    /// The roles of `kind`'s window `epoch` at the sequences from `epoch` to
    /// `epoch + 9`.
    fn roles(rule: &Rule, kind: &str, epoch: u64) -> Vec<Option<Role>> {
        (epoch..epoch + 10)
            .map(|sequence| rule.role(kind, Some(epoch), sequence).unwrap())
            .collect()
    }

    /// `None` up to `first`, the tip until `settled`, the diff after.
    fn expected(epoch: u64, first: u64, settled: u64) -> Vec<Option<Role>> {
        (epoch..epoch + 10)
            .map(|sequence| match sequence {
                sequence if sequence < first => None,
                sequence if sequence < settled => Some(Role::Tip),
                _ => Some(Role::Diff),
            })
            .collect()
    }

    const ORDINARY: [&str; 5] = [
        "log-drep-epochs",
        "log-dreps",
        "log-epochs",
        "log-pools",
        "log-proposals",
    ];

    const PERIOD: [&str; 8] = [
        "log-account-address-epochs",
        "log-account-stats",
        "log-address-stats",
        "log-asset-mint-chunks",
        "log-first-mint-chunks",
        "log-first-script-chunks",
        "log-payment-address-epochs",
        "log-policy-first-mint-chunks",
    ];

    #[test]
    fn the_table_holds_every_epoch_kind_in_inscription_order() {
        let kinds: Vec<_> = ANCHORS.iter().map(|(kind, _)| *kind).collect();

        assert_eq!(kinds, EPOCH_KINDS);
    }

    /// The boundary window is only ever the tip: a layer that exists at its
    /// own window's sequence settles later.
    #[test]
    fn no_window_is_final_at_its_own_boundary() {
        for (kind, anchor) in ANCHORS {
            if let Anchor::After { first, settled } = anchor {
                assert!(first <= settled && settled >= 1, "{kind}");
            }
        }
    }

    /// The boundary sliver at `e` and the closed window from `e + 1`.
    #[test]
    fn a_closed_window_of_an_ordinary_kind_is_in_the_diff_after_it_closes() {
        for (network, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for epoch in [0, 207, 208, 400] {
                assert_eq!(
                    roles(&rule, INDEXES, epoch),
                    expected(epoch, epoch, epoch + 1),
                    "{network} {epoch}"
                );
                assert_eq!(
                    rule.first_listed(INDEXES, Some(epoch)).unwrap(),
                    Some(epoch + 1)
                );

                for kind in ORDINARY {
                    assert_eq!(
                        roles(&rule, kind, epoch),
                        expected(epoch, epoch + 1, epoch + 1),
                        "{network} {kind} {epoch}"
                    );
                }

                assert_eq!(
                    roles(&rule, "log-pool-epochs", epoch),
                    expected(epoch, epoch + 2, epoch + 2),
                    "{network} log-pool-epochs {epoch}"
                );
            }
        }
    }

    /// A window's blocks are in its own boundary's tip and in no other
    /// snapshot: a closed epoch's blocks are the blocks profile's.
    #[test]
    fn blocks_are_the_tip_of_their_own_boundary_alone() {
        for (network, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for epoch in [0, 207, 208, 400] {
                let mut found = roles(&rule, BLOCKS, epoch);

                assert_eq!(found.remove(0), Some(Role::Tip), "{network} {epoch}");
                assert!(found.iter().all(Option::is_none), "{network} {epoch}");
                assert_eq!(rule.first_listed(BLOCKS, Some(epoch)).unwrap(), None);
                assert_eq!(written_role(BLOCKS, Some(epoch), epoch).unwrap(), Role::Tip);

                for sequence in [epoch + 1, epoch + 9] {
                    assert!(matches!(
                        written_role(BLOCKS, Some(epoch), sequence),
                        Err(Error::Unlisted { .. })
                    ));
                }
            }
        }
    }

    #[test]
    fn account_epochs_are_the_tip_at_e_and_e_plus_1_and_the_diff_at_e_plus_2() {
        for (network, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for epoch in [0, 1, 207, 208, 400] {
                let found = roles(&rule, "log-account-epochs", epoch);

                assert_eq!(found[0], Some(Role::Tip), "{network} {epoch}");
                assert_eq!(found[1], Some(Role::Tip), "{network} {epoch}");
                assert!(found[2..].iter().all(|role| *role == Some(Role::Diff)));
                assert_eq!(
                    rule.first_listed("log-account-epochs", Some(epoch))
                        .unwrap(),
                    Some(epoch + 2)
                );
            }
        }
    }

    /// Two boundaries after on preview and on mainnet's Shelley epochs; seven
    /// on mainnet's Byron epochs, which are a sixth of a stability window
    /// long, and fewer as the windows reach Shelley.
    #[test]
    fn period_logs_and_entity_layers_are_in_the_diff_at_their_seal() {
        let (mainnet, mainnet_window) = mainnet();
        let (preview, preview_window) = preview();
        let mainnet = Rule::new(&mainnet, mainnet_window);
        let preview = Rule::new(&preview, preview_window);

        let seals = [
            (&mainnet, 0, 7),
            (&mainnet, 200, 207),
            (&mainnet, 201, 208),
            (&mainnet, 202, 209),
            (&mainnet, 206, 209),
            (&mainnet, 207, 209),
            (&mainnet, 208, 210),
            (&mainnet, 500, 502),
            (&preview, 0, 2),
            (&preview, 500, 502),
        ];

        let entities = ENTITY_KINDS.map(|(kind, _)| kind);

        for (rule, epoch, seal) in seals {
            assert_eq!(rule.seal(epoch), seal, "{epoch}");
            assert!(rule.sealed_at(seal).contains(&epoch), "{epoch}");

            for kind in PERIOD.iter().chain(&entities) {
                assert_eq!(roles(rule, kind, epoch), expected(epoch, seal, seal));
                assert_eq!(rule.first_listed(kind, Some(epoch)).unwrap(), Some(seal));
            }
        }
    }

    /// The seal [`Rule::seal`] names is the one whose newly sealable epochs
    /// [`Rule::sealed_at`] lists, across mainnet's change of epoch length.
    #[test]
    fn each_epoch_is_newly_sealed_at_its_seal_alone() {
        for (network, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for boundary in 0..230 {
                let sealed: Vec<_> = (0..boundary)
                    .filter(|epoch| rule.seal(*epoch) == boundary)
                    .collect();

                assert_eq!(rule.sealed_at(boundary), sealed, "{network} {boundary}");
            }
        }
    }

    /// With no stability window an epoch can no longer roll back once the
    /// next one opens.
    #[test]
    fn without_a_stability_window_the_next_boundary_seals() {
        let (summary, _) = preview();
        let rule = Rule::new(&summary, 0);

        for epoch in [0, 1, 50] {
            assert_eq!(rule.seal(epoch), epoch + 1);
            assert_eq!(rule.sealed_at(epoch + 1), [epoch]);
        }
    }

    #[test]
    fn state_shards_are_the_tip_at_every_sequence() {
        for (_, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for kind in STATE_KIND_NAMES {
                for sequence in [1, 208, 600] {
                    assert_eq!(rule.role(kind, None, sequence).unwrap(), Some(Role::Tip));
                }

                assert_eq!(rule.first_listed(kind, None).unwrap(), None);
            }
        }
    }

    /// A window above the sequence is not in its snapshot.
    #[test]
    fn a_later_window_is_in_no_array() {
        let (summary, window) = preview();
        let rule = Rule::new(&summary, window);

        for (kind, _) in ANCHORS {
            assert_eq!(rule.role(kind, Some(9), 8).unwrap(), None, "{kind}");
        }
    }

    #[test]
    fn a_kind_without_an_array_or_a_mismatched_scope_is_refused() {
        let (summary, window) = preview();
        let rule = Rule::new(&summary, window);

        // the retired `digests` kind is no longer a kind, so it has no role
        assert!(!KINDS.contains(&"digests"));
        assert!(matches!(
            rule.role("digests", None, 3),
            Err(Error::Unassigned(_))
        ));
        assert!(matches!(
            rule.role("log-unknown", Some(1), 3),
            Err(Error::Unassigned(_))
        ));
        assert!(matches!(
            rule.role("state-unknown", None, 3),
            Err(Error::Unassigned(_))
        ));
        assert!(matches!(
            rule.role(BLOCKS, None, 3),
            Err(Error::ScopeMismatch { .. })
        ));
        assert!(matches!(
            rule.role("state-utxos", Some(2), 3),
            Err(Error::ScopeMismatch { .. })
        ));
    }

    /// The geometry-free answer agrees with the rule for every layer the rule
    /// places in a snapshot.
    #[test]
    fn the_written_role_is_the_rules_for_every_layer_a_snapshot_holds() {
        for (network, summary, window) in networks() {
            let rule = Rule::new(&summary, window);

            for sequence in [1, 2, 207, 208, 209, 212, 500] {
                for (kind, _) in ANCHORS {
                    for epoch in 0..=sequence {
                        let Some(role) = rule.role(kind, Some(epoch), sequence).unwrap() else {
                            continue;
                        };

                        assert_eq!(
                            written_role(kind, Some(epoch), sequence).unwrap(),
                            role,
                            "{network} {kind} {epoch} at {sequence}"
                        );
                    }
                }

                for kind in STATE_KIND_NAMES {
                    assert_eq!(written_role(kind, None, sequence).unwrap(), Role::Tip);
                }
            }
        }

        assert!(matches!(
            written_role("log-pool-epochs", Some(7), 8),
            Err(Error::Unlisted { .. })
        ));
        assert!(matches!(
            written_role(STATE_KIND_NAMES[0], Some(7), 8),
            Err(Error::ScopeMismatch { .. })
        ));
    }

    /// A chained snapshot lists in its diff what becomes final at its own
    /// sequence; a lineage's first lists everything final by its cut. Both
    /// list the whole tip.
    #[test]
    fn a_listing_holds_the_diff_of_its_own_sequence_or_all_of_it_at_a_start() {
        let (summary, window) = preview();
        let rule = Rule::new(&summary, window);

        let chained = Listing::new(rule, 10, false);
        let first = Listing::new(rule, 10, true);

        assert_eq!(chained.role(INDEXES, Some(9)).unwrap(), Some(Role::Diff));
        assert_eq!(chained.role(INDEXES, Some(8)).unwrap(), None);
        assert_eq!(first.role(INDEXES, Some(0)).unwrap(), Some(Role::Diff));

        for listing in [chained, first] {
            assert_eq!(listing.role(INDEXES, Some(10)).unwrap(), Some(Role::Tip));
            assert_eq!(listing.role(BLOCKS, Some(10)).unwrap(), Some(Role::Tip));
            assert_eq!(listing.role(BLOCKS, Some(9)).unwrap(), None);
            assert_eq!(listing.role(BLOCKS, Some(0)).unwrap(), None);
            assert_eq!(
                listing.role("log-account-epochs", Some(9)).unwrap(),
                Some(Role::Tip)
            );
            assert_eq!(listing.role("log-pool-epochs", Some(9)).unwrap(), None);
            assert_eq!(
                listing.role(STATE_KIND_NAMES[0], None).unwrap(),
                Some(Role::Tip)
            );
        }

        assert_eq!(
            chained.role("log-account-epochs", Some(8)).unwrap(),
            Some(Role::Diff)
        );
        assert_eq!(chained.role("log-account-epochs", Some(7)).unwrap(), None);
        assert!(first.lists("log-account-epochs", 7).unwrap());

        // preview seals two boundaries after: 8's period logs at 10
        assert_eq!(rule.seal(8), 10);
        assert!(chained.lists("log-address-stats", 8).unwrap());
        assert!(!chained.lists("log-address-stats", 7).unwrap());
        assert!(!chained.lists("log-address-stats", 9).unwrap());
        assert!(first.lists("log-address-stats", 7).unwrap());
        assert!(!first.lists("log-address-stats", 9).unwrap());
    }
}
