//! Which array of an inscription a layer belongs to (decision 0080).
//!
//! An inscription lists its layers in two arrays: `diff`, which every later
//! snapshot inherits, and `tip`, which belongs to its own sequence alone. A
//! layer goes to `diff` once its content is final; until then each snapshot
//! carries its current form in its tip. This module is that rule for this
//! profile, and the one table it reads, [`ANCHORS`]. Export does not use it
//! yet.

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

/// When the layer of one kind for window `e` exists and when it is final.
///
/// Both are sequences: the snapshot at sequence N is cut at the boundary that
/// opens epoch N, so `e + 1` is the first snapshot after window `e` closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Anchor {
    /// First held by the snapshot at `e + first`, final from `e + settled`.
    After { first: u64, settled: u64 },
    /// Written whole by the seal at the boundary that seals window `e`, which
    /// [`Rule::seal`] names.
    Seal,
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
    (BLOCKS, Anchor::CLOSED),
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

    /// The array the snapshot at `sequence` holds a layer of `kind` in, or
    /// `None` where that snapshot holds no such layer. `window` is the epoch
    /// the layer's scope names, and `None` for a state shard.
    pub fn role(
        &self,
        kind: &str,
        window: Option<u64>,
        sequence: u64,
    ) -> Result<Option<Role>, Error> {
        let Some((first, settled)) = self.span(kind, window)? else {
            return Ok(Some(Role::Tip));
        };

        Ok(match sequence {
            sequence if sequence < first => None,
            sequence if sequence < settled => Some(Role::Tip),
            _ => Some(Role::Diff),
        })
    }

    /// The sequence whose `diff` first lists a layer of `kind` for `window`, or
    /// `None` for a state shard, which no `diff` lists.
    pub fn first_listed(&self, kind: &str, window: Option<u64>) -> Result<Option<u64>, Error> {
        Ok(self.span(kind, window)?.map(|(_, settled)| settled))
    }

    /// The first sequence that holds the layer and the first that holds it
    /// final, or `None` for a state shard.
    fn span(&self, kind: &str, window: Option<u64>) -> Result<Option<(u64, u64)>, Error> {
        let mismatch = |scope| Error::ScopeMismatch {
            kind: kind.to_owned(),
            scope,
        };

        if is_state_kind(kind) {
            // a state scope that names an epoch is a retained dump, which
            // decision 0080 removes
            return match window {
                None => Ok(None),
                Some(_) => Err(mismatch("an epoch in a state scope")),
            };
        }

        let anchor = anchor_for(kind).ok_or_else(|| Error::Unassigned(kind.to_owned()))?;
        let epoch = window.ok_or_else(|| mismatch("a scope without an epoch"))?;

        Ok(Some(match anchor {
            Anchor::After { first, settled } => (epoch + first, epoch + settled),
            Anchor::Seal => {
                let seal = self.seal(epoch);
                (seal, seal)
            }
        }))
    }
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
                for kind in DENSE_EPOCH_KINDS {
                    assert_eq!(
                        roles(&rule, kind, epoch),
                        expected(epoch, epoch, epoch + 1),
                        "{network} {kind} {epoch}"
                    );
                    assert_eq!(
                        rule.first_listed(kind, Some(epoch)).unwrap(),
                        Some(epoch + 1)
                    );
                }

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

        assert!(KINDS.contains(&crate::DIGESTS));
        assert!(matches!(
            rule.role(crate::DIGESTS, None, 3),
            Err(Error::Unassigned(_))
        ));
        assert!(matches!(
            rule.role("log-unknown", Some(1), 3),
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
}
