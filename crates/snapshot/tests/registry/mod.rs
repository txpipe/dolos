//! The field registry: what every namespace's record is allowed to look like.
//!
//! Decision 0026 makes `.v{x}` a compatibility *contract* rather than an
//! exact-byte pin — writers may append numbered optional/defaulted fields,
//! readers must skip unknown ones, indexes are never renumbered or
//! repurposed, and enum variant addition is reader-breaking. That contract is
//! discipline, and discipline without CI is the hole the old coarse
//! versioning had, one level up. This module is the enforcement.
//!
//! ## What is pinned
//!
//! Per namespace, a **canary**: a fully-populated value ([`canaries`]) and the
//! hex of its encoding at the current `SCHEMA_REVS` revision. The encoding is
//! taken through the production path — `Entity::encode_entity` for the
//! seventeen entity namespaces, `layers::state::encode_utxo_value` for `utxos`,
//! the one namespace whose value this profile builds rather than carries.
//!
//! ## Append-only history
//!
//! [`Entry::history`] is ascending by revision and append-only. When a field is
//! appended the revision bumps: the old canary's hex is **retained** and a new
//! one is added. Every retained revision must still decode under today's
//! decoder — that is the tolerance the readers rely on, asserted rather than
//! assumed — and the current revision must still encode to its pinned hex,
//! which is what catches renumbering and silent width drift. The last entry is
//! the current revision and the only one a constructor still reproduces; older
//! ones are bytes and nothing else, because the type that produced them is
//! gone.
//!
//! A retained canary is never deleted and never edited. If a change makes an
//! old canary undecodable, the change is breaking: it needs a media-type
//! version bump for that namespace's kind, not an edit here.
//!
//! ## Where the bytes live
//!
//! Namespace goldens are files under `goldens/`, one per `(namespace,
//! revision)`, because some of them run to kilobytes and a wrapped literal
//! would hide a diff. Enum variant tables are short and stay inline in
//! [`enums`], next to the policy they pin.
//!
//! The hex for a **new** revision comes from the ignored
//! `print_current_canary_encodings` test, which prints every canary and every
//! variant row:
//!
//! ```text
//! cargo test -p dolos-snapshot --test field_registry -- --ignored --nocapture
//! ```
//!
//! That is the only legitimate use for it. It prints; it never writes, and a
//! golden that already exists is never refreshed from it — a pin that
//! disagrees with a re-run on unchanged code is a determinism defect, and one
//! that disagrees after a code change is the change being breaking.

pub mod canaries;
pub mod enums;
pub mod ground_rules;

use dolos_cardano::model::{
    AccountEpochLog, AccountStakeLog, AccountState, AssetState, DRepState, DatumState, EpochState,
    EraSummary, FixedNamespace, GovState, LeaderRewardLog, MemberRewardLog, PendingMirState,
    PendingRewardState, PoolDepositRefundLog, PoolState, ProposalState, StakeLog,
};
use dolos_core::{Entity, Namespace};
use dolos_snapshot::{layers::state, UTXOS};

/// One namespace's record encoding, pinned at one schema revision.
pub struct Pinned {
    /// The `SCHEMA_REVS` revision this hex was the current encoding at.
    pub rev: u64,

    /// Hex of the record value. Whitespace is insignificant, so a long golden
    /// may be wrapped.
    pub hex: &'static str,
}

/// A namespace's entry in the registry.
pub struct Entry {
    pub ns: Namespace,

    /// Retained revisions, ascending; the last is current.
    pub history: &'static [Pinned],

    /// The current canary, through the production encode path, with the
    /// namespace that path reports.
    pub encode: fn() -> (Namespace, Vec<u8>),

    /// Today's decoder, applied to a retained canary. `Err` carries the
    /// refusal so a failure names it.
    pub decode: fn(&[u8]) -> Result<(), String>,
}

fn decode_entity_as<T: Entity + FixedNamespace>(bytes: &[u8]) -> Result<(), String> {
    T::decode_entity(T::NS, &bytes.to_vec())
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// Declare one namespace's entry: an encode thunk over its canary, a decode
/// thunk over today's type, and the retained history.
macro_rules! entity_entry {
    ($fn_name:ident, $ty:ty, $ctor:path, $history:expr) => {{
        fn $fn_name() -> (Namespace, Vec<u8>) {
            let (ns, value) = <$ty as Entity>::encode_entity(&$ctor());
            (ns, value)
        }

        Entry {
            ns: <$ty as FixedNamespace>::NS,
            history: $history,
            encode: $fn_name,
            decode: decode_entity_as::<$ty>,
        }
    }};
}

/// The `utxos` value is `[era, body]`, composed by the profile rather than
/// carried from a store, so its canary goes through the profile's own codec.
fn encode_utxos() -> (Namespace, Vec<u8>) {
    let value = state::encode_utxo_value(&canaries::utxo_value())
        .expect("the utxo canary encodes")
        .into_bytes();

    (UTXOS, value)
}

fn decode_utxos(bytes: &[u8]) -> Result<(), String> {
    state::decode_utxo_value(bytes)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// The registry, one entry per namespace in `dolos_snapshot::NAMESPACES`.
///
/// Built rather than `const` because the encode thunks close over nothing but
/// still need names; the test asserts the namespace set against `NAMESPACES`,
/// so an entry that is missing or duplicated is a build failure and not a
/// silent gap in coverage.
pub fn registry() -> Vec<Entry> {
    vec![
        entity_entry!(
            enc_account_epochs,
            AccountEpochLog,
            canaries::account_epoch_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/account-epochs.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_account_stakes,
            AccountStakeLog,
            canaries::account_stake_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/account-stakes.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_accounts,
            AccountState,
            canaries::account_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/accounts.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_assets,
            AssetState,
            canaries::asset_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/assets.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_datums,
            DatumState,
            canaries::datum_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/datums.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_dreps,
            DRepState,
            canaries::drep_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/dreps.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_epochs,
            EpochState,
            canaries::epoch_state,
            // The one namespace whose history has moved. Revision 1 is
            // retained as a decode witness and nothing more: it is *one* of
            // the orderings pre-fix code could emit for
            // `RollingStats::registered_pools`, which was a hash container and
            // therefore had no single encoding. Stores synced before the fix
            // hold rows of this shape, so today's reader must still accept
            // them — which is exactly what a retained canary asserts, and all
            // it asserts.
            &[
                Pinned {
                    rev: 1,
                    hex: include_str!("goldens/epochs.rev1.hex"),
                },
                Pinned {
                    rev: 2,
                    hex: include_str!("goldens/epochs.rev2.hex"),
                },
            ]
        ),
        entity_entry!(
            enc_eras,
            EraSummary,
            canaries::era_summary,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/eras.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_gov,
            GovState,
            canaries::gov_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/gov.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_leader_rewards,
            LeaderRewardLog,
            canaries::leader_reward_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/leader-rewards.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_member_rewards,
            MemberRewardLog,
            canaries::member_reward_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/member-rewards.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_pending_mirs,
            PendingMirState,
            canaries::pending_mir_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/pending_mirs.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_pending_rewards,
            PendingRewardState,
            canaries::pending_reward_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/pending_rewards.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_pool_deposit_refunds,
            PoolDepositRefundLog,
            canaries::pool_deposit_refund_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/pool-deposit-refunds.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_pools,
            PoolState,
            canaries::pool_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/pools.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_proposals,
            ProposalState,
            canaries::proposal_state,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/proposals.rev1.hex"),
            }]
        ),
        entity_entry!(
            enc_stakes,
            StakeLog,
            canaries::stake_log,
            &[Pinned {
                rev: 1,
                hex: include_str!("goldens/stakes.rev1.hex"),
            }]
        ),
        Entry {
            ns: UTXOS,
            history: &[Pinned {
                rev: 1,
                hex: include_str!("goldens/utxos.rev1.hex"),
            }],
            encode: encode_utxos,
            decode: decode_utxos,
        },
    ]
}

/// Hex with whitespace removed, so a golden may be wrapped and a file may end
/// with a newline.
pub fn normalize(hex: &str) -> String {
    hex.chars().filter(|c| !c.is_whitespace()).collect()
}

/// A golden's bytes.
pub fn decode_hex(hex: &str) -> Vec<u8> {
    hex::decode(normalize(hex)).expect("a golden is valid hex")
}
