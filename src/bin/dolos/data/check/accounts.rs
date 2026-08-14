//! Account epoch coherence — every account's epoch values sit where the live
//! epoch says they should, and no epoch boundary is half-finished.
//!
//! An [`EpochValue`] carries its own epoch position, and its `mark`/`set`/`go`
//! accessors are *positional relative to that position*: they only name the
//! epochs a caller expects when the value has been rotated in lockstep with
//! the ledger. ESTART transitions every account each boundary, so an account
//! whose `stake`, `pool` or `drep` lags the live [`EpochState::number`] is not
//! a stale-but-harmless row — every historical snapshot read against it
//! silently answers for the wrong epoch. That is what
//! [`EpochValue::is_at_epoch`] exists to say, and it is what this check asks.
//!
//! The three shard cursors on `EpochState` are the second half: they track a
//! boundary pipeline that is *in flight*. Each is cleared back to `None` by
//! the phase that finishes it, so a store at rest that still carries one was
//! interrupted mid-boundary, and its accounts are a mixture of two epochs.
//! That reading comes first in the report, because it explains every lagging
//! account that follows it.

use dolos_cardano::model::{AccountState, EpochState, SingletonEntity as _};
use dolos_cardano::FixedNamespace as _;
use dolos_core::{EntityKey, StateStore};
use indicatif::ProgressBar;
use miette::{Context as _, IntoDiagnostic as _};
use pallas::ledger::primitives::Epoch;

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::AccountEpochs;

/// How many lagging accounts to name individually before collapsing the rest
/// into a count. A boundary that failed halfway leaves millions of them, and
/// a report an operator cannot read is not a report.
const MAX_REPORTED_ACCOUNTS: usize = 20;

/// The in-flight shard cursors must all be `None` in a store at rest.
pub fn check_shard_cursors(epoch: &EpochState) -> Vec<Issue> {
    let phases = [
        ("ewrap_progress", "EWRAP", epoch.ewrap_progress.as_ref()),
        ("estart_progress", "ESTART", epoch.estart_progress.as_ref()),
        ("rupd_progress", "RUPD", epoch.rupd_progress.as_ref()),
    ];

    phases
        .into_iter()
        .filter_map(|(field, phase, progress)| {
            let progress = progress?;

            Some(Issue::new(
                CHECK,
                format!(
                    "epoch {} still carries `{field}` at shard {}/{}: the {phase} phase was \
                     interrupted mid-boundary and the accounts on disk are a mixture of two \
                     epochs",
                    epoch.number, progress.committed, progress.total,
                ),
            ))
        })
        .collect()
}

/// Verify one account's three epoch values against the live epoch.
///
/// Returns the names of the values that lag, so the caller can report an
/// account once with everything that is wrong with it.
fn lagging_values(account: &AccountState, current: Epoch) -> Vec<String> {
    let candidates = [
        ("stake", account.stake.epoch()),
        ("pool", account.pool.epoch()),
        ("drep", account.drep.epoch()),
    ];

    candidates
        .into_iter()
        .filter(|(_, epoch)| *epoch != Some(current))
        .map(|(name, epoch)| match epoch {
            Some(epoch) => format!("{name} at epoch {epoch}"),
            None => format!("{name} at genesis"),
        })
        .collect()
}

/// Walk every account and report the ones whose epoch values disagree with
/// the live epoch.
///
/// A row that does not decode is reported as an issue rather than aborting
/// the walk: an undecodable account is precisely the kind of damage this
/// command exists to name, and the accounts after it are still worth reading.
pub fn check_account_epochs<E: std::fmt::Display>(
    accounts: impl Iterator<Item = Result<(EntityKey, AccountState), E>>,
    current: Epoch,
    mut on_progress: impl FnMut(u64),
) -> Vec<Issue> {
    let mut issues = Vec::new();
    let mut lagging = 0usize;
    let mut seen = 0u64;

    for record in accounts {
        seen += 1;
        on_progress(seen);

        let (key, account) = match record {
            Ok(x) => x,
            Err(err) => {
                issues.push(Issue::new(
                    CHECK,
                    format!("account row {seen} of the namespace does not decode: {err}"),
                ));
                continue;
            }
        };

        let stale = lagging_values(&account, current);

        if stale.is_empty() {
            continue;
        }

        lagging += 1;

        if lagging <= MAX_REPORTED_ACCOUNTS {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "account {key} lags the live epoch {current}: {}",
                    stale.join(", ")
                ),
            ));
        }
    }

    if lagging > MAX_REPORTED_ACCOUNTS {
        issues.push(Issue::new(
            CHECK,
            format!(
                "{} further account(s) lag the live epoch {current}, not listed individually",
                lagging - MAX_REPORTED_ACCOUNTS
            ),
        ));
    }

    issues
}

pub fn run(stores: &crate::common::Stores, progress: &ProgressBar) -> miette::Result<Vec<Issue>> {
    let epoch = stores
        .state
        .read_entity_typed::<EpochState>(EpochState::NS, &EpochState::singleton_key())
        .into_diagnostic()
        .context("reading the live epoch state")?;

    let Some(epoch) = epoch else {
        // Nothing to be coherent with. `cursors` already reports a state
        // store that should have been bootstrapped and was not.
        return Ok(Vec::new());
    };

    let mut issues = check_shard_cursors(&epoch);

    let accounts = stores
        .state
        .iter_entities_typed::<AccountState>(AccountState::NS, None)
        .into_diagnostic()
        .context("iterating accounts")?;

    issues.extend(check_account_epochs(accounts, epoch.number, |seen| {
        if seen.is_multiple_of(100_000) {
            progress.set_message(format!("account-epochs: {seen} accounts"));
        }
    }));

    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_cardano::model::{EpochValue, ShardProgress, Stake};
    use pallas::ledger::primitives::StakeCredential;

    fn credential(seed: u8) -> StakeCredential {
        StakeCredential::AddrKeyhash([seed; 28].into())
    }

    fn account(epoch: Epoch, seed: u8) -> (EntityKey, AccountState) {
        let mut state = AccountState::new(epoch, credential(seed));
        state.stake = EpochValue::with_live(epoch, Stake::default());

        (EntityKey::from(vec![seed]), state)
    }

    fn epoch_state(number: Epoch) -> EpochState {
        EpochState {
            number,
            ..EpochState::default()
        }
    }

    #[test]
    fn accounts_at_the_live_epoch_pass() {
        let accounts = vec![account(42, 1), account(42, 2)];

        let issues = check_account_epochs(accounts.into_iter().map(Ok::<_, String>), 42, |_| {});

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The corruption fixture: an account left behind at the previous epoch,
    /// whose `mark`/`set`/`go` snapshots would answer for the wrong epochs.
    #[test]
    fn a_stale_epoch_value_is_reported() {
        let mut accounts = vec![account(42, 1), account(42, 2)];
        accounts[1].1.stake = EpochValue::with_live(41, Stake::default());

        let issues = check_account_epochs(accounts.into_iter().map(Ok::<_, String>), 42, |_| {});

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].check, CheckKind::AccountEpochs);
        assert!(issues[0].detail.contains("stake at epoch 41"));
        assert!(issues[0].detail.contains("live epoch 42"));
    }

    /// One account, every lagging value, one line.
    #[test]
    fn all_three_values_are_named_on_one_account() {
        let accounts = vec![account(41, 1)];

        let issues = check_account_epochs(accounts.into_iter().map(Ok::<_, String>), 42, |_| {});

        assert_eq!(issues.len(), 1);
        assert!(issues[0].detail.contains("stake at epoch 41"));
        assert!(issues[0].detail.contains("pool at epoch 41"));
        assert!(issues[0].detail.contains("drep at epoch 41"));
    }

    /// A half-finished boundary leaves every account behind; the report stays
    /// readable by naming a bounded number and counting the rest.
    #[test]
    fn mass_lag_is_summarized_rather_than_listed() {
        let accounts: Vec<_> = (0..=(MAX_REPORTED_ACCOUNTS as u8 + 5))
            .map(|seed| account(41, seed))
            .collect();
        let total = accounts.len();

        let issues = check_account_epochs(accounts.into_iter().map(Ok::<_, String>), 42, |_| {});

        assert_eq!(issues.len(), MAX_REPORTED_ACCOUNTS + 1);
        assert!(issues.last().unwrap().detail.contains(&format!(
            "{} further account(s)",
            total - MAX_REPORTED_ACCOUNTS
        )));
    }

    #[test]
    fn cleared_shard_cursors_pass() {
        assert!(check_shard_cursors(&epoch_state(42)).is_empty());
    }

    /// An interrupted boundary: the cursor that the finishing phase never got
    /// to clear.
    #[test]
    fn an_in_flight_shard_cursor_is_reported() {
        let mut state = epoch_state(42);
        state.ewrap_progress = Some(ShardProgress {
            committed: 3,
            total: 8,
        });

        let issues = check_shard_cursors(&state);

        assert_eq!(issues.len(), 1);
        assert!(issues[0].detail.contains("ewrap_progress"));
        assert!(issues[0].detail.contains("3/8"));
    }
}
