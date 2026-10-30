use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    eras::load_chain_summary_from_state,
    eras::log_epoch_range_to_key_range,
    model::{
        AccountEpochLog, AccountKey, DRepEpochLog, DRepKey, DRepState, PParamKind, PoolKey,
        PoolState, ProposalKey, ProposalState,
    },
    ChainSummary, EpochState, PoolEpochLog,
};
use dolos_core::{config::RootConfig, LogEntity, LogRangeOf};
use miette::{IntoDiagnostic, WrapErr};
use pallas::ledger::{addresses::Network as AddressNetwork, primitives::conway::DRep};
use serde::Serialize;

use super::{Bounds, Layout, Output};
use crate::output::{emit_record, Format};
use dolos::prelude::*;
use dolos_cardano::pallas_extras;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// the first epoch whose logs to dump
    #[arg(long, value_name = "EPOCH")]
    from_epoch: Option<u64>,

    /// the last epoch whose logs to dump
    #[arg(long, value_name = "EPOCH")]
    to_epoch: Option<u64>,

    #[command(flatten)]
    bounds: Bounds,

    #[command(flatten)]
    output: Output,
}

const NAMESPACES: [&str; 9] = [
    "pool-epochs",
    "drep-epochs",
    "dreps",
    "pools",
    "proposals",
    "account-epochs",
    "account-epochs/rewards",
    "epochs",
    "epochs/pparams",
];

struct RowContext {
    network: AddressNetwork,
    layout: Layout,
    summary: Option<ChainSummary>,
}

/// The key of the log a view renders, after the slot.
type KeyOf<T> = <<T as TableRow>::Log as LogEntity>::Key;

trait TableRow: Sized {
    /// The log this view renders, and what a JSON line carries.
    type Log: LogEntity + Serialize;

    /// Whether the view has a dbsync layout.
    const DBSYNC: bool = true;

    fn view(log: Self::Log) -> Self;

    fn header(format: Layout) -> Vec<&'static str>;

    /// The row this record renders as, or empty where it has nothing to say in
    /// this view.
    fn row(&self, slot: BlockSlot, key: &KeyOf<Self>, ctx: &RowContext) -> Vec<String>;

    /// Every output row this record renders as.
    ///
    /// One record is one row for all but the merged account-epoch log, whose
    /// reward view renders one row per list element — which is the whole point
    /// of the lists, and the reason this is not just [`Self::row`].
    fn rows(&self, slot: BlockSlot, key: &KeyOf<Self>, ctx: &RowContext) -> Vec<Vec<String>> {
        match self.row(slot, key, ctx) {
            row if row.is_empty() => Vec::new(),
            row => vec![row],
        }
    }
}

impl TableRow for EpochState {
    type Log = EpochState;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        match format {
            Layout::Table => vec![
                "number",
                "version",
                "nonce",
                "pot reserves",
                "pot utxos",
                "pot treasury",
                "stake deposits",
                "drep deposits",
                "proposal deposits",
                "pot rewards",
                "pot fees",
                "gathered fees",
                "pparams",
                "blocks",
                "effective_rewards",
                "unspendable_rewards",
            ],
            Layout::Dbsync => vec![
                "epoch_no",
                "protocol_major",
                "treasury",
                "reserves",
                "rewards",
                "utxo",
                "deposits_stake",
                "fees",
                "nonce",
                "block_count",
            ],
        }
    }

    fn row(&self, _slot: BlockSlot, _key: &(), ctx: &RowContext) -> Vec<String> {
        match ctx.layout {
            Layout::Table => {
                let pparams = self.pparams.live();
                let rolling = self.rolling.live();

                vec![
                    format!("{}", self.number),
                    format!(
                        "{}",
                        pparams
                            .as_ref()
                            .and_then(|x| x.protocol_major())
                            .unwrap_or_default()
                    ),
                    format_nonce(&self.nonces),
                    format!("{}", self.initial_pots.reserves),
                    format!("{}", self.initial_pots.utxos),
                    format!("{}", self.initial_pots.treasury),
                    format!("{}", self.initial_pots.stake_deposits()),
                    format!("{}", self.initial_pots.drep_deposits),
                    format!("{}", self.initial_pots.proposal_deposits),
                    format!("{}", self.initial_pots.rewards),
                    format!("{}", self.initial_pots.fees),
                    format!(
                        "{}",
                        rolling
                            .as_ref()
                            .map(|x| x.gathered_fees)
                            .unwrap_or_default()
                    ),
                    format!("{}", pparams.as_ref().map(|x| x.len()).unwrap_or_default()),
                    format!(
                        "{}",
                        rolling
                            .as_ref()
                            .map(|x| x.blocks_minted)
                            .unwrap_or_default()
                    ),
                    format!(
                        "{}",
                        self.end
                            .as_ref()
                            .map(|e| e.effective_rewards)
                            .unwrap_or_default()
                    ),
                    format!(
                        "{}",
                        self.end
                            .as_ref()
                            .map(|e| e.unspendable_to_treasury + e.unspendable_to_reserves)
                            .unwrap_or_default()
                    ),
                ]
            }
            Layout::Dbsync => {
                let nonce = self
                    .nonces
                    .as_ref()
                    .map(|x| hex::encode(x.active))
                    .unwrap_or_default();

                let rolling = self.rolling.live();

                let pparams = self.pparams.live();
                let protocol_major = pparams
                    .as_ref()
                    .and_then(|x| x.protocol_major())
                    .unwrap_or_default();

                vec![
                    self.number.to_string(),
                    protocol_major.to_string(),
                    self.initial_pots.treasury.to_string(),
                    self.initial_pots.reserves.to_string(),
                    self.initial_pots.rewards.to_string(),
                    self.initial_pots.utxos.to_string(),
                    self.initial_pots.stake_deposits().to_string(),
                    self.initial_pots.fees.to_string(),
                    nonce,
                    rolling
                        .map(|x| x.blocks_minted)
                        .unwrap_or_default()
                        .to_string(),
                ]
            }
        }
    }
}

fn format_nonce(nonces: &Option<dolos_cardano::Nonces>) -> String {
    let Some(nonces) = nonces else {
        return "-".to_string();
    };

    let hex = hex::encode(nonces.active.as_slice());
    let prefix = &hex[..4];
    let suffix = &hex[hex.len() - 3..];
    format!("{prefix}...{suffix}")
}

/// The epoch of a log row's slot, for a column headed `epoch`. A slot there is
/// off by orders of magnitude and says nothing about being wrong, so without a
/// summary absence renders as absence.
fn epoch_of(slot: BlockSlot, ctx: &RowContext) -> String {
    ctx.summary
        .as_ref()
        .map(|summary| summary.slot_epoch(slot).0.to_string())
        .unwrap_or_else(|| "-".to_string())
}

const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");

impl TableRow for PoolEpochLog {
    type Log = PoolEpochLog;

    const DBSYNC: bool = false;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for stakes logs (#1032)");
        }
        vec![
            //"pool hex",
            "pool bech32",
            "epoch",
            "blocks minted",
            "active stake",
            "delegators count",
            "live pledge",
            "total rewards",
        ]
    }

    fn row(&self, slot: BlockSlot, pool: &PoolKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.layout, Layout::Dbsync) {
            unreachable!("dbsync format not supported for stakes logs (#1032)");
        }
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool.0.as_ref()).unwrap();

        vec![
            //format!("{}", pool_hex),
            format!("{}", pool_bech32),
            epoch_of(slot, ctx),
            format!("{}", self.blocks_minted),
            format!("{}", self.total_stake),
            format!("{}", self.delegators_count),
            format!("{}", self.live_pledge),
            format!("{}", self.total_rewards),
        ]
    }
}

/// A retired pool's final row (`dolos_cardano::final_rows`). The epoch is the
/// key's, the one the boundary that moved it closed: its retiring epoch + 2.
impl TableRow for PoolState {
    type Log = PoolState;

    const DBSYNC: bool = false;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pools logs");
        }
        vec![
            "pool bech32",
            "epoch",
            "retiring epoch",
            "deposit",
            "calidus",
        ]
    }

    fn row(&self, slot: BlockSlot, pool: &PoolKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.layout, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pools logs");
        }
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool.0.as_ref()).unwrap();

        vec![
            pool_bech32,
            epoch_of(slot, ctx),
            self.retiring_epoch
                .map_or("-".to_string(), |x| x.to_string()),
            self.deposit.to_string(),
            self.calidus
                .as_ref()
                .and_then(|x| x.pub_key)
                .map_or("-".to_string(), hex::encode),
        ]
    }
}

const DREP_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("drep");

/// One DRep's power in one epoch's distribution. A credential DRep renders as
/// its CIP-129 id, which is its stored key; the two pseudo-DReps by the names
/// Koios gives them. The epoch is the key's, the one the boundary closed;
/// Koios's `epoch_no` for the row is one more.
impl TableRow for DRepEpochLog {
    type Log = DRepEpochLog;

    const DBSYNC: bool = false;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for drep-epochs logs");
        }
        vec!["drep", "epoch", "voting power", "registered"]
    }

    fn row(&self, slot: BlockSlot, drep: &DRepKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.layout, Layout::Dbsync) {
            unreachable!("dbsync format not supported for drep-epochs logs");
        }
        let drep = match drep.0 {
            DRep::Abstain => "drep_always_abstain".to_string(),
            DRep::NoConfidence => "drep_always_no_confidence".to_string(),
            _ => {
                bech32::encode::<bech32::Bech32>(DREP_HRP, &dolos_core::keys::encode(drep)).unwrap()
            }
        };

        vec![
            drep,
            epoch_of(slot, ctx),
            self.voting_power.to_string(),
            self.registered.to_string(),
        ]
    }
}

/// A moved DRep's final row (`dolos_cardano::final_rows`). The epoch is the
/// key's, the one the boundary that moved it closed.
impl TableRow for DRepState {
    type Log = DRepState;

    const DBSYNC: bool = false;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for dreps logs");
        }
        vec![
            "drep",
            "epoch",
            "registered slot",
            "unregistered slot",
            "deposit",
        ]
    }

    fn row(&self, slot: BlockSlot, drep: &DRepKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.layout, Layout::Dbsync) {
            unreachable!("dbsync format not supported for dreps logs");
        }

        let at =
            |x: Option<(BlockSlot, _)>| x.map_or("-".to_string(), |(slot, _)| slot.to_string());

        vec![
            bech32::encode::<bech32::Bech32>(DREP_HRP, &dolos_core::keys::encode(drep)).unwrap(),
            epoch_of(slot, ctx),
            at(self.registered_at),
            at(self.unregistered_at),
            self.deposit.to_string(),
        ]
    }
}

/// A resolved proposal's final row (`dolos_cardano::final_rows`). The epoch is
/// the key's, the one the boundary that moved it closed: the proposal's enacted
/// or dropped epoch.
impl TableRow for ProposalState {
    type Log = ProposalState;

    const DBSYNC: bool = false;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for proposals logs");
        }
        vec![
            "proposal",
            "epoch",
            "proposed in",
            "ratified",
            "canceled",
            "deposit",
        ]
    }

    fn row(&self, slot: BlockSlot, key: &ProposalKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.layout, Layout::Dbsync) {
            unreachable!("dbsync format not supported for proposals logs");
        }

        let epoch = |x: Option<u64>| x.map_or("-".to_string(), |x| x.to_string());

        vec![
            ProposalState::id(key.tx, key.idx),
            epoch_of(slot, ctx),
            epoch(self.proposed_in),
            epoch(self.ratified_epoch),
            epoch(self.canceled_epoch),
            epoch(self.deposit),
        ]
    }
}

/// The merged account-epoch log, in the stake view: what the four namespaces'
/// `account-stakes` dump produced, under the namespace that carries it now.
///
/// The dbsync layout is the `stake-{epoch}.csv` one the `epoch_pots` harness
/// compares against db-sync, unchanged — the record moved, the columns did not.
/// A row with no stake leg renders as nothing: it is an account that took a
/// reward this epoch without being in its distribution, which the stake view
/// has never had a line for.
impl TableRow for AccountEpochLog {
    type Log = AccountEpochLog;

    fn view(log: Self::Log) -> Self {
        log
    }

    fn header(format: Layout) -> Vec<&'static str> {
        match format {
            Layout::Table => vec![
                "slot",
                "stake",
                "pool",
                "active stake",
                "member reward",
                "leader rewards",
                "deposit refunds",
            ],
            Layout::Dbsync => vec!["stake", "pool", "lovelace"],
        }
    }

    fn row(&self, slot: BlockSlot, account: &AccountKey, ctx: &RowContext) -> Vec<String> {
        let stake = render_stake_address(account, ctx);
        let pool = self
            .pool_id
            .map(render_pool)
            .unwrap_or_else(|| "-".to_string());

        match ctx.layout {
            Layout::Table => vec![
                slot.to_string(),
                stake,
                pool,
                self.active_stake
                    .map(|amount| amount.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                self.member_reward
                    .map(|amount| amount.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                render_pool_amounts(&self.leader_rewards),
                render_pool_amounts(&self.deposit_refunds),
            ],
            Layout::Dbsync => match (self.active_stake, self.pool_id) {
                (Some(amount), Some(_)) => vec![stake, pool, amount.to_string()],
                _ => Vec::new(),
            },
        }
    }
}

/// The merged account-epoch log, in the reward view: one row per reward the
/// account took, which is where the lists earn their keep.
///
/// The dbsync layout is the one the three reward namespaces shared, with
/// `earned_epoch` read straight off the key — the merged row is filed under the
/// epoch it describes, so the `- 1` those dumps applied went with their key.
struct AccountEpochRewards(AccountEpochLog);

impl TableRow for AccountEpochRewards {
    type Log = AccountEpochLog;

    fn view(log: Self::Log) -> Self {
        AccountEpochRewards(log)
    }

    fn header(format: Layout) -> Vec<&'static str> {
        match format {
            Layout::Table => vec!["epoch", "stake", "pool", "type", "amount"],
            Layout::Dbsync => vec!["stake", "pool", "amount", "type", "earned_epoch"],
        }
    }

    fn row(&self, _slot: BlockSlot, _account: &AccountKey, _ctx: &RowContext) -> Vec<String> {
        unreachable!("the reward view renders through `rows`")
    }

    fn rows(&self, slot: BlockSlot, account: &AccountKey, ctx: &RowContext) -> Vec<Vec<String>> {
        let epoch = epoch_of(slot, ctx);

        let stake = render_stake_address(account, ctx);

        let member = self
            .0
            .member_reward
            .zip(self.0.pool_id)
            .map(|(amount, pool)| (pool, amount, "member"));

        let leader = self
            .0
            .leader_rewards
            .iter()
            .map(|(pool, amount)| (*pool, *amount, "leader"));

        let refunds = self
            .0
            .deposit_refunds
            .iter()
            .map(|(pool, amount)| (*pool, *amount, "pool_deposit_refund"));

        leader
            .chain(member)
            .chain(refunds)
            // The `> 0` filter the three reward dumps already applied.
            .filter(|(_, amount, _)| *amount > 0)
            .map(|(pool, amount, kind)| match ctx.layout {
                Layout::Table => vec![
                    epoch.clone(),
                    stake.clone(),
                    render_pool(pool),
                    kind.to_string(),
                    amount.to_string(),
                ],
                Layout::Dbsync => vec![
                    stake.clone(),
                    render_pool(pool),
                    amount.to_string(),
                    kind.to_string(),
                    epoch.clone(),
                ],
            })
            .collect()
    }
}

fn render_stake_address(account: &AccountKey, ctx: &RowContext) -> String {
    pallas_extras::stake_credential_to_address(ctx.network, account.credential())
        .to_bech32()
        .unwrap_or_else(|_| "<invalid>".to_string())
}

fn render_pool(pool: dolos_cardano::PoolHash) -> String {
    bech32::encode::<bech32::Bech32>(POOL_HRP, pool.as_ref())
        .unwrap_or_else(|_| "<invalid>".to_string())
}

fn render_pool_amounts(entries: &[(dolos_cardano::PoolHash, u64)]) -> String {
    if entries.is_empty() {
        return "-".to_string();
    }

    entries
        .iter()
        .map(|(pool, amount)| format!("{}:{amount}", render_pool(*pool)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Wrapper around EpochState that outputs protocol parameters as CSV columns.
struct EpochPParams(EpochState);

impl TableRow for EpochPParams {
    type Log = EpochState;

    fn view(log: Self::Log) -> Self {
        EpochPParams(log)
    }

    fn header(format: Layout) -> Vec<&'static str> {
        match format {
            Layout::Table | Layout::Dbsync => vec![
                "epoch_no",
                "protocol_major",
                "protocol_minor",
                "min_fee_a",
                "min_fee_b",
                "key_deposit",
                "pool_deposit",
                "expansion_rate",
                "treasury_growth_rate",
                "decentralisation",
                "desired_pool_number",
                "min_pool_cost",
                "influence",
            ],
        }
    }

    fn row(&self, _slot: BlockSlot, _key: &(), _ctx: &RowContext) -> Vec<String> {
        let pparams = self.0.pparams.live();
        let pparams = match pparams.as_ref() {
            Some(p) => p,
            None => return Vec::new(),
        };

        let (major, minor) = pparams.protocol_version().unwrap_or((0, 0));

        fn rational_to_f64(r: &pallas::ledger::primitives::RationalNumber) -> f64 {
            if r.denominator == 0 {
                0.0
            } else {
                r.numerator as f64 / r.denominator as f64
            }
        }

        fn get_rational(pparams: &dolos_cardano::model::PParamsSet, kind: PParamKind) -> f64 {
            match pparams.get(kind) {
                Some(v) => match v {
                    dolos_cardano::model::PParamValue::ExpansionRate(r)
                    | dolos_cardano::model::PParamValue::TreasuryGrowthRate(r)
                    | dolos_cardano::model::PParamValue::DecentralizationConstant(r) => {
                        rational_to_f64(r)
                    }
                    dolos_cardano::model::PParamValue::PoolPledgeInfluence(r) => rational_to_f64(r),
                    _ => 0.0,
                },
                None => 0.0,
            }
        }

        vec![
            self.0.number.to_string(),
            major.to_string(),
            minor.to_string(),
            pparams.min_fee_a().unwrap_or(0).to_string(),
            pparams.min_fee_b().unwrap_or(0).to_string(),
            pparams.key_deposit().unwrap_or(0).to_string(),
            pparams.pool_deposit().unwrap_or(0).to_string(),
            get_rational(pparams, PParamKind::ExpansionRate).to_string(),
            get_rational(pparams, PParamKind::TreasuryGrowthRate).to_string(),
            get_rational(pparams, PParamKind::DecentralizationConstant).to_string(),
            pparams
                .desired_number_of_stake_pools()
                .unwrap_or(0)
                .to_string(),
            pparams.min_pool_cost().unwrap_or(0).to_string(),
            get_rational(pparams, PParamKind::PoolPledgeInfluence).to_string(),
        ]
    }
}

enum Formatter<T: TableRow> {
    Table(Table, PhantomData<T>),
    Csv,
}

impl<T: TableRow> Formatter<T> {
    fn new(format: Layout) -> Self {
        match format {
            Layout::Table => {
                let mut table = Table::new();
                table.set_header(T::header(Layout::Table));
                Self::Table(table, PhantomData::<T>)
            }
            Layout::Dbsync => {
                println!("{}", T::header(Layout::Dbsync).join(","));
                Self::Csv
            }
        }
    }

    fn write(&mut self, slot: BlockSlot, key: KeyOf<T>, value: T, ctx: &RowContext) {
        for row in value.rows(slot, &key, ctx) {
            match self {
                Formatter::Table(table, _) => {
                    table.add_row(row);
                }
                Formatter::Csv => println!("{}", row.join(",")),
            }
        }
    }

    fn flush(self) {
        match self {
            Formatter::Table(table, _) => println!("{table}"),
            Formatter::Csv => {}
        }
    }
}

/// One log record, as a JSON line.
#[derive(Serialize)]
struct Record<'a, T> {
    slot: BlockSlot,
    key: String,
    value: &'a T,
}

fn dump_logs<T: TableRow>(
    archive: &impl ArchiveStore,
    bounds: &Bounds,
    format: Format,
    ctx: &RowContext,
    slots: (Option<u64>, Option<u64>, Option<std::ops::Range<BlockSlot>>),
) -> miette::Result<()> {
    if ctx.layout == Layout::Dbsync && !T::DBSYNC {
        return Err(crate::output::Usage::new(
            format!("the {} namespace has no dbsync layout", T::Log::NS),
            None,
        )
        .into());
    }

    let (start_slot, end_slot, range) = slots;

    let within = |slot: BlockSlot| {
        start_slot.is_none_or(|start| slot >= start) && end_slot.is_none_or(|end| slot < end)
    };

    let logs = archive
        .iter_logs_typed::<T::Log>(range.map(LogRangeOf::slots))
        .into_diagnostic()
        .wrap_err("iterating logs")?
        .filter(|log| log.as_ref().map_or(true, |(slot, _, _)| within(*slot)));

    let logs = bounds.apply(logs);

    if format == Format::Json {
        for log in logs {
            let (slot, key, value) = log.into_diagnostic()?;

            emit_record(&Record {
                slot,
                key: EntityKey::of(&key).to_string(),
                value: &value,
            })?;
        }

        return Ok(());
    }

    let mut formatter = Formatter::<T>::new(ctx.layout);

    for log in logs {
        let (slot, key, value) = log.into_diagnostic()?;
        formatter.write(slot, key, T::view(value), ctx);
    }

    formatter.flush();

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let output = args.output.checked()?;
    output.setup_tracing()?;

    if !NAMESPACES.contains(&args.namespace.as_str()) {
        return Err(super::unknown_namespace("log", &args.namespace, &NAMESPACES).into());
    }

    let stores = super::observe(config)?;
    let genesis = crate::common::open_genesis(config)?;
    let network = dolos_cardano::network_from_genesis(&genesis);

    let use_epoch_filter = args.from_epoch.is_some() || args.to_epoch.is_some();

    // The stakes and reward views head a column `epoch`, and only the summary
    // turns the log's slot key into one, so they pay the summary read in
    // every layout.
    let need_summary = use_epoch_filter
        || matches!(output.layout, Layout::Dbsync)
        || matches!(
            args.namespace.as_str(),
            "pool-epochs"
                | "drep-epochs"
                | "dreps"
                | "pools"
                | "proposals"
                | "account-epochs/rewards"
        );
    let summary = if need_summary {
        Some(
            load_chain_summary_from_state(&stores.state)
                .map_err(|err| miette::miette!("{err:?}"))?,
        )
    } else {
        None
    };

    let ctx = RowContext {
        network,
        layout: output.layout,
        summary,
    };

    let slots = if use_epoch_filter {
        let summary = ctx
            .summary
            .as_ref()
            .expect("chain summary available for epoch filtering");

        // `--to-epoch` is inclusive: the range ends where the next epoch starts.
        let end = args.to_epoch.map(|epoch| epoch.saturating_add(1));

        log_epoch_range_to_key_range(summary, args.from_epoch, end)
    } else {
        (None, None, None)
    };

    let archive = &stores.archive;
    let (bounds, format) = (&args.bounds, output.format.format);

    match args.namespace.as_str() {
        "pool-epochs" => dump_logs::<PoolEpochLog>(archive, bounds, format, &ctx, slots),
        "drep-epochs" => dump_logs::<DRepEpochLog>(archive, bounds, format, &ctx, slots),
        "dreps" => dump_logs::<DRepState>(archive, bounds, format, &ctx, slots),
        "pools" => dump_logs::<PoolState>(archive, bounds, format, &ctx, slots),
        "proposals" => dump_logs::<ProposalState>(archive, bounds, format, &ctx, slots),
        "account-epochs" => dump_logs::<AccountEpochLog>(archive, bounds, format, &ctx, slots),
        "account-epochs/rewards" => {
            dump_logs::<AccountEpochRewards>(archive, bounds, format, &ctx, slots)
        }
        "epochs" => dump_logs::<EpochState>(archive, bounds, format, &ctx, slots),
        "epochs/pparams" => dump_logs::<EpochPParams>(archive, bounds, format, &ctx, slots),
        _ => unreachable!("checked against NAMESPACES"),
    }
}
