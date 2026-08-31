use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    eras::load_chain_summary_from_state,
    eras::log_epoch_range_to_key_range,
    model::{AccountEpochLog, PParamKind},
    ChainSummary, EpochState, StakeLog,
};
use dolos_core::config::RootConfig;
use miette::{IntoDiagnostic, WrapErr};
use pallas::codec::minicbor;
use pallas::ledger::addresses::Network as AddressNetwork;
use pallas::ledger::primitives::StakeCredential;

use crate::data::OutputFormat;
use dolos::prelude::*;
use dolos_cardano::pallas_extras;
use tracing_subscriber::{filter::Targets, prelude::*};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// output format
    #[arg(long, value_enum, default_value = "default")]
    format: OutputFormat,

    /// number of logs to skip
    #[arg(long, default_value = "0")]
    skip: usize,

    /// number of logs to dump
    #[arg(long, default_value = "100")]
    take: usize,

    /// start log epoch (inclusive)
    #[arg(long)]
    epoch_start: Option<u64>,

    /// end log epoch (exclusive)
    #[arg(long)]
    epoch_end: Option<u64>,
}

struct RowContext {
    network: AddressNetwork,
    format: OutputFormat,
    summary: Option<ChainSummary>,
}

trait TableRow: Entity {
    fn header(format: OutputFormat) -> Vec<&'static str>;

    /// The row this record renders as, or empty where it has nothing to say in
    /// this view.
    fn row(&self, key: &LogKey, ctx: &RowContext) -> Vec<String>;

    /// Every output row this record renders as.
    ///
    /// One record is one row for all but the merged account-epoch log, whose
    /// reward view renders one row per list element — which is the whole point
    /// of the lists, and the reason this is not just [`Self::row`].
    fn rows(&self, key: &LogKey, ctx: &RowContext) -> Vec<Vec<String>> {
        match self.row(key, ctx) {
            row if row.is_empty() => Vec::new(),
            row => vec![row],
        }
    }
}

impl TableRow for EpochState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        match format {
            OutputFormat::Default => vec![
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
            OutputFormat::Dbsync => vec![
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

    fn row(&self, _key: &LogKey, ctx: &RowContext) -> Vec<String> {
        match ctx.format {
            OutputFormat::Default => {
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
            OutputFormat::Dbsync => {
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

const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");

impl TableRow for StakeLog {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for stakes logs (#1032)");
        }
        vec![
            //"pool hex",
            "pool bech32",
            "epoch",
            "blocks minted",
            "active stake",
            "delegators count",
            "live pledge",
            "declared pledge",
            "total rewards",
            "operator share",
        ]
    }

    fn row(&self, key: &LogKey, ctx: &RowContext) -> Vec<String> {
        if matches!(ctx.format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for stakes logs (#1032)");
        }
        let temporal = TemporalKey::from(key.clone());
        let epoch = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());
        let entity_key = EntityKey::from(key.clone());
        let pool_hash = entity_key.as_ref()[..28].try_into().unwrap();
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool_hash).unwrap();

        vec![
            //format!("{}", pool_hex),
            format!("{}", pool_bech32),
            format!("{}", epoch),
            format!("{}", self.blocks_minted),
            format!("{}", self.total_stake),
            format!("{}", self.delegators_count),
            format!("{}", self.live_pledge),
            format!("{}", self.declared_pledge),
            format!("{}", self.total_rewards),
            format!("{}", self.operator_share),
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
    fn header(format: OutputFormat) -> Vec<&'static str> {
        match format {
            OutputFormat::Default => vec![
                "slot",
                "stake",
                "pool",
                "active stake",
                "member reward",
                "leader rewards",
                "deposit refunds",
            ],
            OutputFormat::Dbsync => vec!["stake", "pool", "lovelace"],
        }
    }

    fn row(&self, key: &LogKey, ctx: &RowContext) -> Vec<String> {
        let temporal = TemporalKey::from(key.clone());
        let entity = EntityKey::from(key.clone());
        let slot = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());

        let stake = render_stake_address(&entity, ctx);
        let pool = self
            .pool_id
            .map(render_pool)
            .unwrap_or_else(|| "-".to_string());

        match ctx.format {
            OutputFormat::Default => vec![
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
            OutputFormat::Dbsync => match (self.active_stake, self.pool_id) {
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

impl Entity for AccountEpochRewards {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, dolos_core::ChainError> {
        AccountEpochLog::decode_entity(ns, value).map(AccountEpochRewards)
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        AccountEpochLog::encode_entity(&value.0)
    }
}

impl TableRow for AccountEpochRewards {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        match format {
            OutputFormat::Default => vec!["epoch", "stake", "pool", "type", "amount"],
            OutputFormat::Dbsync => vec!["stake", "pool", "amount", "type", "earned_epoch"],
        }
    }

    fn row(&self, _key: &LogKey, _ctx: &RowContext) -> Vec<String> {
        unreachable!("the reward view renders through `rows`")
    }

    fn rows(&self, key: &LogKey, ctx: &RowContext) -> Vec<Vec<String>> {
        let temporal = TemporalKey::from(key.clone());
        let entity = EntityKey::from(key.clone());
        let slot = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());

        // A slot under a column headed `epoch` is off by orders of magnitude
        // and says nothing about being wrong, so absence renders as
        // absence.
        let epoch = ctx
            .summary
            .as_ref()
            .map(|summary| summary.slot_epoch(slot).0.to_string())
            .unwrap_or_else(|| "-".to_string());

        let stake = render_stake_address(&entity, ctx);

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
            .map(|(pool, amount, kind)| match ctx.format {
                OutputFormat::Default => vec![
                    epoch.clone(),
                    stake.clone(),
                    render_pool(pool),
                    kind.to_string(),
                    amount.to_string(),
                ],
                OutputFormat::Dbsync => vec![
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

/// An undecodable key means the row isn't a credential-keyed log at all (wrong
/// namespace, corruption). Report it as `<invalid>` rather than falling back to
/// a zero credential, which would render as a well-formed stake address and
/// silently mislabel the account — and collapse every failed row onto the same
/// address in a dbsync diff.
fn render_stake_address(entity: &EntityKey, ctx: &RowContext) -> String {
    decode_stake_credential(entity)
        .ok()
        .and_then(|credential| {
            pallas_extras::stake_credential_to_address(ctx.network, &credential)
                .to_bech32()
                .ok()
        })
        .unwrap_or_else(|| "<invalid>".to_string())
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

impl Entity for EpochPParams {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, dolos_core::ChainError> {
        EpochState::decode_entity(ns, value).map(EpochPParams)
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        EpochState::encode_entity(&value.0)
    }
}

impl TableRow for EpochPParams {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        match format {
            OutputFormat::Default | OutputFormat::Dbsync => vec![
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

    fn row(&self, _key: &LogKey, _ctx: &RowContext) -> Vec<String> {
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
    // TODO
    // Json,
}

impl<T: TableRow> Formatter<T> {
    fn new(format: OutputFormat) -> Self {
        match format {
            OutputFormat::Default => {
                let mut table = Table::new();
                table.set_header(T::header(OutputFormat::Default));
                Self::Table(table, PhantomData::<T>)
            }
            OutputFormat::Dbsync => {
                println!("{}", T::header(OutputFormat::Dbsync).join(","));
                Self::Csv
            }
        }
    }

    fn write(&mut self, key: LogKey, value: T, ctx: &RowContext) {
        for row in value.rows(&key, ctx) {
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

#[allow(clippy::too_many_arguments)]
fn dump_logs<T: TableRow>(
    archive: &impl ArchiveStore,
    ns: Namespace,
    skip: usize,
    take: usize,
    ctx: &RowContext,
    start_slot: Option<u64>,
    end_slot: Option<u64>,
    range: Option<std::ops::Range<LogKey>>,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new(ctx.format);

    let mut skipped = 0usize;
    let mut taken = 0usize;
    let take_limit = if take == 0 { None } else { Some(take) };

    archive
        .iter_logs_typed(ns, range)
        .into_diagnostic()
        .wrap_err("iterating logs")?
        .for_each(|x| match x {
            Ok((key, value)) => {
                let log_slot = log_slot_from_key(&key);
                if let Some(start) = start_slot {
                    if log_slot < start {
                        return;
                    }
                }
                if let Some(end) = end_slot {
                    if log_slot >= end {
                        return;
                    }
                }

                if skipped < skip {
                    skipped += 1;
                    return;
                }

                if let Some(limit) = take_limit {
                    if taken >= limit {
                        return;
                    }
                }

                formatter.write(key, value, ctx);
                taken += 1;
            }
            Err(e) => panic!("{e}"),
        });

    formatter.flush();

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    setup_tracing_for_format(config, args.format)?;

    let archive = crate::common::open_archive_store(config)?;
    let genesis = crate::common::open_genesis_files(&config.genesis)?;
    let network = dolos_cardano::network_from_genesis(&genesis);

    let use_epoch_filter = args.epoch_start.is_some() || args.epoch_end.is_some();

    // The reward view heads a column `epoch`, and only the summary turns the
    // log's slot key into one, so it pays the state-store open in every format.
    let need_summary = use_epoch_filter
        || matches!(args.format, OutputFormat::Dbsync)
        || args.namespace == "account-epochs/rewards";
    let summary = if need_summary {
        let state = crate::common::open_state_store(config)?;
        Some(load_chain_summary_from_state(&state).map_err(|err| miette::miette!("{err:?}"))?)
    } else {
        None
    };

    let ctx = RowContext {
        network,
        format: args.format,
        summary,
    };

    let (start_slot, end_slot, range) = if use_epoch_filter {
        let summary = ctx
            .summary
            .as_ref()
            .expect("chain summary available for epoch filtering");
        log_epoch_range_to_key_range(summary, args.epoch_start, args.epoch_end)
    } else {
        (None, None, None)
    };

    match args.namespace.as_str() {
        "stakes" => dump_logs::<StakeLog>(
            &archive,
            "stakes",
            args.skip,
            args.take,
            &ctx,
            start_slot,
            end_slot,
            range.clone(),
        )?,
        "account-epochs" => dump_logs::<AccountEpochLog>(
            &archive,
            "account-epochs",
            args.skip,
            args.take,
            &ctx,
            start_slot,
            end_slot,
            range.clone(),
        )?,
        "account-epochs/rewards" => dump_logs::<AccountEpochRewards>(
            &archive,
            "account-epochs",
            args.skip,
            args.take,
            &ctx,
            start_slot,
            end_slot,
            range.clone(),
        )?,
        "epochs" => dump_logs::<EpochState>(
            &archive, "epochs", args.skip, args.take, &ctx, start_slot, end_slot, range,
        )?,
        "epochs/pparams" => dump_logs::<EpochPParams>(
            &archive, "epochs", args.skip, args.take, &ctx, start_slot, end_slot, range,
        )?,
        _ => todo!(),
    }

    Ok(())
}

fn setup_tracing_for_format(config: &RootConfig, format: OutputFormat) -> miette::Result<()> {
    if matches!(format, OutputFormat::Dbsync) {
        let filter = Targets::new().with_default(tracing::Level::ERROR);

        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(filter)
            .init();

        tracing_log::LogTracer::init().ok();

        return Ok(());
    }

    crate::common::setup_tracing(&config.logging, &config.telemetry)
}

fn decode_stake_credential(key: &EntityKey) -> Result<StakeCredential, dolos_core::ChainError> {
    let mut decoder = minicbor::Decoder::new(key.as_ref());
    decoder.decode().map_err(Into::into)
}

fn log_slot_from_key(key: &LogKey) -> u64 {
    let temporal = TemporalKey::from(key.clone());
    u64::from_be_bytes(temporal.as_ref().try_into().unwrap())
}
