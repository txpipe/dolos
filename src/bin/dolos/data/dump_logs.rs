use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    eras::load_chain_summary_from_state, eras::log_epoch_range_to_key_range, model::RewardLog,
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
    fn row(&self, key: &LogKey, ctx: &RowContext) -> Vec<String>;
}

impl TableRow for RewardLog {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        match format {
            OutputFormat::Default => vec!["epoch", "slot", "as leader", "amount"],
            OutputFormat::Dbsync => vec!["stake", "pool", "amount", "type", "earned_epoch"],
        }
    }

    fn row(&self, key: &LogKey, ctx: &RowContext) -> Vec<String> {
        let temporal = TemporalKey::from(key.clone());
        let entity = EntityKey::from(key.clone());
        let slot = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());

        match ctx.format {
            OutputFormat::Default => vec![
                format!("{}", slot),
                format!("{}", hex::encode(entity.as_ref())),
                format!("{}", self.as_leader),
                format!("{}", self.amount),
            ],
            OutputFormat::Dbsync => {
                if self.amount == 0 {
                    return Vec::new();
                }
                let credential = decode_stake_credential(&entity)
                    .unwrap_or_else(|_| StakeCredential::AddrKeyhash([0; 28].into()));
                let stake = pallas_extras::stake_credential_to_address(ctx.network, &credential)
                    .to_bech32()
                    .unwrap_or_else(|_| "<invalid>".to_string());
                let pool_id = bech32::encode::<bech32::Bech32>(POOL_HRP, &self.pool_id)
                    .unwrap_or_else(|_| "<invalid>".to_string());
                let reward_type = if self.as_leader { "leader" } else { "member" };
                let earned_epoch = match ctx.summary.as_ref() {
                    Some(summary) => summary.slot_epoch(slot).0.saturating_sub(1),
                    None => slot.saturating_sub(1),
                };

                vec![
                    stake,
                    pool_id,
                    self.amount.to_string(),
                    reward_type.to_string(),
                    earned_epoch.to_string(),
                ]
            }
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
            ],
            OutputFormat::Dbsync => vec![
                "epoch_no",
                "treasury",
                "reserves",
                "rewards",
                "utxo",
                "deposits_stake",
                "fees",
                "nonce",
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
                ]
            }
            OutputFormat::Dbsync => {
                let nonce = self
                    .nonces
                    .as_ref()
                    .map(|x| hex::encode(x.active))
                    .unwrap_or_default();

                vec![
                    self.number.to_string(),
                    self.initial_pots.treasury.to_string(),
                    self.initial_pots.reserves.to_string(),
                    self.initial_pots.rewards.to_string(),
                    self.initial_pots.utxos.to_string(),
                    self.initial_pots.stake_deposits().to_string(),
                    self.initial_pots.fees.to_string(),
                    nonce,
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
            todo!("dbsync format not supported for stakes logs");
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
            todo!("dbsync format not supported for stakes logs");
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
        match self {
            Formatter::Table(table, _) => {
                let row = value.row(&key, ctx);
                table.add_row(row);
            }
            Formatter::Csv => {
                let row = value.row(&key, ctx);
                if row.is_empty() {
                    return;
                }
                println!("{}", row.join(","));
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
    let need_summary = use_epoch_filter || matches!(args.format, OutputFormat::Dbsync);
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
        "rewards" => dump_logs::<RewardLog>(
            &archive,
            "rewards",
            args.skip,
            args.take,
            &ctx,
            start_slot,
            end_slot,
            range.clone(),
        )?,
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
        "epochs" => dump_logs::<EpochState>(
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

    crate::common::setup_tracing(&config.logging)
}

fn decode_stake_credential(key: &EntityKey) -> Result<StakeCredential, dolos_core::ChainError> {
    let mut decoder = minicbor::Decoder::new(key.as_ref());
    decoder.decode().map_err(Into::into)
}

fn log_slot_from_key(key: &LogKey) -> u64 {
    let temporal = TemporalKey::from(key.clone());
    u64::from_be_bytes(temporal.as_ref().try_into().unwrap())
}
