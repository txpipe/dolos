use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    model::AccountState, EpochState, EpochValue, EraSummary, PendingRewardState, PoolSnapshot,
    PoolState, ProposalAction, ProposalState,
};
use miette::{Context, IntoDiagnostic};
use tracing_subscriber::{filter::Targets, prelude::*};

use crate::data::OutputFormat;
use dolos::prelude::*;
use dolos_cardano::{network_from_genesis, pallas_extras};
use pallas::ledger::addresses::Network as AddressNetwork;
use pallas::ledger::primitives::Epoch;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// output format
    #[arg(long, value_enum, default_value = "default")]
    format: OutputFormat,

    /// count of entities to dump
    #[arg(long, default_value = "100")]
    count: usize,
}

trait TableRow: Entity {
    fn header(format: OutputFormat) -> Vec<&'static str>;
    fn row(&self, key: &EntityKey, network: AddressNetwork, format: OutputFormat) -> Vec<String>;
}

fn format_stake_at(account: &AccountState, epoch: Epoch) -> String {
    let stake = account
        .stake
        .snapshot_at(epoch)
        .map(|x| x.total().to_string());

    let pool = account
        .delegated_pool_at(epoch)
        .map(|x| hex::encode(x.as_ref())[..3].to_string());

    format!(
        "{} ({})",
        stake.unwrap_or_else(|| "x".to_string()),
        pool.unwrap_or_else(|| "x".to_string())
    )
}

impl TableRow for AccountState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for accounts state");
        }
        vec![
            "cred",
            "reg",
            "dereg",
            "stake (-3)",
            "stake (-2)",
            "stake (-1)",
            "live stake",
            "rewards",
            "withdrawals",
            "epoch version",
        ]
    }

    fn row(&self, key: &EntityKey, _network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for accounts state");
        }
        let epoch = self.stake.epoch().unwrap_or_default();

        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.registered_at.unwrap_or_default()),
            format!("{}", self.deregistered_at.unwrap_or_default()),
            format_stake_at(self, epoch - 3),
            format_stake_at(self, epoch - 2),
            format_stake_at(self, epoch - 1),
            format_stake_at(self, epoch),
            format!(
                "{},{},{}",
                self.stake.epoch().unwrap_or_default(),
                self.pool.epoch().unwrap_or_default(),
                self.drep.epoch().unwrap_or_default(),
            ),
        ]
    }
}

impl TableRow for EpochState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for epochs state");
        }
        vec![
            "number",
            "version",
            "pot reserves",
            "pot utxos",
            "pot treasury",
            "pot deposits",
            "pot rewards",
            "pot fees",
            "gathered fees",
            "pparams",
            "nonce",
        ]
    }

    fn row(&self, _key: &EntityKey, _network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for epochs state");
        }
        let pparams = self.pparams.live();

        vec![
            format!("{}", self.number),
            format!(
                "{}",
                pparams
                    .as_ref()
                    .and_then(|x| x.protocol_major())
                    .unwrap_or_default()
            ),
            format!("{}", self.initial_pots.reserves),
            format!("{}", self.initial_pots.utxos),
            format!("{}", self.initial_pots.treasury),
            format!("{}", self.initial_pots.obligations()),
            format!("{}", self.initial_pots.rewards),
            format!("{}", self.initial_pots.fees),
            format!(
                "{}",
                self.rolling
                    .live()
                    .map(|x| x.gathered_fees)
                    .unwrap_or_default()
            ),
            format!(
                "{}",
                self.pparams.live().map(|x| x.len()).unwrap_or_default()
            ),
            format!(
                "{}",
                self.nonces
                    .as_ref()
                    .map(|x| hex::encode(x.active))
                    .unwrap_or("".to_string())
            ),
        ]
    }
}

impl TableRow for EraSummary {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            return vec!["protocol", "start_epoch", "epoch_length", "slot_length"];
        }
        vec![
            "key",
            "start epoch",
            "start slot",
            "start timestamp",
            "end epoch",
            "end slot",
            "end timestamp",
            "epoch length",
            "slot length",
        ]
    }

    fn row(&self, key: &EntityKey, _network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            return vec![
                self.protocol.to_string(),
                self.start.epoch.to_string(),
                self.epoch_length.to_string(),
                self.slot_length.to_string(),
            ];
        }

        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.start.epoch),
            format!("{}", self.start.slot),
            format!("{}", self.start.timestamp),
            format!("{}", self.end.as_ref().map(|x| x.epoch).unwrap_or_default()),
            format!("{}", self.end.as_ref().map(|x| x.slot).unwrap_or_default()),
            format!(
                "{}",
                self.end.as_ref().map(|x| x.timestamp).unwrap_or_default()
            ),
            format!("{}", self.epoch_length),
            format!("{}", self.slot_length),
        ]
    }
}

impl TableRow for PendingRewardState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for pending rewards state");
        }
        vec![
            "stake bech32",
            "stake hex",
            "total",
            "spendable",
            "leader count",
            "member count",
            "leader total",
            "member total",
        ]
    }

    fn row(&self, key: &EntityKey, network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for pending rewards state");
        }
        let stake_hex = hex::encode(key.as_ref());
        let stake_bech32 = pallas_extras::stake_credential_to_address(network, &self.credential)
            .to_bech32()
            .unwrap_or_else(|_| "<invalid>".to_string());
        let leader_total: u64 = self.as_leader.iter().map(|(_, v)| v).sum();
        let member_total: u64 = self.as_delegator.iter().map(|(_, v)| v).sum();

        vec![
            stake_bech32,
            stake_hex,
            self.total_value().to_string(),
            self.is_spendable.to_string(),
            self.as_leader.len().to_string(),
            self.as_delegator.len().to_string(),
            leader_total.to_string(),
            member_total.to_string(),
        ]
    }
}

const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");

fn format_pool_epoch(values: &EpochValue<PoolSnapshot>, epoch_delta: u64) -> String {
    let epoch = values
        .epoch()
        .unwrap_or_default()
        .saturating_sub(epoch_delta);

    let snapshot = values.snapshot_at(epoch);

    format!(
        "{} {} ({})",
        snapshot.map(|x| x.is_retired).unwrap_or_default(),
        snapshot.map(|x| x.blocks_minted).unwrap_or_default(),
        epoch,
    )
}

impl TableRow for PoolState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for pools state");
        }
        vec![
            "key",
            "pool bech32",
            "registered at",
            "retiring epoch",
            "pledge (go)",
            "pledge (set)",
            "pledge (mark)",
            "pledge (live)",
            "pledge (next)",
        ]
    }

    fn row(&self, key: &EntityKey, _network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for pools state");
        }
        let entity_key = key.clone();
        let pool_hash = entity_key.as_ref()[..28].try_into().unwrap();
        let pool_hex = hex::encode(pool_hash);
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool_hash).unwrap();

        vec![
            format!("{}", pool_hex),
            format!("{}", pool_bech32),
            format!("{}", self.register_slot),
            format!(
                "{}",
                self.retiring_epoch
                    .map(|x| x.to_string())
                    .unwrap_or_default()
            ),
            format_pool_epoch(&self.snapshot, 3),
            format_pool_epoch(&self.snapshot, 2),
            format_pool_epoch(&self.snapshot, 1),
            format_pool_epoch(&self.snapshot, 0),
        ]
    }
}

impl TableRow for ProposalState {
    fn header(format: OutputFormat) -> Vec<&'static str> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for proposals state");
        }
        vec![
            "key",
            "tx",
            "idx",
            "action",
            "max epoch",
            "ratified epoch",
            "canceled epoch",
            "deposit",
            "reward account",
        ]
    }

    fn row(&self, key: &EntityKey, _network: AddressNetwork, format: OutputFormat) -> Vec<String> {
        if matches!(format, OutputFormat::Dbsync) {
            todo!("dbsync format not supported for proposals state");
        }
        let action = match &self.action {
            ProposalAction::ParamChange(x) => format!("Params({})", x.len()),
            ProposalAction::HardFork((x, _)) => {
                format!("HardFork({:?})", x)
            }
            ProposalAction::TreasuryWithdrawal(x) => format!("TreasuryWithdrawal({:?})", x.len()),
            ProposalAction::Other => "Other".to_string(),
        };

        vec![
            format!("{}", hex::encode(key)),
            format!("{}", hex::encode(self.tx)),
            format!("{}", self.idx),
            format!("{}", action),
            format!(
                "{}",
                self.max_epoch.map(|x| x.to_string()).unwrap_or_default()
            ),
            format!(
                "{}",
                self.ratified_epoch
                    .map(|x| x.to_string())
                    .unwrap_or_default()
            ),
            format!(
                "{}",
                self.canceled_epoch
                    .map(|x| x.to_string())
                    .unwrap_or_default()
            ),
            format!(
                "{}",
                self.deposit.map(|x| x.to_string()).unwrap_or_default()
            ),
            format!("{}", self.reward_account.is_some()),
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

    fn write(&mut self, key: EntityKey, value: T, network: AddressNetwork, format: OutputFormat) {
        match self {
            Formatter::Table(table, _) => {
                let row = value.row(&key, network, format);
                table.add_row(row);
            }
            Formatter::Csv => {
                let row = value.row(&key, network, format);
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

fn dump_state<T: TableRow>(
    state: &impl StateStore,
    ns: Namespace,
    count: usize,
    network: AddressNetwork,
    format: OutputFormat,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new(format);

    let iter = state
        .iter_entities_typed::<T>(ns, None)
        .into_diagnostic()
        .context("iterating entities")?;

    if count == 0 {
        iter.for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value, network, format),
            Err(e) => panic!("{e}"),
        });
    } else {
        iter.take(count).for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value, network, format),
            Err(e) => panic!("{e}"),
        });
    }

    formatter.flush();

    Ok(())
}

use dolos_core::config::RootConfig;

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    setup_tracing_for_format(config, args.format)?;

    let state = crate::common::open_state_store(config)?;
    let genesis = crate::common::open_genesis_files(&config.genesis)?;
    let network = network_from_genesis(&genesis);

    match args.namespace.as_str() {
        "eras" => dump_state::<EraSummary>(&state, "eras", args.count, network, args.format)?,
        "epochs" => dump_state::<EpochState>(&state, "epochs", args.count, network, args.format)?,
        "accounts" => {
            dump_state::<AccountState>(&state, "accounts", args.count, network, args.format)?
        }
        "pools" => dump_state::<PoolState>(&state, "pools", args.count, network, args.format)?,
        "proposals" => {
            dump_state::<ProposalState>(&state, "proposals", args.count, network, args.format)?
        }
        "pending-rewards" => {
            dump_state::<PendingRewardState>(
                &state,
                "pending_rewards",
                args.count,
                network,
                args.format,
            )?;
        }
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
