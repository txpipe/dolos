use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    model::AccountState, DRepState, DatumState, EpochState, EpochValue, EraState,
    PendingRewardState, PoolSnapshot, PoolState, ProposalAction, ProposalState,
};
use miette::{Context, IntoDiagnostic};
use serde::Serialize;

use super::{Bounds, Layout, Output};
use crate::output::{emit_record, Format};
use dolos::prelude::*;
use dolos_cardano::{network_from_genesis, pallas_extras};
use pallas::ledger::addresses::Network as AddressNetwork;
use pallas::ledger::primitives::{conway::DRep, Epoch};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    #[command(flatten)]
    bounds: Bounds,

    #[command(flatten)]
    output: Output,
}

const NAMESPACES: [&str; 8] = [
    "eras",
    "epochs",
    "accounts",
    "pools",
    "proposals",
    "dreps",
    "pending-rewards",
    "datums",
];

trait TableRow: StateEntity + Serialize {
    /// Whether the entity has a dbsync layout.
    const DBSYNC: bool = false;

    fn header(format: Layout) -> Vec<&'static str>;
    fn row(&self, key: &Self::Key, network: AddressNetwork, format: Layout) -> Vec<String>;
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
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for accounts state (#1032)");
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

    fn row(&self, key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for accounts state (#1032)");
        }
        let epoch = self.stake.epoch().unwrap_or_default();

        vec![
            format!("{}", EntityKey::of(key)),
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
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for epochs state (#1032)");
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

    fn row(&self, _key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for epochs state (#1032)");
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

impl TableRow for EraState {
    const DBSYNC: bool = true;

    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
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

    fn row(&self, key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            return vec![
                self.protocol.to_string(),
                self.start.epoch.to_string(),
                self.epoch_length.to_string(),
                self.slot_length.to_string(),
            ];
        }

        vec![
            format!("{}", EntityKey::of(key)),
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
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pending rewards state (#1032)");
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

    fn row(&self, key: &Self::Key, network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pending rewards state (#1032)");
        }
        let stake_hex = EntityKey::of(key).to_string();
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

impl TableRow for DatumState {
    const DBSYNC: bool = true;

    fn header(_format: Layout) -> Vec<&'static str> {
        vec!["hash", "refcount", "bytes"]
    }

    fn row(&self, key: &Self::Key, _network: AddressNetwork, _format: Layout) -> Vec<String> {
        vec![
            hex::encode(key.0),
            self.refcount.to_string(),
            hex::encode(&self.bytes),
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
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pools state (#1032)");
        }
        vec![
            "key",
            "pool bech32",
            "retiring epoch",
            "calidus key",
            "pledge (go)",
            "pledge (set)",
            "pledge (mark)",
            "pledge (live)",
            "pledge (next)",
        ]
    }

    fn row(&self, key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for pools state (#1032)");
        }
        let pool_hash: &[u8] = key.0.as_ref();
        let pool_hex = hex::encode(pool_hash);
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool_hash).unwrap();

        vec![
            format!("{}", pool_hex),
            format!("{}", pool_bech32),
            format!(
                "{}",
                self.retiring_epoch
                    .map(|x| x.to_string())
                    .unwrap_or_default()
            ),
            self.calidus
                .as_ref()
                .and_then(|x| x.pub_key)
                .map(hex::encode)
                .unwrap_or_default(),
            format_pool_epoch(&self.snapshot, 3),
            format_pool_epoch(&self.snapshot, 2),
            format_pool_epoch(&self.snapshot, 1),
            format_pool_epoch(&self.snapshot, 0),
        ]
    }
}

impl TableRow for DRepState {
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for dreps state (#1032)");
        }
        vec![
            "credential",
            "kind",
            "registered slot",
            "unregistered slot",
            "expiry epoch",
            "expired",
            "deposit",
        ]
    }

    fn row(&self, _key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for dreps state (#1032)");
        }

        let (credential, kind) = match &self.identifier {
            DRep::Key(hash) => (hex::encode(hash), "key"),
            DRep::Script(hash) => (hex::encode(hash), "script"),
            DRep::Abstain => (String::new(), "abstain"),
            DRep::NoConfidence => (String::new(), "no-confidence"),
        };

        vec![
            credential,
            kind.to_string(),
            self.registered_at
                .map(|(slot, _)| slot.to_string())
                .unwrap_or_default(),
            self.unregistered_at
                .map(|(slot, _)| slot.to_string())
                .unwrap_or_default(),
            self.expiry.current.to_string(),
            self.expired.to_string(),
            self.deposit.to_string(),
        ]
    }
}

impl TableRow for ProposalState {
    fn header(format: Layout) -> Vec<&'static str> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for proposals state (#1032)");
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

    fn row(&self, key: &Self::Key, _network: AddressNetwork, format: Layout) -> Vec<String> {
        if matches!(format, Layout::Dbsync) {
            unreachable!("dbsync format not supported for proposals state (#1032)");
        }
        let action = match &self.action {
            ProposalAction::ParamChange(x) => format!("Params({})", x.len()),
            ProposalAction::HardFork((x, _)) => {
                format!("HardFork({:?})", x)
            }
            ProposalAction::TreasuryWithdrawal(x) => format!("TreasuryWithdrawal({:?})", x.len()),
            ProposalAction::Other => "Other".to_string(),
            ProposalAction::NoConfidence => "NoConfidence".to_string(),
            ProposalAction::UpdateCommittee {
                to_remove, to_add, ..
            } => {
                format!("UpdateCommittee(-{}, +{})", to_remove.len(), to_add.len())
            }
            ProposalAction::NewConstitution { .. } => "NewConstitution".to_string(),
            ProposalAction::Info => "Info".to_string(),
        };

        vec![
            format!("{}", EntityKey::of(key)),
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

    fn write(&mut self, key: T::Key, value: T, network: AddressNetwork, format: Layout) {
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

/// One entity, as a JSON line.
#[derive(Serialize)]
struct Record<'a, T> {
    key: String,
    value: &'a T,
}

fn dump_state<T: TableRow>(
    state: &impl StateStore,
    bounds: &Bounds,
    network: AddressNetwork,
    format: Format,
    layout: Layout,
) -> miette::Result<()> {
    if layout == Layout::Dbsync && !T::DBSYNC {
        return Err(crate::output::Usage::new(
            format!("the {} namespace has no dbsync layout", T::NS),
            None,
        )
        .into());
    }

    let entities = state
        .iter_entities_typed::<T>(None)
        .into_diagnostic()
        .context("iterating entities")?;

    let entities = bounds.apply_fallible(entities);

    if format == Format::Json {
        for entity in entities {
            let (key, value) = entity.into_diagnostic()?;

            emit_record(&Record {
                key: EntityKey::of(&key).to_string(),
                value: &value,
            })?;
        }

        return Ok(());
    }

    let mut formatter = Formatter::<T>::new(layout);

    for entity in entities {
        let (key, value) = entity.into_diagnostic()?;
        formatter.write(key, value, network, layout);
    }

    formatter.flush();

    Ok(())
}

use dolos_core::config::RootConfig;

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let output = args.output.checked()?;
    output.setup_tracing()?;

    if !NAMESPACES.contains(&args.namespace.as_str()) {
        return Err(super::unknown_namespace("state", &args.namespace, &NAMESPACES).into());
    }

    let state = super::observe_state(config)?;
    let genesis = crate::common::open_genesis(config)?;
    let network = network_from_genesis(&genesis);

    let state = &state;
    let bounds = &args.bounds;
    let (format, layout) = (output.format.format, output.layout);

    match args.namespace.as_str() {
        "eras" => dump_state::<EraState>(state, bounds, network, format, layout),
        "epochs" => dump_state::<EpochState>(state, bounds, network, format, layout),
        "accounts" => dump_state::<AccountState>(state, bounds, network, format, layout),
        "pools" => dump_state::<PoolState>(state, bounds, network, format, layout),
        "proposals" => dump_state::<ProposalState>(state, bounds, network, format, layout),
        "dreps" => dump_state::<DRepState>(state, bounds, network, format, layout),
        "pending-rewards" => {
            dump_state::<PendingRewardState>(state, bounds, network, format, layout)
        }
        "datums" => dump_state::<DatumState>(state, bounds, network, format, layout),
        _ => unreachable!("checked against NAMESPACES"),
    }
}
