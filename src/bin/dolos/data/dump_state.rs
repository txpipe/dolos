use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    model::AccountState, EpochState, EpochValue, EraSummary, PoolSnapshot, PoolState,
    ProposalAction, ProposalState,
};
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;
use pallas::ledger::primitives::Epoch;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// count of entities to dump
    #[arg(long, default_value = "100")]
    count: usize,
}

trait TableRow: Entity {
    fn header() -> Vec<&'static str>;
    fn row(&self, key: &EntityKey) -> Vec<String>;
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
    fn header() -> Vec<&'static str> {
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

    fn row(&self, key: &EntityKey) -> Vec<String> {
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
    fn header() -> Vec<&'static str> {
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

    fn row(&self, _key: &EntityKey) -> Vec<String> {
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
    fn header() -> Vec<&'static str> {
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

    fn row(&self, key: &EntityKey) -> Vec<String> {
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
    fn header() -> Vec<&'static str> {
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

    fn row(&self, key: &EntityKey) -> Vec<String> {
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
    fn header() -> Vec<&'static str> {
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

    fn row(&self, key: &EntityKey) -> Vec<String> {
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
    // TODO
    // Json,
}

impl<T: TableRow> Formatter<T> {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(T::header());

        Self::Table(table, PhantomData::<T>)
    }

    fn write(&mut self, key: EntityKey, value: T) {
        match self {
            Formatter::Table(table, _) => {
                let row = value.row(&key);
                table.add_row(row);
            }
        }
    }

    fn flush(self) {
        match self {
            Formatter::Table(table, _) => println!("{table}"),
        }
    }
}

fn dump_state<T: TableRow>(
    state: &impl StateStore,
    ns: Namespace,
    count: usize,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new_table();

    state
        .iter_entities_typed::<T>(ns, None)
        .into_diagnostic()
        .context("iterating entities")?
        .take(count)
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(e) => panic!("{e}"),
        });

    formatter.flush();

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "eras" => dump_state::<EraSummary>(&state, "eras", args.count)?,
        "epochs" => dump_state::<EpochState>(&state, "epochs", args.count)?,
        "accounts" => dump_state::<AccountState>(&state, "accounts", args.count)?,
        "pools" => dump_state::<PoolState>(&state, "pools", args.count)?,
        "proposals" => dump_state::<ProposalState>(&state, "proposals", args.count)?,
        _ => todo!(),
    }

    Ok(())
}
