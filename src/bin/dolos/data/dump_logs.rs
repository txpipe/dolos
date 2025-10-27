use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{model::RewardLog, EpochState, StakeLog};
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// number of logs to skip
    #[arg(long, default_value = "0")]
    skip: usize,

    /// number of logs to dump
    #[arg(long, default_value = "100")]
    take: usize,
}

trait TableRow: Entity {
    fn header() -> Vec<&'static str>;
    fn row(&self, key: &LogKey) -> Vec<String>;
}

impl TableRow for RewardLog {
    fn header() -> Vec<&'static str> {
        vec!["epoch", "slot", "as leader", "amount"]
    }

    fn row(&self, key: &LogKey) -> Vec<String> {
        let temporal = TemporalKey::from(key.clone());
        let entity = EntityKey::from(key.clone());
        let epoch = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());

        vec![
            format!("{}", epoch),
            format!("{}", hex::encode(entity.as_ref())),
            format!("{}", self.as_leader),
            format!("{}", self.amount),
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
            "stake deposits",
            "drep deposits",
            "proposal deposits",
            "pot rewards",
            "pot fees",
            "gathered fees",
            "pparams",
            "blocks",
        ]
    }

    fn row(&self, _key: &LogKey) -> Vec<String> {
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
}

const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");

impl TableRow for StakeLog {
    fn header() -> Vec<&'static str> {
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

    fn row(&self, key: &LogKey) -> Vec<String> {
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
    // TODO
    // Json,
}

impl<T: TableRow> Formatter<T> {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(T::header());

        Self::Table(table, PhantomData::<T>)
    }

    fn write(&mut self, key: LogKey, value: T) {
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

fn dump_logs<T: TableRow>(
    archive: &impl ArchiveStore,
    ns: Namespace,
    skip: usize,
    take: usize,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new_table();

    archive
        .iter_logs_typed(ns, None)
        .into_diagnostic()
        .context("iterating logs")?
        .skip(skip)
        .take(take)
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(e) => panic!("{e}"),
        });

    formatter.flush();

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let archive = crate::common::open_archive_store(config)?;

    match args.namespace.as_str() {
        "rewards" => dump_logs::<RewardLog>(&archive, "rewards", args.skip, args.take)?,
        "stakes" => dump_logs::<StakeLog>(&archive, "stakes", args.skip, args.take)?,
        "epochs" => dump_logs::<EpochState>(&archive, "epochs", args.skip, args.take)?,
        _ => todo!(),
    }

    Ok(())
}
