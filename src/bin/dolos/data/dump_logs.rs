use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{EpochState, RewardLog};
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
        vec!["slot", "pool", "as leader", "amount"]
    }

    fn row(&self, key: &LogKey) -> Vec<String> {
        let temporal = TemporalKey::from(key.clone());
        let temporal = u64::from_be_bytes(temporal.as_ref().try_into().unwrap());

        vec![
            format!("{}", temporal),
            format!("{}", hex::encode(&self.pool_id)),
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
            "gathered fees",
            "gathered deposits",
            "decayed deposits",
            "reserves",
            "utxos",
            "treasury",
            "to treasury",
            "to distribute",
            "nonce",
        ]
    }

    fn row(&self, _key: &LogKey) -> Vec<String> {
        vec![
            format!("{}", self.number),
            format!("{}", self.pparams.protocol_major().unwrap_or_default()),
            format!("{}", self.gathered_fees),
            format!("{}", self.gathered_deposits),
            format!("{}", self.decayed_deposits),
            format!("{}", self.reserves),
            format!("{}", self.utxos),
            format!("{}", self.treasury),
            format!("{}", self.rewards_to_treasury.unwrap_or_default()),
            format!("{}", self.rewards_to_distribute.unwrap_or_default()),
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
        _ => todo!(),
    }

    Ok(())
}
