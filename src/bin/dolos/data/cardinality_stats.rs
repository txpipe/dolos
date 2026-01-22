use comfy_table::Table;
use dolos_core::config::RootConfig;
use dolos_redb3::redb::{ReadableDatabase, ReadableTable as _, TableDefinition};
use miette::{bail, Context, IntoDiagnostic};
use redb_extras::buckets::BucketedKey;
use redb_extras::roaring::RoaringValue;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

const DEFAULT_TABLES: [&str; 10] = [
    "byaddress",
    "bypayment",
    "bystake",
    "byasset",
    "bydatum",
    "bymetadata",
    "bypolicy",
    "byscript",
    "byspenttxo",
    "bystakeactions",
];

#[derive(Clone, Copy)]
struct TableSpec {
    name: &'static str,
    def: TableDefinition<'static, BucketedKey<u64>, RoaringValue>,
}

impl TableSpec {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            def: TableDefinition::new(name),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Entry {
    table: &'static str,
    bucket: u64,
    base_key: u64,
    count: u64,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.count
            .cmp(&other.count)
            .then_with(|| self.table.cmp(other.table))
            .then_with(|| self.bucket.cmp(&other.bucket))
            .then_with(|| self.base_key.cmp(&other.base_key))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {
    /// number of entries to show per table
    #[arg(long, default_value_t = 20)]
    top: usize,

    /// archive roaring tables to scan (comma-separated or repeated)
    #[arg(long, value_delimiter = ',')]
    tables: Vec<String>,
}

fn default_table_specs() -> Vec<TableSpec> {
    DEFAULT_TABLES
        .iter()
        .map(|name| TableSpec::new(name))
        .collect()
}

fn push_top(heap: &mut BinaryHeap<Reverse<Entry>>, entry: Entry, limit: usize) {
    if limit == 0 {
        return;
    }

    if heap.len() < limit {
        heap.push(Reverse(entry));
        return;
    }

    if let Some(mut smallest) = heap.peek_mut() {
        if entry > smallest.0 {
            *smallest = Reverse(entry);
        }
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let archive = crate::common::open_archive_store(config)?;
    let rx = archive
        .db()
        .begin_read()
        .into_diagnostic()
        .context("opening archive read transaction")?;

    let table_specs = default_table_specs();
    let selected_tables = if args.tables.is_empty() {
        table_specs
    } else {
        let mut selected = Vec::new();

        for name in &args.tables {
            let name = name.to_ascii_lowercase();
            let Some(spec) = table_specs.iter().find(|spec| spec.name == name) else {
                bail!(
                    "unknown table '{name}'. Valid tables are: {}",
                    DEFAULT_TABLES.join(", ")
                );
            };

            selected.push(*spec);
        }

        selected
    };

    let mut table = Table::new();
    table.set_header(vec!["table", "rank", "bucket", "base_key", "count"]);

    for spec in selected_tables {
        let mut heap = BinaryHeap::new();
        let store = rx
            .open_table(spec.def)
            .into_diagnostic()
            .with_context(|| format!("opening roaring table {}", spec.name))?;

        for entry in store.iter().into_diagnostic()? {
            let (key, value) = entry.into_diagnostic()?;
            let key = key.value();
            let count = value.value().len();

            push_top(
                &mut heap,
                Entry {
                    table: spec.name,
                    bucket: key.bucket(),
                    base_key: *key.base_key(),
                    count,
                },
                args.top,
            );
        }

        let mut entries: Vec<Entry> = heap.into_iter().map(|entry| entry.0).collect();
        entries.sort_by(|a, b| b.cmp(a));

        for (index, entry) in entries.iter().enumerate() {
            table.add_row(vec![
                entry.table.to_string(),
                format!("{}", index + 1),
                format!("{}", entry.bucket),
                format!("0x{:016x}", entry.base_key),
                format!("{}", entry.count),
            ]);
        }
    }

    println!("{table}");

    Ok(())
}
