use comfy_table::Table;
use dolos::storage::{Access, ArchiveStoreBackend, OpenError, StateStoreBackend};
use dolos_core::config::RootConfig;
use dolos_redb3::TableFootprint;
use miette::{bail, IntoDiagnostic as _};
use serde::Serialize;
use serde_json::{json, Value};

use crate::output::FormatArgs;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    format: FormatArgs,
}

/// Each persistent store's footprint, in the shape its engine reports.
#[derive(Debug, Serialize)]
struct Report {
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive: Option<Value>,
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sections = [("state", &self.state), ("archive", &self.archive)];
        let mut first = true;

        for (store, section) in sections {
            let Some(section) = section else {
                continue;
            };

            if !std::mem::take(&mut first) {
                writeln!(f)?;
            }

            let mut table = Table::new();

            if section["engine"] == "fjall" {
                let tuning = &section["tuning"];

                writeln!(
                    f,
                    "{store}: fjall, preset {}, cache {} MB, journal {} MB, {} workers, flush on commit {}",
                    cell(&tuning["preset"]),
                    cell(&tuning["cache_mb"]),
                    cell(&tuning["journal_mb"]),
                    cell(&tuning["workers"]),
                    cell(&tuning["flush_on_commit"]),
                )?;

                table.set_header([
                    "keyspace",
                    "disk bytes",
                    "memtable MB",
                    "L0 threshold",
                    "table MB",
                    "level ratio",
                ]);

                for (name, keyspace) in section["keyspaces"].as_object().into_iter().flatten() {
                    let tuning = &keyspace["tuning"];
                    let compaction = &tuning["compaction"];

                    table.add_row([
                        name.clone(),
                        cell(&keyspace["disk_bytes"]),
                        cell(&tuning["memtable_mb"]),
                        cell(&compaction["l0_threshold"]),
                        cell(&compaction["table_mb"]),
                        cell(&compaction["level_ratio"]),
                    ]);
                }
            } else {
                writeln!(f, "{store}: {}", cell(&section["engine"]))?;

                table.set_header([
                    "table",
                    "rows",
                    "bytes per row",
                    "leaf fill",
                    "stored bytes",
                    "fragmented bytes",
                    "tree height",
                ]);

                for (name, footprint) in section["tables"].as_object().into_iter().flatten() {
                    table.add_row([
                        name.clone(),
                        cell(&footprint["rows"]),
                        cell(&footprint["bytes_per_row"]),
                        cell(&footprint["leaf_fill"]),
                        cell(&footprint["stored_bytes"]),
                        cell(&footprint["fragmented_bytes"]),
                        cell(&footprint["tree_height"]),
                    ]);
                }
            }

            write!(f, "{table}")?;
        }

        Ok(())
    }
}

/// A JSON value as a table cell: strings unquoted, absence as `-`.
fn cell(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

fn footprint_to_json(footprint: &TableFootprint) -> serde_json::Value {
    let stats = &footprint.stats;

    json!({
        "rows": footprint.rows,
        "bytes_per_row": footprint.bytes_per_row(),
        "leaf_fill": footprint.leaf_fill(),
        "stored_bytes": stats.stored_bytes(),
        "fragmented_bytes": stats.fragmented_bytes(),
        "leaf_pages": stats.leaf_pages(),
        "metadata_bytes": stats.metadata_bytes(),
        "tree_height": stats.tree_height(),
        "branch_pages": stats.branch_pages(),
    })
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    // Each store the dataset holds, so one that holds only an archive is
    // reported too; with neither, the state's absence is the refusal.
    let archive = match dolos::storage::open_existing_archive(config, Access::Observe) {
        Err(OpenError::Missing { .. }) => None,
        archive => Some(archive?),
    };

    let state = match dolos::storage::open_existing_state(config, Access::Observe) {
        Err(OpenError::Missing { .. })
            if matches!(archive, Some(ArchiveStoreBackend::Fjall(_))) =>
        {
            None
        }
        state => Some(state?),
    };

    let mut report = Report {
        state: None,
        archive: None,
    };

    // Each engine reports the shape it actually has: redb gets per-table
    // B-tree footprints (rows, leaf fill, page counts), fjall gets per-keyspace
    // disk footprints — LSM trees have no leaf-fill analogue and faking one
    // would mislead. Stores on neither engine are left out rather than
    // aborting the command.
    match &state {
        None | Some(StateStoreBackend::Memory(_)) => (),
        Some(StateStoreBackend::Redb(state)) => {
            let stats = state.utxoset_stats().into_diagnostic()?;

            report.state = Some(redb_section(stats));
        }
        Some(StateStoreBackend::Fjall(state)) => {
            report.state = Some(fjall_section(state.disk_usage(), &state.tuning()));
        }
    }

    match &archive {
        Some(ArchiveStoreBackend::Fjall(archive)) => {
            report.archive = Some(fjall_section(archive.disk_usage(), &archive.tuning()));
        }
        None
        | Some(
            ArchiveStoreBackend::Memory(_)
            | ArchiveStoreBackend::LogsOnly(_)
            | ArchiveStoreBackend::EntityReads(_)
            | ArchiveStoreBackend::NoOp(_),
        ) => (),
    }

    if report.state.is_none() && report.archive.is_none() {
        bail!("no persistent store backends are configured, nothing to report");
    }

    crate::output::emit(&report, &args.format)
}

fn redb_section<'a>(
    stats: impl IntoIterator<Item = (&'a str, dolos_redb3::redb::TableStats)>,
) -> serde_json::Value {
    let tables: serde_json::Map<_, _> = stats
        .into_iter()
        .map(|(name, stats)| {
            (
                name.to_string(),
                footprint_to_json(&TableFootprint::new(None, stats)),
            )
        })
        .collect();

    json!({ "engine": "redb", "tables": serde_json::Value::Object(tables) })
}

fn fjall_section(
    usage: Vec<(&'static str, u64, std::path::PathBuf)>,
    tuning: &dolos_fjall::preset::Tuning,
) -> serde_json::Value {
    let keyspaces: serde_json::Map<_, _> = usage
        .into_iter()
        .map(|(name, bytes, path)| {
            let keyspace = tuning
                .keyspaces
                .iter()
                .find(|(tuned, _)| *tuned == name)
                .map(|(_, keyspace)| {
                    json!({
                        "memtable_mb": keyspace.memtable_mb,
                        "compaction": {
                            "strategy": "leveled",
                            "l0_threshold": keyspace.l0_threshold,
                            "table_mb": keyspace.table_mb,
                            "level_ratio": keyspace.level_ratio,
                        },
                    })
                });

            (
                name.to_string(),
                json!({
                    "disk_bytes": bytes,
                    "path": path.display().to_string(),
                    "tuning": keyspace,
                }),
            )
        })
        .collect();

    json!({
        "engine": "fjall",
        "tuning": {
            "preset": tuning.preset.name(),
            "cache_mb": tuning.cache_mb,
            "journal_mb": tuning.journal_mb,
            "workers": tuning.workers,
            "flush_on_commit": tuning.flush_on_commit,
        },
        "keyspaces": serde_json::Value::Object(keyspaces),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_fjall::archive::ArchiveStore;
    use dolos_fjall::{Preset, StateStore};

    fn assert_reports(section: &serde_json::Value, tuning: &dolos_fjall::preset::Tuning) {
        let cores = std::thread::available_parallelism().map_or(1, usize::from);

        assert_eq!(
            section["tuning"],
            json!({
                "preset": "standard",
                "cache_mb": 500,
                "journal_mb": 512,
                "workers": cores.min(4),
                "flush_on_commit": false,
            })
        );

        for (name, row) in &tuning.keyspaces {
            assert_eq!(
                section["keyspaces"][name]["tuning"],
                json!({
                    "memtable_mb": row.memtable_mb,
                    "compaction": {
                        "strategy": "leveled",
                        "l0_threshold": row.l0_threshold,
                        "table_mb": row.table_mb,
                        "level_ratio": row.level_ratio,
                    },
                }),
                "{name}"
            );
        }
    }

    #[test]
    fn fjall_stores_report_their_tuning() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open(dir.path(), Preset::Standard).unwrap();
        let tuning = state.tuning();
        assert_reports(&fjall_section(state.disk_usage(), &tuning), &tuning);

        let archive = ArchiveStore::for_tempdir(Default::default()).unwrap();
        let tuning = archive.tuning();
        assert_reports(&fjall_section(archive.disk_usage(), &tuning), &tuning);
    }
}
