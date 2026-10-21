//! The tuning a Fjall store opens with.
//!
//! Tuning is chosen by name, never field by field: a preset is one table per
//! store that sets its cache, journal size, workers, flush-on-commit, and each
//! keyspace's memtable size and leveled compaction. Those values reach Fjall
//! explicitly, Fjall's own defaults among them, so upgrading Fjall cannot move
//! them. Block size, compression, filters and journal compression are not
//! set, and follow Fjall's defaults.
//!
//! Fjall records a keyspace's memtable size and compaction strategy when it
//! creates the keyspace and keeps them for its lifetime, so a preset's keyspace
//! rows govern the stores it creates. Its database row applies at every open.

use std::fmt::{self, Display};
use std::path::Path;
use std::sync::Arc;

use dolos_core::config::FjallPreset;
use fjall::{compaction::Leveled, Database, DatabaseBuilder, KeyspaceCreateOptions};

const MIB: u64 = 1024 * 1024;

/// A named tuning for both stores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Preset {
    /// What a default-configured store got before presets existed.
    #[default]
    Standard,
}

impl Preset {
    pub fn name(self) -> &'static str {
        match self {
            Self::Standard => "standard",
        }
    }

    pub fn state(self) -> &'static StatePreset {
        match self {
            Self::Standard => &StatePreset::STANDARD,
        }
    }

    pub fn archive(self) -> &'static ArchivePreset {
        match self {
            Self::Standard => &ArchivePreset::STANDARD,
        }
    }
}

impl From<FjallPreset> for Preset {
    fn from(preset: FjallPreset) -> Self {
        match preset {
            FjallPreset::Standard => Self::Standard,
        }
    }
}

/// Settings of one Fjall database, applied at every open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseTuning {
    /// Block cache, in MiB.
    pub cache_mb: u64,
    /// Journal size across all keyspaces before memtables are flushed, in MiB.
    pub journal_mb: u64,
    /// Background flush and compaction workers: one per core, at most this
    /// many.
    pub max_workers: usize,
    /// Hand the journal to the OS after every commit instead of when its
    /// buffer fills.
    pub flush_on_commit: bool,
}

impl DatabaseTuning {
    /// The worker count on this machine.
    pub fn workers(&self) -> usize {
        std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(self.max_workers)
    }

    pub(crate) fn builder(&self, path: &Path) -> DatabaseBuilder<Database> {
        Database::builder(path)
            .cache_size(self.cache_mb * MIB)
            .max_journaling_size(self.journal_mb * MIB)
            .worker_threads(self.workers())
    }
}

/// Settings of one keyspace, fixed when Fjall creates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyspaceTuning {
    /// Memtable size before it is flushed to a table, in MiB.
    pub memtable_mb: u64,
    /// Leveled compaction: L0 tables that trigger a compaction into L1.
    pub l0_threshold: u8,
    /// Leveled compaction: target size of a table, in MiB.
    pub table_mb: u64,
    /// Leveled compaction: size ratio between one level and the next.
    pub level_ratio: u8,
}

impl KeyspaceTuning {
    /// Fjall 3.1's own keyspace defaults, which the primary keyspaces always
    /// opened with.
    const FJALL_DEFAULT: Self = Self {
        memtable_mb: 64,
        l0_threshold: 4,
        table_mb: 64,
        level_ratio: 10,
    };

    /// The index keyspaces: what the standalone index store applied to them,
    /// kept when they moved into the state and archive stores.
    const INDEX: Self = Self {
        memtable_mb: 128,
        l0_threshold: 8,
        ..Self::FJALL_DEFAULT
    };

    pub(crate) fn options(&self) -> KeyspaceCreateOptions {
        let compaction = Leveled::default()
            .with_l0_threshold(self.l0_threshold)
            .with_table_target_size(self.table_mb * MIB)
            .with_level_ratio_policy(vec![f32::from(self.level_ratio)]);

        KeyspaceCreateOptions::default()
            .max_memtable_size(self.memtable_mb * MIB)
            .compaction_strategy(Arc::new(compaction))
    }
}

impl Display for KeyspaceTuning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "memtable {} MiB, leveled l0 {} table {} MiB ratio {}",
            self.memtable_mb, self.l0_threshold, self.table_mb, self.level_ratio
        )
    }
}

/// The state store's tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatePreset {
    pub database: DatabaseTuning,
    pub cursor: KeyspaceTuning,
    pub utxos: KeyspaceTuning,
    pub entities: KeyspaceTuning,
    pub tags: KeyspaceTuning,
}

impl StatePreset {
    pub const STANDARD: Self = Self {
        database: DatabaseTuning::STANDARD,
        cursor: KeyspaceTuning::FJALL_DEFAULT,
        utxos: KeyspaceTuning::FJALL_DEFAULT,
        entities: KeyspaceTuning::FJALL_DEFAULT,
        tags: KeyspaceTuning::INDEX,
    };
}

/// The archive store's tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePreset {
    pub database: DatabaseTuning,
    pub blocks: KeyspaceTuning,
    pub logs: KeyspaceTuning,
    pub tags: KeyspaceTuning,
    pub exact: KeyspaceTuning,
}

impl ArchivePreset {
    pub const STANDARD: Self = Self {
        database: DatabaseTuning::STANDARD,
        blocks: KeyspaceTuning::FJALL_DEFAULT,
        logs: KeyspaceTuning::FJALL_DEFAULT,
        tags: KeyspaceTuning::INDEX,
        exact: KeyspaceTuning::INDEX,
    };
}

impl DatabaseTuning {
    /// The store's own 500 MiB cache over Fjall 3.1's other defaults.
    const STANDARD: Self = Self {
        cache_mb: 500,
        journal_mb: 512,
        max_workers: 4,
        flush_on_commit: false,
    };
}

/// The tuning a store opened with, its workers counted for this machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    pub preset: Preset,
    pub cache_mb: u64,
    pub journal_mb: u64,
    pub workers: usize,
    pub flush_on_commit: bool,
    /// Each keyspace by name, in the store's order.
    pub keyspaces: Vec<(&'static str, KeyspaceTuning)>,
}

impl Tuning {
    pub(crate) fn new(
        preset: Preset,
        database: &DatabaseTuning,
        keyspaces: Vec<(&'static str, KeyspaceTuning)>,
    ) -> Self {
        Self {
            preset,
            cache_mb: database.cache_mb,
            journal_mb: database.journal_mb,
            workers: database.workers(),
            flush_on_commit: database.flush_on_commit,
            keyspaces,
        }
    }
}

impl Display for Tuning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "preset {}: cache {} MiB, journal {} MiB, {} workers, flush on commit {}",
            self.preset.name(),
            self.cache_mb,
            self.journal_mb,
            self.workers,
            self.flush_on_commit
        )?;

        for (name, keyspace) in &self.keyspaces {
            write!(f, "; {name}: {keyspace}")?;
        }

        Ok(())
    }
}

/// Asserts that Fjall holds `tuning` for `db`: its cache as opened, and the
/// store's keyspaces and each one's leveled compaction as Fjall persisted
/// them. Fjall keeps the memtable size, journal size and worker count
/// private, so those are not read back; [`assert_memtables_hold`] observes
/// the memtable size instead.
#[cfg(test)]
pub(crate) fn assert_fjall_holds(db: &Database, tuning: &Tuning) {
    assert_eq!(db.cache_capacity(), tuning.cache_mb * MIB, "cache");

    let mut reported: Vec<&str> = tuning.keyspaces.iter().map(|(name, _)| *name).collect();
    reported.sort_unstable();
    let mut held: Vec<String> = db
        .list_keyspace_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    held.sort_unstable();
    assert_eq!(held, reported, "keyspaces");

    for (name, row) in &tuning.keyspaces {
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
        let strategy = &keyspace.config.compaction_strategy;

        let held: Vec<(String, Vec<u8>)> = strategy
            .get_config()
            .into_iter()
            .map(|(key, value)| (String::from_utf8(key.to_vec()).unwrap(), value.to_vec()))
            .collect();

        let ratio = [&[1u8][..], &f32::from(row.level_ratio).to_le_bytes()].concat();
        let expected = vec![
            ("leveled_l0_threshold".to_string(), vec![row.l0_threshold]),
            (
                "leveled_target_size".to_string(),
                (row.table_mb * MIB).to_le_bytes().to_vec(),
            ),
            ("leveled_level_ratio_policy".to_string(), ratio),
        ];

        assert_eq!(strategy.get_name(), "LeveledCompaction", "{name}");
        assert_eq!(held, expected, "{name}");
    }
}

/// Asserts that each keyspace rotates its memtable at the size `tuning`
/// sets: it holds just under that size without rotating, and rotates once
/// written past it. An insert past the limit only requests the rotation and a
/// worker runs it, so the rotation is polled for; a limit below the row is
/// caught when a worker runs its rotation within the pause under the row.
#[cfg(test)]
pub(crate) fn assert_memtables_hold(db: &Database, tuning: &Tuning) {
    use fjall::AbstractTree;
    use std::time::{Duration, Instant};

    let value = vec![0u8; MIB as usize];

    for (name, row) in &tuning.keyspaces {
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
        let tree = &keyspace.tree;
        let rotated = || tree.sealed_memtable_count() + tree.table_count() > 0;
        assert!(!rotated(), "{name}: keyspace not empty");

        let mut key = 0u64;
        let mut insert = |count: u64| {
            for _ in 0..count {
                keyspace.insert(key.to_be_bytes(), &value).unwrap();
                key += 1;
            }
        };

        insert(row.memtable_mb - 1);
        std::thread::sleep(Duration::from_millis(250));
        assert!(!rotated(), "{name}: rotated under {} MiB", row.memtable_mb);
        assert!(
            tree.active_memtable().size() > (row.memtable_mb - 1) * MIB,
            "{name}"
        );

        insert(2);
        let deadline = Instant::now() + Duration::from_secs(30);
        while !rotated() {
            assert!(
                Instant::now() < deadline,
                "{name}: did not rotate past {} MiB",
                row.memtable_mb
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values the pre-preset code applied to a default-configured store:
    /// its own cache and index constants, and Fjall 3.1.0's defaults where it
    /// set nothing (`Config::new`, `CreateOptions::default`,
    /// `Leveled::default`). Written out rather than built from the preset's
    /// own constants, so an edit to a `STANDARD` row fails here.
    const PRIMARY: KeyspaceTuning = KeyspaceTuning {
        memtable_mb: 64,
        l0_threshold: 4,
        table_mb: 64,
        level_ratio: 10,
    };

    const INDEX: KeyspaceTuning = KeyspaceTuning {
        memtable_mb: 128,
        l0_threshold: 8,
        table_mb: 64,
        level_ratio: 10,
    };

    const DATABASE: DatabaseTuning = DatabaseTuning {
        cache_mb: 500,
        journal_mb: 512,
        max_workers: 4,
        flush_on_commit: false,
    };

    #[test]
    fn standard_state_is_the_pre_preset_default() {
        let StatePreset {
            database,
            cursor,
            utxos,
            entities,
            tags,
        } = StatePreset::STANDARD;

        assert_eq!(database, DATABASE);
        assert_eq!(cursor, PRIMARY);
        assert_eq!(utxos, PRIMARY);
        assert_eq!(entities, PRIMARY);
        assert_eq!(tags, INDEX);
    }

    #[test]
    fn standard_archive_is_the_pre_preset_default() {
        let ArchivePreset {
            database,
            blocks,
            logs,
            tags,
            exact,
        } = ArchivePreset::STANDARD;

        assert_eq!(database, DATABASE);
        assert_eq!(blocks, PRIMARY);
        assert_eq!(logs, PRIMARY);
        assert_eq!(tags, INDEX);
        assert_eq!(exact, INDEX);
    }

    #[test]
    fn workers_are_the_cores_up_to_four() {
        let cores = std::thread::available_parallelism().map_or(1, usize::from);

        assert_eq!(DATABASE.workers(), cores.min(4));
    }

    #[test]
    fn the_configured_preset_selects_the_tables() {
        let preset = Preset::from(FjallPreset::Standard);

        assert_eq!(preset, Preset::Standard);
        assert_eq!(preset.state(), &StatePreset::STANDARD);
        assert_eq!(preset.archive(), &ArchivePreset::STANDARD);
    }
}
