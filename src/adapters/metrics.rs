//! The `dolos` meter's storage gauges: what fjall does not report, read from
//! the stores and the file system when the meter collects them.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use dolos_core::{telemetry, WalStore as _};
use dolos_fjall::flatfiles::parse_segment_filename;
use opentelemetry::{global, metrics::ObservableGauge, KeyValue};

use super::{ArchiveStoreBackend, WalAdapter};

/// The meter of this crate's storage gauges.
pub const METER: &str = "dolos";

/// The gauge of the bytes a store takes on disk.
pub const DISK_USAGE: &str = "dolos.storage.disk.usage";

/// The gauge of the oldest slot the WAL holds.
pub const WAL_START_SLOT: &str = "dolos.storage.wal.start.slot";

/// The gauge of the newest slot the WAL holds.
pub const WAL_TIP_SLOT: &str = "dolos.storage.wal.tip.slot";

/// The gauge of the oldest block slot the archive holds.
pub const ARCHIVE_START_SLOT: &str = "dolos.storage.archive.start.slot";

/// The gauge of the newest block slot the archive holds.
pub const ARCHIVE_TIP_SLOT: &str = "dolos.storage.archive.tip.slot";

/// What the gauges read.
struct Source {
    wal: WalAdapter,
    wal_path: Option<PathBuf>,
    archive: ArchiveStoreBackend,
}

type Read = fn(&Source) -> Option<u64>;

/// The block and WAL disk usage and the four range gauges.
///
/// The gauges hold what they read weakly, so they report nothing once this
/// value is dropped: keep it as long as the stores are open. An empty store
/// reports no range.
pub struct StorageGauges {
    _source: Arc<Source>,
    _disk_usage: ObservableGauge<u64>,
    _ranges: Vec<ObservableGauge<u64>>,
}

impl StorageGauges {
    /// Registers the gauges over `wal`, whose file is at `wal_path` when it
    /// has one, and `archive`.
    pub fn register(
        wal: &WalAdapter,
        wal_path: Option<PathBuf>,
        archive: &ArchiveStoreBackend,
    ) -> Self {
        let source = Arc::new(Source {
            wal: wal.clone(),
            wal_path,
            archive: archive.clone(),
        });
        let meter = global::meter(METER);

        let disk_usage = {
            let weak = Arc::downgrade(&source);

            meter
                .u64_observable_gauge(DISK_USAGE)
                .with_unit("By")
                .with_description("The bytes the store's files take on disk")
                .with_callback(move |observer| {
                    let Some(source) = weak.upgrade() else {
                        return;
                    };

                    let blocks = source.archive.segments_dir();
                    let usage = [
                        ("blocks", blocks.as_deref().and_then(segments_size)),
                        ("wal", source.wal_path.as_deref().and_then(file_size)),
                    ];

                    for (store, bytes) in usage {
                        if let Some(bytes) = bytes {
                            observer.observe(bytes, &[KeyValue::new(telemetry::STORE, store)]);
                        }
                    }
                })
                .build()
        };

        let range = |name: &'static str, read: Read| {
            let weak: Weak<Source> = Arc::downgrade(&source);

            meter
                .u64_observable_gauge(name)
                .with_unit("{slot}")
                .with_callback(move |observer| {
                    if let Some(slot) = weak.upgrade().and_then(|source| read(&source)) {
                        observer.observe(slot, &[]);
                    }
                })
                .build()
        };

        let ranges = vec![
            range(WAL_START_SLOT, |source| {
                let (point, _) = source.wal.find_start().ok()??;
                Some(point.slot())
            }),
            range(WAL_TIP_SLOT, |source| {
                let (point, _) = source.wal.find_tip().ok()??;
                Some(point.slot())
            }),
            range(ARCHIVE_START_SLOT, |source| {
                source.archive.oldest_block_slot().ok()?
            }),
            range(ARCHIVE_TIP_SLOT, |source| {
                source.archive.newest_block_slot().ok()?
            }),
        ];

        Self {
            _source: source,
            _disk_usage: disk_usage,
            _ranges: ranges,
        }
    }
}

/// The size of the file at `path`, when it is there.
fn file_size(path: &Path) -> Option<u64> {
    Some(std::fs::metadata(path).ok()?.len())
}

/// The sum of the sizes of the segment files in `dir`, when it is there.
fn segments_size(dir: &Path) -> Option<u64> {
    let entries = std::fs::read_dir(dir).ok()?;

    let size = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            name.to_str().and_then(parse_segment_filename).is_some()
        })
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum();

    Some(size)
}
