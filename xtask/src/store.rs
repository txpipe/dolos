//! Development tooling over a stopped node's store directories.
//!
//! `store relabel` overwrites the compatibility identifier and internal
//! format a state or archive store recorded, so a development or soak
//! instance survives a prerelease identifier bump without a re-restore. It is
//! a development tool, never an operator migration, and it never ships in the
//! `dolos` binary.

use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::{Preset, StateStore};

/// The keys `record_identity` writes, in the kind's metadata keyspace.
const IDENTITY_KEY: &[u8] = b"identity";
const FORMAT_KEY: &[u8] = b"format";

#[derive(clap::Subcommand)]
pub enum Cmd {
    /// Overwrite the identity and format a stopped store recorded
    Relabel(RelabelArgs),
}

#[derive(clap::Args)]
pub struct RelabelArgs {
    /// A state directory (`<storage.path>/state`) or an archive directory
    /// (`<storage.path>/archive`)
    pub path: PathBuf,

    /// The compatibility identifier to record, taken as given
    #[arg(long, value_name = "ID")]
    pub contract: String,

    /// The internal format to record, 1 or more
    #[arg(long, value_name = "N")]
    pub format: u32,

    /// Write the new values; without it nothing is written
    #[arg(long)]
    pub yes: bool,
}

pub fn run(command: Cmd) -> Result<()> {
    match command {
        Cmd::Relabel(args) => relabel(&args).map(|_| ()),
    }
}

/// The two kinds of store that record an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    State,
    Archive,
}

impl StoreKind {
    /// The fjall database of a store at `path`.
    fn database(self, path: &Path) -> PathBuf {
        match self {
            Self::State => path.to_path_buf(),
            Self::Archive => path.join("index"),
        }
    }

    /// A keyspace every store of this kind holds.
    fn marker(self) -> &'static str {
        match self {
            Self::State => "state-cursor",
            Self::Archive => "archive-blocks",
        }
    }

    /// The keyspace holding the identity and format.
    fn meta(self) -> &'static str {
        match self {
            Self::State => "state-cursor",
            Self::Archive => "archive-meta",
        }
    }
}

impl fmt::Display for StoreKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::State => "state",
            Self::Archive => "archive",
        })
    }
}

/// The identity and format a store records; `None` where it records none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Label {
    pub identity: Option<String>,
    pub format: Option<u32>,
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.identity {
            Some(identity) => write!(f, "identity {identity:?}")?,
            None => f.write_str("no identity")?,
        }

        match self.format {
            Some(format) => write!(f, ", format {format}"),
            None => f.write_str(", no format"),
        }
    }
}

/// What a relabel found and, with `--yes`, what it read back after writing.
#[derive(Debug)]
pub struct Outcome {
    pub kind: StoreKind,
    pub before: Label,
    pub after: Option<Label>,
}

/// Print the store's current and new labels and, with `--yes`, write the new
/// one through `record_identity` and print what a reopen reads back.
pub fn relabel(args: &RelabelArgs) -> Result<Outcome> {
    if args.format < 1 {
        bail!("--format must be 1 or more");
    }

    let path = &args.path;
    let (kind, before) = inspect(path)?;
    let wanted = Label {
        identity: Some(args.contract.clone()),
        format: Some(args.format),
    };

    println!("{kind} store at {}", path.display());
    println!("  current: {before}");
    println!("  new:     {wanted}");

    if !args.yes {
        println!("dry run: nothing written; pass --yes to write");

        return Ok(Outcome {
            kind,
            before,
            after: None,
        });
    }

    write(kind, path, &args.contract, args.format)?;
    let after = read_back(kind, path)?;
    println!("  read back: {after}");

    if after != wanted {
        bail!(
            "{kind} store at {} read back {after}, not {wanted}",
            path.display()
        );
    }

    Ok(Outcome {
        kind,
        before,
        after: Some(after),
    })
}

/// The kind of store at `path`, from its keyspaces, and the label it records.
///
/// Opens the fjall database alone, exclusively and without background
/// workers, and creates no keyspace: a dry run stops here.
pub fn inspect(path: &Path) -> Result<(StoreKind, Label)> {
    let kind = [StoreKind::State, StoreKind::Archive]
        .into_iter()
        .find(|kind| dolos_fjall::database_exists(&kind.database(path)))
        .ok_or_else(|| {
            anyhow!(
                "{} is neither a state nor an archive store directory",
                path.display()
            )
        })?;

    let database = kind.database(path);
    let db = fjall::Database::builder(&database)
        .worker_threads_unchecked(0)
        .open()
        .map_err(|error| refusal(&database, error))?;

    if !db.keyspace_exists(kind.marker()) {
        bail!(
            "{} holds a fjall database without the {kind} store's keyspaces",
            path.display()
        );
    }

    if !db.keyspace_exists(kind.meta()) {
        return Ok((kind, Label::default()));
    }

    let meta = db.keyspace(kind.meta(), fjall::KeyspaceCreateOptions::default)?;
    let identity = meta
        .get(IDENTITY_KEY)?
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
    let format = meta
        .get(FORMAT_KEY)?
        .map(|bytes| {
            <[u8; 4]>::try_from(bytes.as_ref())
                .map(u32::from_be_bytes)
                .map_err(|_| anyhow!("a store format of {} bytes, not 4", bytes.len()))
        })
        .transpose()?;

    Ok((kind, Label { identity, format }))
}

fn write(kind: StoreKind, path: &Path, identity: &str, format: u32) -> Result<()> {
    match kind {
        StoreKind::State => {
            let store = open_state(path)?;
            store.record_identity(identity, format)?;
            store.shutdown()?;
        }
        StoreKind::Archive => {
            let store = open_archive(path)?;
            store.record_identity(identity, format)?;
            store.shutdown()?;
        }
    }

    Ok(())
}

fn read_back(kind: StoreKind, path: &Path) -> Result<Label> {
    let label = match kind {
        StoreKind::State => {
            let store = open_state(path)?;
            let label = Label {
                identity: store.identity()?,
                format: store.format()?,
            };
            store.shutdown()?;
            label
        }
        StoreKind::Archive => {
            let store = open_archive(path)?;
            let label = Label {
                identity: store.identity()?,
                format: store.format()?,
            };
            store.shutdown()?;
            label
        }
    };

    Ok(label)
}

fn open_state(path: &Path) -> Result<StateStore> {
    StateStore::open(path, Preset::Standard).map_err(|error| typed_refusal(path, error))
}

/// The archive's index, with its segment files where the store keeps them by
/// default; a relabel reads and writes no segment.
fn open_archive(path: &Path) -> Result<ArchiveStore> {
    let schema = dolos_cardano::model::build_log_schema();

    ArchiveStore::open(schema, path, None, Preset::Standard)
        .map_err(|error| typed_refusal(path, error))
}

fn refusal(path: &Path, error: fjall::Error) -> anyhow::Error {
    match error {
        fjall::Error::Locked => held(path),
        error => anyhow::Error::new(error).context(format!("opening {}", path.display())),
    }
}

fn typed_refusal(path: &Path, error: dolos_fjall::Error) -> anyhow::Error {
    if error.is_locked() {
        held(path)
    } else {
        anyhow::Error::new(error).context(format!("opening {}", path.display()))
    }
}

fn held(path: &Path) -> anyhow::Error {
    anyhow!(
        "{} is held by another process; stop it first",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dolos::storage::{ArchiveStoreBackend, StateStoreBackend};
    use dolos_core::compat::{STORE_FORMAT, SUPPORTED_CONTRACTS};

    use super::*;

    const OLD_CONTRACT: &str = "v2-alpha.0";

    type Dump = BTreeMap<(String, Vec<u8>), Vec<u8>>;

    fn args(path: &Path, contract: &str, format: u32, yes: bool) -> RelabelArgs {
        RelabelArgs {
            path: path.to_path_buf(),
            contract: contract.to_string(),
            format,
            yes,
        }
    }

    /// A row in every keyspace of `db`, so a dump shows any change to them.
    fn fill(db: &fjall::Database) {
        for name in db.list_keyspace_names() {
            let keyspace = db
                .keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .unwrap();
            keyspace
                .insert(format!("{}-key", &*name), format!("{}-value", &*name))
                .unwrap();
        }
    }

    /// A state store holding data under an identity this release refuses.
    fn state_store(dir: &Path) -> PathBuf {
        let path = dir.join("state");
        let store = StateStore::open(&path, Preset::Standard).unwrap();
        fill(store.database());
        store.record_identity(OLD_CONTRACT, 1).unwrap();
        store.shutdown().unwrap();

        path
    }

    /// An archive store holding data under an identity this release refuses.
    fn archive_store(dir: &Path) -> PathBuf {
        let path = dir.join("archive");
        let store = open_archive(&path).unwrap();
        fill(store.database());
        store.record_identity(OLD_CONTRACT, 1).unwrap();
        store.shutdown().unwrap();

        path
    }

    fn stores(dir: &Path) -> [(StoreKind, PathBuf); 2] {
        [
            (StoreKind::State, state_store(dir)),
            (StoreKind::Archive, archive_store(dir)),
        ]
    }

    /// Every key and value of every keyspace in the store's database.
    fn dump(kind: StoreKind, path: &Path) -> Dump {
        let db = fjall::Database::builder(kind.database(path))
            .worker_threads_unchecked(0)
            .open()
            .unwrap();
        let mut dump = Dump::new();

        for name in db.list_keyspace_names() {
            let keyspace = db
                .keyspace(&name, fjall::KeyspaceCreateOptions::default)
                .unwrap();

            for entry in keyspace.iter() {
                let (key, value) = entry.into_inner().unwrap();
                dump.insert((name.to_string(), key.to_vec()), value.to_vec());
            }
        }

        dump
    }

    fn label(contract: &str, format: u32) -> Label {
        Label {
            identity: Some(contract.to_string()),
            format: Some(format),
        }
    }

    #[test]
    fn a_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();

        for (kind, path) in stores(dir.path()) {
            let before = dump(kind, &path);
            let outcome = relabel(&args(&path, "v2-beta.9", 3, false)).unwrap();

            assert_eq!(outcome.kind, kind);
            assert_eq!(outcome.before, label(OLD_CONTRACT, 1));
            assert_eq!(outcome.after, None);
            assert_eq!(dump(kind, &path), before, "{kind}");
        }
    }

    #[test]
    fn yes_rewrites_both_keys_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();

        for (kind, path) in stores(dir.path()) {
            let mut expected = dump(kind, &path);
            let meta = kind.meta().to_string();
            expected.insert((meta.clone(), IDENTITY_KEY.to_vec()), b"dev-7".to_vec());
            expected.insert((meta, FORMAT_KEY.to_vec()), 42u32.to_be_bytes().to_vec());

            let outcome = relabel(&args(&path, "dev-7", 42, true)).unwrap();

            assert_eq!(outcome.kind, kind);
            assert_eq!(outcome.before, label(OLD_CONTRACT, 1));
            assert_eq!(outcome.after, Some(label("dev-7", 42)));
            assert_eq!(dump(kind, &path), expected, "{kind}");
            assert_eq!(inspect(&path).unwrap(), (kind, label("dev-7", 42)));
        }
    }

    #[test]
    fn a_held_store_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        for (kind, path) in stores(dir.path()) {
            let holder: Box<dyn FnOnce()> = match kind {
                StoreKind::State => {
                    let store = open_state(&path).unwrap();
                    Box::new(move || store.shutdown().unwrap())
                }
                StoreKind::Archive => {
                    let store = open_archive(&path).unwrap();
                    Box::new(move || store.shutdown().unwrap())
                }
            };

            let error = relabel(&args(&path, "dev-7", 2, true)).unwrap_err();
            assert!(
                error.to_string().contains("held by another process"),
                "{error:#}"
            );

            holder();
            assert_eq!(inspect(&path).unwrap(), (kind, label(OLD_CONTRACT, 1)));
        }
    }

    #[test]
    fn a_plain_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), "not a store").unwrap();

        let error = relabel(&args(dir.path(), "dev-7", 2, true)).unwrap_err();

        assert!(
            error.to_string().contains("neither a state nor an archive"),
            "{error:#}"
        );
        let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1, "the refusal created nothing");
    }

    #[test]
    fn a_foreign_fjall_database_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let archive = archive_store(dir.path());

        // the archive's index is a fjall database, but not a state store
        let error = relabel(&args(&archive.join("index"), "dev-7", 2, true)).unwrap_err();

        assert!(
            error.to_string().contains("without the state store's"),
            "{error:#}"
        );
    }

    #[test]
    fn a_zero_format_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = state_store(dir.path());

        let error = relabel(&args(&path, "dev-7", 0, true)).unwrap_err();

        assert!(error.to_string().contains("--format"), "{error:#}");
        assert_eq!(inspect(&path).unwrap().1, label(OLD_CONTRACT, 1));
    }

    #[test]
    fn a_relabeled_store_is_admitted() {
        let dir = tempfile::tempdir().unwrap();
        let schema = dolos_cardano::model::build_log_schema;
        let (state, archive) = (state_store(dir.path()), archive_store(dir.path()));

        assert!(StateStoreBackend::open_fjall(&state, Preset::Standard).is_err());
        assert!(
            ArchiveStoreBackend::open_fjall(&archive, schema(), None, Preset::Standard).is_err()
        );

        for path in [&state, &archive] {
            relabel(&args(path, SUPPORTED_CONTRACTS[0], STORE_FORMAT, true)).unwrap();
        }

        let state = StateStoreBackend::open_fjall(&state, Preset::Standard).unwrap();
        state.shutdown().unwrap();

        let archive =
            ArchiveStoreBackend::open_fjall(&archive, schema(), None, Preset::Standard).unwrap();
        drop(archive);
    }
}
