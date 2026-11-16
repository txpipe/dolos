//! `dolos bootstrap stelae` — rebuild a node from a Stelae snapshot.
//!
//! Everything below this module is `dolos_snapshot::restore`, which is generic
//! over the store traits. What only this module knows is the node: its network
//! magic, which comes from genesis and never from a file an operator can edit,
//! and its `history.window`, which bounds how much chain history a restore
//! bothers to read.
//!
//! ## A source names a network root
//!
//! The stele is in the root's `dolos` repository and the blocks of its closed
//! epochs in its `blocks` repository: `oci://HOST/PATH` reads `PATH/dolos` and
//! `PATH/blocks`, and `file://DIR` reads `DIR/dolos` and `DIR/blocks`, each an
//! OCI image layout. `file://DIR` and `oci://HOST/PATH` differ in where the
//! bytes come from and in nothing else: the same plan, the same refusals, the
//! same store writes, the same checkpoints. Only two things are the registry's
//! alone — `--point`, which names which stele in a repository to read, and
//! `--insecure`, which is for a registry on a loopback address.
//!
//! ## `--continue` resumes the stele a restore started from
//!
//! A restore records, beside its progress file, the source it reads and the
//! stele's sequence and digest. `--continue` reads that stele again, by its
//! epoch tag in a registry and never `latest`, and skips the layers the
//! progress file names, refusing when the stele is gone or the records can't
//! be trusted. With no records it restores as without the flag, over storage
//! that holds no data. Without the flag a restore refuses partial data until
//! `--force` clears it, and overwrites any progress file rather than obey it:
//! `dolos_snapshot::restore::Checkpoint` states why.

use std::path::{Path, PathBuf};

use dolos_cardano::networks::KnownNetwork;
use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};

use dolos_snapshot::{
    facade::{self as snapshot, RestoreInput, SnapshotRepository},
    node,
    registry::{self, Auth, Point, Repository},
    restore::{
        blocks_dir, blocks_wanted, profile_repository, ResumeRefusal, Source, SourceRecord,
        BLOCKS_REPOSITORY, DOLOS_REPOSITORY,
    },
};

use crate::{
    feedback::{Feedback, RestoreProgress},
    output::Usage,
    registry::CredentialArgs,
};

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// Where to restore from, as a URL naming a network root: `file://DIR`, a
    /// directory holding `dolos/` and `blocks/`, each an OCI image layout, or
    /// `oci://HOST/PATH`, whose `PATH/dolos` and `PATH/blocks` repositories
    /// are read. Defaults to the configured network's official root, and is
    /// required for a custom network
    #[arg(long)]
    pub source: Option<Source>,

    /// which stele to restore: `latest`, or `epoch-N` for the stele cut at the
    /// boundary that opens epoch N. Registry sources only.
    #[arg(long, value_name = "POINT", default_value = "latest")]
    pub point: Point,

    /// talk to the repository over plaintext HTTP rather than HTTPS; for a
    /// registry on a loopback address or a mirror inside a cluster, and for
    /// nothing reachable from outside one
    #[arg(long, action)]
    pub insecure: bool,

    /// directory to stage pulled layers in; defaults to
    /// `<storage.path>/scratch`. registry sources only — a `file://` restore
    /// stages nothing
    #[arg(long, value_name = "DIR")]
    pub scratch_dir: Option<PathBuf>,

    /// skip the restore disk-space preflight and continue even when the
    /// estimate of what the restore will occupy exceeds the available space
    #[arg(long, action)]
    pub skip_space_check: bool,

    /// how many layers to read ahead of the one being written; a registry
    /// restore stages each in the scratch directory until its turn. 0 or 1
    /// reads one layer at a time
    #[arg(long, value_name = "LAYERS", default_value_t = dolos_snapshot::restore::DEFAULT_READ_AHEAD)]
    pub read_ahead: usize,

    /// Resume an interrupted restore from the layers it recorded, reading the
    /// stele it started from again; with nothing to resume, restore as
    /// without the flag into storage that holds no data
    #[arg(long = "continue")]
    pub resume: bool,

    #[command(flatten)]
    pub credentials: CredentialArgs,
}

/// What the prompt offers as the stele's source.
#[cfg(feature = "utils")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceChoice {
    /// The configured network's official repository.
    Official(&'static str),
    /// A URL the operator types.
    Custom,
}

#[cfg(feature = "utils")]
impl std::fmt::Display for SourceChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceChoice::Official(url) => write!(f, "the official repository ({url})"),
            SourceChoice::Custom => {
                f.write_str("another source (a network root in a registry or a file:// directory)")
            }
        }
    }
}

/// The placeholder the custom source's question shows.
#[cfg(feature = "utils")]
fn custom_placeholder() -> &'static str {
    dolos_cardano::networks::MAINNET.stelae.registry
}

/// The prompt's choices, default first: a known network's official
/// repository, then a custom source. A custom network has only the latter.
#[cfg(feature = "utils")]
fn source_choices(network: Option<&KnownNetwork>) -> Vec<SourceChoice> {
    network
        .map(|network| SourceChoice::Official(network.stelae.registry))
        .into_iter()
        .chain([SourceChoice::Custom])
        .collect()
}

impl Args {
    #[cfg(feature = "utils")]
    pub fn inquire(network: Option<&KnownNetwork>) -> miette::Result<Self> {
        let choice = match source_choices(network).as_slice() {
            [only] => *only,
            choices => inquire::Select::new("where is the stele?", choices.to_vec())
                .prompt()
                .into_diagnostic()?,
        };

        let source = match choice {
            SourceChoice::Official(url) => url.to_string(),
            SourceChoice::Custom => inquire::Text::new("where is the stele?")
                .with_help_message(
                    "oci://HOST/PATH for a network root in a registry, or file://DIR for a directory holding dolos/ and blocks/",
                )
                .with_placeholder(custom_placeholder())
                .prompt()
                .into_diagnostic()?,
        };

        Ok(Self {
            source: Some(source.parse().map_err(|e: String| miette::miette!("{e}"))?),
            point: Point::default(),
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            read_ahead: dolos_snapshot::restore::DEFAULT_READ_AHEAD,
            resume: false,
            credentials: super::from_environment()?,
        })
    }
}

/// The stele to restore: what the invocation names, else the known network's
/// official repository.
pub(super) fn source(args: &Args, network: Option<&KnownNetwork>) -> miette::Result<Source> {
    if let Some(source) = &args.source {
        return Ok(source.clone());
    }

    let network = network.ok_or_else(|| {
        miette::miette!("a custom network has no default stele repository; pass --source")
    })?;

    network
        .stelae
        .registry
        .parse()
        .map_err(|e: String| miette::miette!("{e}"))
}

/// Where a restore reads the stele and its blocks from, with the credentials
/// both repositories are read with.
enum Reader {
    Dir {
        dolos: PathBuf,
        blocks: PathBuf,
    },
    Repo {
        dolos: Repository,
        blocks: Repository,
        auth: Auth,
    },
}

/// A restore's resolved inputs.
pub(super) struct Inputs {
    reader: Reader,
    /// The network root as read, a directory by its resolved path, for the
    /// record.
    source: Source,
    /// The stele to read in a repository.
    point: Point,
    resuming: Option<Resuming>,
}

/// What a `--continue` resumes.
struct Resuming {
    record: SourceRecord,
    /// The record's source, parsed.
    source: Source,
    /// The layers the progress file records.
    layers: usize,
}

/// Resolves the source and, for a repository, the credentials, so that
/// `--force`, which `clears` says is to clear storage, clears nothing for a
/// restore that would refuse them, and the restore reads with the credentials
/// that were checked. A `--continue` with records takes the recorded source
/// and stele.
pub(super) fn resolve(
    config: &RootConfig,
    args: &Args,
    network: Option<&KnownNetwork>,
    clears: bool,
) -> miette::Result<Inputs> {
    let resuming = match args.resume {
        true => unfinished(config, args)?,
        false => None,
    };

    let (source, point) = match &resuming {
        Some(resuming) => (
            resuming.source.clone(),
            Point::Epoch(resuming.record.sequence),
        ),
        None => (source(args, network)?, args.point),
    };

    let (reader, source) = match source {
        Source::Dir(root) => {
            let root = match &resuming {
                Some(resuming) => reopen_dir(&root, &resuming.record)?,
                None => check_dir(config, &root, clears)?,
            };

            let reader = Reader::Dir {
                dolos: root.join(DOLOS_REPOSITORY),
                blocks: root.join(BLOCKS_REPOSITORY),
            };

            (reader, Source::Dir(root))
        }
        Source::Repo(root) => {
            let auth = args.credentials.auth(&root, network)?;
            let repository = |name| {
                profile_repository(&root, name)
                    .into_diagnostic()
                    .with_context(|| format!("naming the {name} repository under {root}"))
            };

            let reader = Reader::Repo {
                dolos: repository(DOLOS_REPOSITORY)?,
                blocks: repository(BLOCKS_REPOSITORY)?,
                auth,
            };

            (reader, Source::Repo(root))
        }
    };

    Ok(Inputs {
        reader,
        source,
        point,
        resuming,
    })
}

/// What `--continue` resumes: the source record an interrupted restore left,
/// with the layers its progress file records, once the invocation agrees
/// with it. `None` over storage holding no data and no records, which
/// restores as without the flag.
fn unfinished(config: &RootConfig, args: &Args) -> miette::Result<Option<Resuming>> {
    let root = &config.storage.path;

    let unfinished = match dolos_snapshot::restore::unfinished(root) {
        Ok(unfinished) => unfinished,
        Err(dolos_snapshot::Error::Resume(refusal)) => return Err(refused(refusal)),
        Err(other) => {
            return Err(other)
                .into_diagnostic()
                .context("reading the records of an interrupted restore")
        }
    };

    match (unfinished.record, unfinished.progress) {
        (Some(record), progress) => {
            let source = record.source.parse().map_err(|reason: String| {
                refused(ResumeRefusal::Unreadable {
                    path: dolos_snapshot::restore::source_path_in(root),
                    reason,
                })
            })?;

            agrees(args, &record)?;

            let layers = progress.map_or(0, |progress| progress.completed.len());
            still_holds(config, layers)?;

            Ok(Some(Resuming {
                record,
                source,
                layers,
            }))
        }
        (None, Some(_)) => Err(refused(ResumeRefusal::Unrecorded {
            storage: root.clone(),
        })),
        (None, None) => {
            let held = dolos::storage::held_data(config).into_diagnostic()?;

            if held.is_empty() {
                return Ok(None);
            }

            let held: Vec<_> = held.iter().map(|path| path.display().to_string()).collect();

            miette::bail!(
                "nothing to resume: no stele restore left its records in {}, and storage holds \
                 data ({}). Use --force to replace it",
                root.display(),
                held.join(", "),
            )
        }
    }
}

/// Refuse a resume over stores something else wrote since the restore was
/// interrupted, as a WAL with entries shows, or, when the progress file
/// records `layers`, over an archive kept on disk that no longer holds them:
/// an index holding no data, whatever block segments survive it, or a
/// segment file missing or cut short before the newest block the index
/// locates. Recorded layers wrote index rows, save empty leading epochs, so a
/// stop after only those is refused too. Older segments lost beside the
/// newest aren't caught. The library refuses a state cursor other than the
/// stele's.
fn still_holds(config: &RootConfig, layers: usize) -> miette::Result<()> {
    let index = dolos::storage::archive_index(config).into_diagnostic()?;

    if dolos::storage::wal_holds_entries(config) {
        miette::bail!(
            "the WAL at {} has entries, which a stele restore never writes, so something else \
             wrote this node after the restore was interrupted. Use --force to start over",
            config.wal_path().unwrap_or_default().display(),
        );
    }

    let on_disk = config.keeps_archive() && config.storage.archive_path().is_some();

    if layers == 0 || !on_disk {
        return Ok(());
    }

    if !index.holds_data {
        miette::bail!(
            "the restore progress in {} records {layers} layers, and the archive index they were \
             written to holds no data. Use --force to start over",
            config.storage.path.display(),
        );
    }

    if let Some(segment) = index.missing_segment {
        miette::bail!(
            "the restore progress in {} records {layers} layers, and {}, where the archive index \
             locates their newest block, is missing or ends before that block. Use --force to \
             start over",
            config.storage.path.display(),
            segment.display(),
        );
    }

    Ok(())
}

/// Refuse, as a usage error naming the recorded value, a `--source` or an
/// `--point epoch-N` other than the record's. `--point latest`, the default,
/// always agrees: the record pins the stele, and the moving tag isn't read.
fn agrees(args: &Args, record: &SourceRecord) -> miette::Result<()> {
    if let Some(given) = &args.source {
        let given = match given {
            Source::Dir(dir) => Source::Dir(
                dolos::storage::resolve_parent_dirs(dir)
                    .into_diagnostic()
                    .context("resolving --source")?,
            ),
            repo => repo.clone(),
        };

        if given.to_string() != record.source {
            Err(Usage::new(
                format!(
                    "--source {given} is not the source of the restore --continue resumes, {}",
                    record.source
                ),
                Some(
                    "leave --source out to resume, or use --force to start over from this one"
                        .to_owned(),
                ),
            ))?;
        }
    }

    if let Point::Epoch(epoch) = args.point {
        if epoch != record.sequence {
            Err(Usage::new(
                format!(
                    "--point epoch-{epoch} is not the stele the restore --continue resumes, \
                     epoch-{}",
                    record.sequence
                ),
                Some(
                    "leave --point out to resume, or use --force to start over from this one"
                        .to_owned(),
                ),
            ))?;
        }
    }

    Ok(())
}

/// The recorded network root, refused as gone when its `dolos` directory
/// holds no stele.
fn reopen_dir(root: &Path, record: &SourceRecord) -> miette::Result<PathBuf> {
    match dolos_snapshot::dir::SnapshotDir::open(root.join(DOLOS_REPOSITORY)) {
        Ok(_) => Ok(root.to_path_buf()),
        Err(_) => Err(refused(ResumeRefusal::Gone(record.clone()))),
    }
}

/// A resume refused, with the flag that starts over.
fn refused(refusal: ResumeRefusal) -> miette::Report {
    miette::miette!("{refusal}. Use --force to start over")
}

/// The network root `root`, once it holds a `dolos` and a `blocks` image
/// layout, the first's latest stele resolves, chain and all, and the second
/// holds the blocks snapshot that stele reads, when it reads one; and, when
/// `clears`, once both lie out of the reach of the clear with their own
/// files, the ones the restore reads. An image layout alone is not a stele:
/// a refused publish leaves one with no tags. The restore reads the root by
/// the path checked, with any `..` in it resolved.
fn check_dir(config: &RootConfig, root: &Path, clears: bool) -> miette::Result<PathBuf> {
    use dolos_snapshot::dir::SnapshotDir;

    let root = dolos::storage::resolve_parent_dirs(root)
        .into_diagnostic()
        .context("resolving --source")?;

    let [dolos, blocks] = [DOLOS_REPOSITORY, BLOCKS_REPOSITORY].map(|name| root.join(name));

    for (dir, name) in [(&dolos, DOLOS_REPOSITORY), (&blocks, BLOCKS_REPOSITORY)] {
        if !dir.is_dir() {
            miette::bail!(
                help = format!(
                    "a file:// source names a network root holding `{DOLOS_REPOSITORY}/` and \
                     `{BLOCKS_REPOSITORY}/`, each an OCI image layout"
                ),
                "{} holds no `{name}` directory",
                root.display(),
            );
        }
    }

    let stele = SnapshotDir::open(&dolos)
        .and_then(|layout| layout.resolve_latest(&dolos_snapshot::DolosProfile))
        .map_err(dolos_snapshot::Error::from)
        .into_diagnostic()
        .with_context(|| format!("{} holds no stele", dolos.display()))?;

    let wanted = match stele.chain() {
        Some(chain) => blocks_wanted(chain, config.history.max_slots())
            .into_diagnostic()
            .with_context(|| format!("reading the stele in {}", dolos.display()))?
            .map(|reference| (reference, chain.head().sequence)),
        None => None,
    };

    match wanted {
        Some((reference, sequence)) => blocks_dir(&blocks, reference, sequence)
            .map(drop)
            .into_diagnostic(),
        None => SnapshotDir::open(&blocks)
            .map(drop)
            .map_err(dolos_snapshot::Error::from)
            .into_diagnostic()
            .with_context(|| format!("{} is not a blocks repository", blocks.display())),
    }?;

    if clears {
        use dolos_snapshot::dir::{BLOBS_DIR, INDEX_FILE, LAYOUT_FILE};

        let reads: Vec<_> = [&dolos, &blocks]
            .into_iter()
            .flat_map(|dir| [LAYOUT_FILE, INDEX_FILE, BLOBS_DIR].map(|name| dir.join(name)))
            .collect();

        super::refuse_what_the_clear_takes(
            config,
            &root,
            &reads,
            "the source",
            "the restore",
            "move the source out of the storage root",
        )?;
    }

    Ok(root)
}

/// What every restore needs before it can read a layer, whatever the source is.
struct Node {
    root: PathBuf,
    stores: crate::common::Stores,
    magic: u64,
    max_history: Option<u64>,
}

#[derive(Clone, Copy)]
struct RestoreOptions<'a> {
    feedback: &'a Feedback,
    skip_space_check: bool,
    read_ahead: usize,
}

impl Node {
    fn open(config: &RootConfig) -> miette::Result<Self> {
        let root = crate::common::ensure_storage_path(config)
            .into_diagnostic()
            .context("creating the storage directory")?;

        let genesis = crate::common::open_genesis(config)?;

        let stores = crate::common::open_data_stores(config)
            .into_diagnostic()
            .context("opening the data stores")?;

        Ok(Self {
            root,
            stores,
            magic: u64::from(genesis.network_magic()),
            max_history: config.history.max_slots(),
        })
    }

    /// What this node knows about itself, as the restore driver takes it.
    fn restoring<'a>(
        &'a self,
        inputs: &'a Inputs,
        options: RestoreOptions<'_>,
    ) -> dolos_snapshot::restore::Restoring<'a> {
        dolos_snapshot::restore::Restoring {
            network_magic: self.magic,
            max_history: self.max_history,
            storage_path: &self.root,
            resume: inputs.resuming.is_some(),
            skip_space_check: options.skip_space_check,
            journals: self.journals(),
            read_ahead: options.read_ahead,
            source: Some(&inputs.source),
        }
    }

    /// What the stores' journals may hold between them: each fjall store's
    /// `journal_mb`, and nothing for a backend that keeps no such journal.
    fn journals(&self) -> u64 {
        use dolos::storage::{ArchiveStoreBackend, StateStoreBackend};

        const MIB: u64 = 1024 * 1024;

        let state = match &self.stores.state {
            StateStoreBackend::Fjall(state) => state.tuning().journal_mb,
            _ => 0,
        };

        let archive = match &self.stores.archive {
            ArchiveStoreBackend::Fjall(archive) => archive.tuning().journal_mb,
            _ => 0,
        };

        (state + archive) * MIB
    }

    /// Where the restore writes.
    fn target(
        &self,
    ) -> dolos_snapshot::restore::Target<
        '_,
        impl dolos_core::ArchiveStore,
        impl dolos_core::StateStore,
    > {
        dolos_snapshot::restore::Target::new(&self.stores.archive, &self.stores.state)
    }
}

fn restore(
    config: &RootConfig,
    args: &Args,
    inputs: Inputs,
    options: RestoreOptions<'_>,
) -> miette::Result<()> {
    let node = Node::open(config)?;
    let progress = RestoreProgress::for_stderr(options.feedback);
    let observer = progress.observer();
    let restoring = node.restoring(&inputs, options);

    let mut opened = None;

    let (input, reading) = match &inputs.reader {
        Reader::Dir { dolos, blocks } => (
            RestoreInput::Directory {
                path: dolos,
                blocks: Some(blocks),
            },
            dolos.display().to_string(),
        ),
        Reader::Repo {
            dolos,
            blocks,
            auth,
        } => {
            // Staging resolution remains node policy. The profile facade
            // receives the resolved inputs and never reads configuration or
            // environment on its own. Both repositories stage in the same
            // directory, which the preflight sizes for the two.
            let scratch = node::scratch_dir(&config.storage, args.scratch_dir.as_deref());
            let open = |repo: &Repository| {
                SnapshotRepository::open(
                    repo,
                    args.insecure,
                    auth.clone(),
                    scratch.clone(),
                    registry::Tuning::default(),
                )
                .into_diagnostic()
                .with_context(|| format!("opening the repository {repo}"))
            };

            let (repository, blocks_repository) = opened.insert((open(dolos)?, open(blocks)?));

            println!("source:   {dolos} ({})", inputs.point);
            println!("blocks:   {blocks}");

            (
                RestoreInput::Repository {
                    repository,
                    point: inputs.point,
                    blocks: Some(blocks_repository),
                },
                dolos.to_string(),
            )
        }
    };

    report_resume(&inputs);

    let outcome = snapshot::restore(input, restoring, node.target(), Some(&observer));

    let outcome = match outcome {
        Err(dolos_snapshot::Error::Resume(refusal)) => return Err(refused(refusal)),
        outcome => outcome
            .into_diagnostic()
            .with_context(|| format!("restoring the stele from {reading}"))?,
    };

    progress.finish();

    report(&outcome.plan, &outcome.outlook, &outcome.summary);

    Ok(())
}

/// The line a resumed restore starts with, before its first layer.
fn report_resume(inputs: &Inputs) {
    if let Some(Resuming { record, layers, .. }) = &inputs.resuming {
        println!(
            "resuming: {} epoch-{} ({}), {layers} layers recorded",
            record.source, record.sequence, record.inscription_digest,
        );
    }
}

/// What the run did, in the numbers an operator checks.
///
/// Still printed after the restore, and now that is a choice rather than a gap:
/// the run itself is drawn while it happens through the profile observer seam,
/// so what is left for the end is the arithmetic a bar cannot carry.
fn report(
    plan: &dolos_snapshot::restore::Plan,
    outlook: &dolos_snapshot::restore::Outlook,
    summary: &dolos_snapshot::restore::Summary,
) {
    println!(
        "network:  {} ({})",
        plan.position.network.name(),
        plan.position.network.magic()
    );
    println!("cursor:   {}", plan.position.point);
    println!("sequence: {}", plan.sequence);

    if plan.skipped_epochs > 0 {
        println!(
            "epochs:   {} restored, {} skipped by history.window",
            plan.epochs.len(),
            plan.skipped_epochs,
        );
    } else {
        println!("epochs:   {}", plan.epochs.len());
    }

    // Printed only when it happened, and never folded into the epoch line: an
    // epoch dropped by the history window is this node's own configuration, and
    // a layer dropped for a kind this build does not implement is a stele from
    // a publisher ahead of it. An operator acts on the second by upgrading.
    if !plan.skipped_unknown.is_empty() {
        println!(
            "skipped:  {} layer(s) this build has no kind for ({}); upgrade to restore them",
            plan.skipped_unknown.len(),
            plan.skipped_kinds().join(", "),
        );
    }

    println!(
        "fetched:  {} layers ({} skipped), {} compressed bytes planned",
        summary.layers_fetched, summary.layers_skipped, outlook.remaining.compressed_bytes,
    );

    println!(
        "restored: {} blocks, {} logs, {} index records, {} first-seen entries, {} entities, {} utxos",
        summary.blocks,
        summary.logs,
        summary.index_records,
        summary.entity_index,
        summary.entities,
        summary.utxos,
    );
}

pub(super) fn run(
    config: &RootConfig,
    args: &Args,
    inputs: Inputs,
    feedback: &Feedback,
) -> miette::Result<()> {
    let options = RestoreOptions {
        feedback,
        skip_space_check: args.skip_space_check,
        read_ahead: args.read_ahead,
    };

    restore(config, args, inputs, options)
}

#[cfg(test)]
mod progress_tests {
    use dolos_snapshot::progress::{Event, Outcome, Progress as _};

    use crate::feedback::{Feedback, SteleProgress};

    const PER_RESTORE: usize = 3 + 16;

    /// The wiring `restore_dir` and `restore_repo` use, driven over a
    /// fixture-scale restore that resumes.
    ///
    /// The resume is the case worth rendering and the one worth testing: a
    /// skipped layer moves no blob and no bytes, so a bar that advanced only on
    /// bytes would stall on exactly the layers a resumed run gets for free.
    #[test]
    fn the_restore_renderer_tracks_a_resumed_fixture_scale_run() {
        let progress = SteleProgress::restoring(&Feedback::hidden());

        let epoch = serde_json::json!({"networkMagic": 2, "epoch": 0});
        let shard = serde_json::json!({"networkMagic": 2, "epoch": 0, "shard": 0});

        // The layer an earlier attempt had already committed: announced, closed,
        // nothing pulled for it.
        progress.on(Event::LayerStarted {
            index: 0,
            total: PER_RESTORE,
            kind: "blocks",
            scope: &epoch,
        });

        progress.on(Event::LayerFinished {
            index: 0,
            total: PER_RESTORE,
            kind: "blocks",
            outcome: Outcome::Skipped,
        });

        assert_eq!(
            progress.layers_position(),
            1,
            "a skip still advances the run"
        );
        assert_eq!(progress.blob_position(), 0);

        for index in 1..PER_RESTORE {
            let kind = if index < 3 { "logs" } else { "state" };
            let scope = if index < 3 { &epoch } else { &shard };

            progress.on(Event::LayerStarted {
                index,
                total: PER_RESTORE,
                kind,
                scope,
            });

            // A registry reader pulls the whole blob before a record comes back
            // out of it, which is the stretch this bar exists for.
            progress.on(Event::Blob {
                moved: true,
                bytes: 8_192,
            });

            for _ in 0..8 {
                progress.on(Event::Bytes(1_024));
            }

            progress.on(Event::Records(2_000));

            progress.on(Event::LayerFinished {
                index,
                total: PER_RESTORE,
                kind,
                outcome: Outcome::Transferred,
            });
        }

        assert_eq!(progress.layers_position(), PER_RESTORE as u64);
        assert_eq!(progress.layers_length(), Some(PER_RESTORE as u64));

        // A running total over every blob the restore pulled, rather than the
        // last one's size — see `SteleProgress`.
        let pulled = (PER_RESTORE as u64 - 1) * 8_192;

        assert_eq!(progress.blob_position(), pulled);
        assert_eq!(progress.blob_length(), Some(pulled));

        assert_eq!(
            progress.records_position(),
            2_000 * (PER_RESTORE as u64 - 1),
            "the skipped layer contributed no records"
        );

        progress.finish();
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use dolos_cardano::networks;

    use super::*;

    fn args(source: Option<&str>) -> Args {
        Args {
            source: source.map(|raw| raw.parse().unwrap()),
            point: Point::default(),
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            read_ahead: dolos_snapshot::restore::DEFAULT_READ_AHEAD,
            resume: false,
            credentials: CredentialArgs::default(),
        }
    }

    fn repository(source: Source) -> String {
        match source {
            Source::Repo(repo) => format!("{}/{}", repo.registry(), repo.repository()),
            Source::Dir(dir) => panic!("expected a repository, got {}", dir.display()),
        }
    }

    #[test]
    fn a_known_network_restores_from_its_official_repository() {
        let resolved = source(&args(None), Some(&networks::PREVIEW)).unwrap();

        assert_eq!(
            format!("oci://{}", repository(resolved)),
            networks::PREVIEW.stelae.registry
        );
    }

    #[test]
    fn an_explicit_source_wins() {
        let resolved = source(&args(Some("file://stele")), Some(&networks::PREVIEW)).unwrap();
        assert!(matches!(resolved, Source::Dir(dir) if dir == Path::new("stele")));

        let resolved = source(
            &args(Some("oci://ghcr.io/txpipe/dolos-snapshots/preview")),
            Some(&networks::PREVIEW),
        )
        .unwrap();
        assert_eq!(
            repository(resolved),
            "ghcr.io/txpipe/dolos-snapshots/preview"
        );
    }

    #[cfg(feature = "utils")]
    #[test]
    fn the_prompt_defaults_to_the_networks_official_repository() {
        assert_eq!(
            source_choices(Some(&networks::PREPROD)),
            [
                SourceChoice::Official(networks::PREPROD.stelae.registry),
                SourceChoice::Custom,
            ]
        );

        assert_eq!(source_choices(None), [SourceChoice::Custom]);

        let placeholder = custom_placeholder();
        assert!(placeholder.contains("oci.stelae.store"), "{placeholder}");
        assert!(placeholder.parse::<Source>().is_ok(), "{placeholder}");
    }

    /// A registry root resolves to its `dolos` and `blocks` repositories, and
    /// the root is what the restore records.
    #[test]
    fn a_registry_root_reads_its_dolos_and_blocks_repositories() {
        let storage = tempfile::tempdir().unwrap();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = storage.path().to_path_buf();

        for (given, root) in [
            (None, networks::PREVIEW.stelae.registry),
            (
                Some("oci://127.0.0.1:5000/mirror/preview"),
                "oci://127.0.0.1:5000/mirror/preview",
            ),
        ] {
            let inputs = resolve(&config, &args(given), Some(&networks::PREVIEW), false).unwrap();

            let Reader::Repo { dolos, blocks, .. } = &inputs.reader else {
                panic!("{root} did not resolve to repositories");
            };

            assert_eq!(dolos.to_string(), format!("{root}/dolos"));
            assert_eq!(blocks.to_string(), format!("{root}/blocks"));
            assert_eq!(inputs.source.to_string(), root);
        }
    }

    /// The network root a `file://` source names, with whichever of its two
    /// directories it lacks.
    fn root_without(missing: Option<&str>) -> (tempfile::TempDir, RootConfig) {
        let dir = tempfile::tempdir().unwrap();

        for name in [DOLOS_REPOSITORY, BLOCKS_REPOSITORY] {
            if Some(name) != missing {
                dolos_snapshot::dir::SnapshotDir::create(dir.path().join(name)).unwrap();
            }
        }

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = dir.path().join("data");

        (dir, config)
    }

    /// A root missing either directory is refused, naming the one it lacks.
    #[test]
    fn a_root_missing_a_directory_is_refused_naming_it() {
        for name in [DOLOS_REPOSITORY, BLOCKS_REPOSITORY] {
            let (root, config) = root_without(Some(name));

            let message = format!("{:?}", check_dir(&config, root.path(), false).unwrap_err());

            assert!(
                message.contains(&format!("holds no `{name}` directory")),
                "{message}"
            );
        }
    }

    /// A root with both directories is read for its stele: an empty `dolos`
    /// layout is refused as holding none.
    #[test]
    fn a_root_with_both_directories_is_read_for_its_stele() {
        let (root, config) = root_without(None);

        let message = check_dir(&config, root.path(), false)
            .unwrap_err()
            .to_string();

        assert!(message.contains("holds no stele"), "{message}");
        assert!(
            message.contains(&root.path().join(DOLOS_REPOSITORY).display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn a_custom_network_without_a_source_names_the_flag() {
        let message = source(&args(None), None).unwrap_err().to_string();

        assert!(message.contains("--source"), "{message}");
    }

    /// A `--continue` over a registry record reads the recorded repository
    /// at the recorded epoch's tag, never `latest`, which `--point` defaults
    /// to and the soak chart passes.
    #[test]
    fn a_registry_resume_reads_the_recorded_epoch_tag() {
        let storage = tempfile::tempdir().unwrap();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = storage.path().to_path_buf();

        let record = SourceRecord::new(
            &"oci://127.0.0.1:5000/cardano/preview".parse().unwrap(),
            42,
            format!("sha256:{}", "ab".repeat(32)).parse().unwrap(),
        )
        .unwrap();

        record
            .save(&dolos_snapshot::restore::source_path_in(storage.path()))
            .unwrap();

        let args = Args {
            resume: true,
            ..args(None)
        };
        assert_eq!(args.point, Point::Latest);

        let inputs = resolve(&config, &args, None, false).unwrap();

        assert_eq!(inputs.point, Point::Epoch(42));
        assert!(matches!(inputs.reader, Reader::Repo { .. }));
        assert_eq!(inputs.source.to_string(), record.source);

        let resuming = inputs.resuming.expect("nothing to resume");
        assert_eq!(resuming.record, record);
        assert_eq!(resuming.layers, 0);
    }

    /// Recorded layers are checked against an archive index only where one
    /// is kept on disk. With no archive, the restore itself refuses at its
    /// first index records, which this check must not pre-empt with `--force`
    /// advice; an archive in memory is empty after any run.
    #[test]
    fn recorded_layers_are_checked_only_against_an_archive_on_disk() {
        use dolos_core::config::{HistoryMode, StoreType};

        let storage = tempfile::tempdir().unwrap();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = storage.path().to_path_buf();

        still_holds(&config, 0).unwrap();

        let message = still_holds(&config, 3).unwrap_err().to_string();
        assert!(message.contains("archive index"), "{message}");
        assert!(message.contains("--force"), "{message}");

        let mut none = config.clone();
        none.history.mode = HistoryMode::None;
        still_holds(&none, 3).unwrap();

        let mut in_memory = config.clone();
        in_memory.storage.archive.kind = StoreType::Ephemeral;
        still_holds(&in_memory, 3).unwrap();
    }

    /// Over fjall stores, the journals the restore is handed are both stores'
    /// `journal_mb`, as their own tuning states it.
    #[test]
    fn the_restore_is_handed_both_fjall_journals() {
        use dolos::storage::{ArchiveStoreBackend, StateStoreBackend};

        let storage = tempfile::tempdir().unwrap();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = storage.path().to_path_buf();

        let node = Node::open(&config).unwrap();

        let (StateStoreBackend::Fjall(state), ArchiveStoreBackend::Fjall(archive)) =
            (&node.stores.state, &node.stores.archive)
        else {
            panic!("the default configuration opens fjall stores");
        };

        let expected = (state.tuning().journal_mb + archive.tuning().journal_mb) << 20;
        assert!(expected > 0);
        assert_eq!(node.journals(), expected);

        let inputs = Inputs {
            reader: Reader::Dir {
                dolos: storage.path().join("root/dolos"),
                blocks: storage.path().join("root/blocks"),
            },
            source: Source::Dir(storage.path().join("root")),
            point: Point::Latest,
            resuming: None,
        };
        let feedback = Feedback::hidden();
        let options = RestoreOptions {
            feedback: &feedback,
            skip_space_check: false,
            read_ahead: dolos_snapshot::restore::DEFAULT_READ_AHEAD,
        };

        assert_eq!(node.restoring(&inputs, options).journals, expected);
    }

    /// A node that keeps no archive is handed its state store's journal
    /// alone.
    #[test]
    fn a_node_without_an_archive_is_handed_only_its_state_journal() {
        use dolos::storage::StateStoreBackend;
        use dolos_core::config::HistoryMode;

        let storage = tempfile::tempdir().unwrap();

        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.storage.path = storage.path().to_path_buf();
        config.history.mode = HistoryMode::None;

        let node = Node::open(&config).unwrap();

        let StateStoreBackend::Fjall(state) = &node.stores.state else {
            panic!("the default configuration opens a fjall state store");
        };

        let expected = state.tuning().journal_mb << 20;
        assert!(expected > 0);
        assert_eq!(node.journals(), expected);
    }
}
