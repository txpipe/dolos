//! A stele in an OCI registry.
//!
//! This is the transport the format was designed for. Registries are
//! content-addressed, so a push asks whether each blob is already there and
//! sends only the ones that are not, and a pull reads a manifest that says
//! exactly which blob holds which layer. Everything before this module produced
//! a format *capable* of delta transfer; this is the half that performs any.
//!
//! It is the second implementation of [`crate::transport`] and adds no
//! vocabulary of its own: a registry is another place a
//! [`crate::dir::SteleDir`] could have been, and a profile driving one writes
//! the same code.
//!
//! ## Two documents, one stele
//!
//! A stele in a registry is an OCI image manifest whose config blob is the
//! inscription:
//!
//! ```text
//! manifest   artifactType application/vnd.stelae.stele.v1
//!   config   application/vnd.stelae.inscription.v1+json  -> the inscription
//!   layers[] the profile's media types, in inscription order,
//!            each annotated with its kind, its diffId and its scope
//! ```
//!
//! The two documents describe one thing from two sides. The inscription holds
//! **identity** — `diffId`s, over uncompressed bytes, reproducible by a
//! publisher who compressed differently. The manifest holds **transport** —
//! compressed digests and sizes, which are what a registry addresses a blob by
//! and are not stable across zstd versions. Neither is derivable from the
//! other, which is why [`crate::BlobIndex`] exists at all, and why a directory
//! has to reconstruct by brute force what a manifest states.
//!
//! Because they are two views of one stele, **a disagreement between them is a
//! refusal**, in either direction: a layer the inscription describes and the
//! manifest does not carry cannot be fetched, and a layer the manifest carries
//! and the inscription does not describe is a blob nothing attests. Positional
//! correspondence is checked as well as the `diffId` annotations, so the
//! ordering the canonical document fixes is the ordering on the wire.
//!
//! ## Bounded by one layer, in both directions
//!
//! [`oci_client::Client::push_blob_stream`] needs the blob's digest *up front*,
//! and a layer's digest is only known once its last record has been written. So
//! a layer is staged into a temporary file exactly as a directory stages one,
//! and then streamed up from it. A pulled layer is streamed to a temporary file
//! and read back synchronously.
//!
//! Neither direction ever holds a whole stele or a whole layer. The staging
//! files are unlinked at creation, so an abandoned push or a failed pull leaves
//! nothing behind and needs no cleanup path of its own.
//!
//! ## The async boundary, and the one rule it comes with
//!
//! `oci-client` is async and this crate is not: `export` and `restore` are
//! synchronous iterator code driving fallible store iterators, and threading a
//! runtime through them would change every profile's shape for the benefit of
//! one transport. So the transport owns **one current-thread runtime** and
//! enters it with `block_on` at each call — the idiom `dolos bootstrap mithril`
//! already uses.
//!
//! **A [`Registry`] must never be used from inside an async context, and must
//! never be dropped inside one.** `Runtime::block_on` panics when called from a
//! runtime thread, and dropping a runtime from inside one panics too. Every
//! caller today is a synchronous CLI path, which is what makes this safe; a
//! caller that is not is a design question, and the answer is not a second
//! runtime.
//!
//! ## TLS, and the second rule it comes with
//!
//! The client speaks TLS through rustls, built with **no crypto provider wired
//! in** (`reqwest/rustls-no-provider`). The alternative is the backend
//! `oci-client`'s own `rustls-tls` feature selects, `aws-lc-rs`, whose
//! `aws-lc-sys` needs `cmake` on the build machine — a build tool this
//! protocol will not make a contributor install to compile a snapshot format.
//! `crates/stelae/Cargo.toml` records which dependency each half of that
//! choice lands on.
//!
//! The trade is the same one the async boundary makes: a guarantee moves from
//! build time to run time.
//!
//! **A process that opens a [`Registry`] must have installed a process-default
//! [`rustls`] `CryptoProvider` before it does so.** Nothing here can do it —
//! the choice of provider belongs to the program, not to one of its
//! transports, and a library that installed one would silently win a race
//! against whatever its host had chosen. Omitting it panics inside
//! [`Registry::open`], where `oci-client` builds its HTTP client: `reqwest`
//! resolves its TLS backend there, before any request and before any URL
//! scheme, so [`Options::insecure`] does not spare a plaintext registry.
//! That is the worse failure mode being bought — a runtime abort rather than a
//! link error — though the panic does name the missing feature.
//!
//! In Dolos this is `main()`, which installs `ring` for `mithril-client`'s
//! sake and covers this transport by the same line. In this crate's own tests
//! it is an explicit install in the fixture, so the suite proves the
//! precondition rather than inheriting a provider by luck.
//!
//! [`rustls`]: https://docs.rs/rustls
//!
//! ## Authentication
//!
//! Anonymous, a bearer token, or a Basic credential pair — whichever the caller
//! puts in [`Options::auth`]. That is the whole of it: [`Auth`] is a value the
//! caller constructs and hands over.
//!
//! **Where those credentials came from is not this crate's business, and it has
//! no way to ask.** A protocol library that read an environment variable would
//! be deciding its host's credential policy for it, and naming the variable
//! would freeze that decision into a published API — a program embedding this
//! transport gets no say in either. So a host reads its own environment, its
//! own configuration file, its own secret manager, or all three in whatever
//! order it has decided, and the answer arrives here as an [`Auth`].
//!
//! In Dolos that host is the `dolos` binary; `dolos::common` holds the
//! variables and the precedence between them.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_util::Stream;
use oci_client::{
    client::{ClientConfig, ClientProtocol},
    manifest::{OciDescriptor, OciImageManifest, OCI_IMAGE_MEDIA_TYPE},
    secrets::RegistryAuth,
    Client,
};

pub use oci_client::Reference;

use crate::{
    digest::{LayerDigests, LayerWriter},
    frame::{CanonicalCbor, Limits, SeqWriter},
    inscription::{canonical_json, Inscription, LayerDescriptor},
    layer::LayerReader,
    profile::{checked_tag_for_sequence, validate_tag, Profile},
    progress::{Event, Observer},
    transport::{
        open_layer, BlobIndex, LayerSpec, RecordSink, SteleReader, SteleWriter, WrittenLayer,
    },
    Digest, Error, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE, MANIFEST_SIZE_LIMIT,
};

/// How a [`Registry`] authenticates.
///
/// The three shapes `oci-client` implements, named here rather than re-exported
/// so that a caller assembling credentials does not have to depend on the
/// registry client this transport happens to be built on. Constructing one is
/// the caller's whole side of the arrangement: this crate never sources
/// credentials, so there is no `from_env` here and no variable name for a host
/// to inherit.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum Auth {
    /// No credentials. What a genuinely public repository wants, and what a
    /// registry that authenticates every request will answer with a 401.
    #[default]
    Anonymous,
    /// A bearer token, as GHCR and the token-exchange registries issue.
    Bearer(String),
    /// A user and password, sent as HTTP Basic. What a registry fronted by
    /// htpasswd — or by a Worker checking a credential table — expects.
    Basic { user: String, password: String },
}

/// Says which shape it is and never what is in it.
///
/// A transport is held in structures that get logged and printed in error
/// context; a derived `Debug` would put a publisher's password in the first
/// backtrace anybody pastes into an issue.
impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => f.write_str("Anonymous"),
            Self::Bearer(_) => f.write_str("Bearer(<redacted>)"),
            Self::Basic { user, .. } => f
                .debug_struct("Basic")
                .field("user", user)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

impl Auth {
    /// Whether these credentials name anybody.
    ///
    /// The question a host layering credential sources asks — "did that one say
    /// anything, or do I fall through to the next?" — so it is answered here
    /// rather than by every host matching on the variant.
    pub fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous)
    }

    fn to_registry_auth(&self) -> RegistryAuth {
        match self {
            Self::Anonymous => RegistryAuth::Anonymous,
            Self::Bearer(token) => RegistryAuth::Bearer(token.clone()),
            Self::Basic { user, password } => RegistryAuth::Basic(user.clone(), password.clone()),
        }
    }
}

/// Annotation naming a layer's profile-defined kind.
///
/// The three annotation keys below are the specification's: ADR-004's "OCI
/// layout and the inscription" section names them, reverse-DNS under
/// `stelae.store`, a domain TxPipe owns. They are transport metadata and sit
/// outside the inscription, so they are outside a stele's identity — but only
/// two of them are informational. [`DIFF_ID_ANNOTATION`] is normative, because
/// it is load-bearing on the way back: it *is* the identity→blob map a
/// directory has to rebuild by decompressing everything. The golden freezes
/// all three.
pub const KIND_ANNOTATION: &str = "store.stelae.layer.kind";

/// Annotation carrying a layer's `diffId` — its identity, and the key of the
/// map a pull reads off the manifest.
pub const DIFF_ID_ANNOTATION: &str = "store.stelae.layer.diffId";

/// Annotation carrying the canonical JSON of a layer's profile-owned scope.
///
/// Informational: a human or a generic tool reading the manifest can see which
/// epoch or shard a blob covers without fetching the config blob.
pub const SCOPE_ANNOTATION: &str = "store.stelae.layer.scope";

/// How much of a staged layer is held in memory on the way up.
///
/// One chunk at a time, allocated and handed to the client, which sends it as
/// one `PATCH`. Well under `oci-client`'s own 4 MiB ceiling per chunk, so this
/// number — and not the layer's size — is what bounds the upload.
const UPLOAD_CHUNK: usize = 1024 * 1024;

/// What a push moved, and what it did not have to.
///
/// The blob-skip is the whole point of a content-addressed registry, so its
/// outcome is a number the caller gets back rather than a line in a log: a
/// publisher that believes it is transferring a delta can check.
///
/// Counts layer blobs only. The config blob — the inscription — is small, is
/// different for every stele by construction, and would only blur the number
/// that matters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Transfer {
    /// Layer blobs the registry did not have, and that were uploaded.
    pub layers_uploaded: u64,
    /// Layer blobs the registry already had, and that were not.
    pub layers_skipped: u64,
    /// Layer blobs that were never built, because [`Registry::adopt_layer`]
    /// took them from a stele already in this repository — or because
    /// [`Registry::adopt_carried`] took them from an earlier attempt of this
    /// same publish.
    ///
    /// Deliberately not folded into `layers_skipped`. A skipped layer was
    /// built, hashed and then found to be present already, so the publisher
    /// paid for it and saved only the upload; an adopted one was never read out
    /// of a store at all. They are different costs and a publisher comparing
    /// two publishes wants to tell them apart.
    pub layers_reused: u64,
    /// Compressed bytes uploaded.
    pub bytes_uploaded: u64,
    /// Compressed bytes the skip saved.
    pub bytes_skipped: u64,
    /// Compressed bytes an adopted layer did not move, as the manifest or the
    /// record it came from reports them.
    pub bytes_reused: u64,
}

/// How to reach a registry.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Talk to the registry over plaintext HTTP rather than HTTPS.
    ///
    /// For a registry on a loopback address — a test fixture, or a mirror
    /// inside a cluster. Never for anything reachable from outside one.
    pub insecure: bool,

    /// Where layers are staged on the way up and pulled blobs land on the way
    /// down. Defaults to the platform temporary directory.
    ///
    /// Worth setting: a mainnet state shard is hundreds of megabytes
    /// compressed, and the platform temporary directory is not always on the
    /// volume with room for sixteen of them.
    pub scratch_dir: Option<PathBuf>,

    /// How to authenticate, decided entirely by the caller.
    ///
    /// Defaults to [`Auth::Anonymous`]. Nothing in this crate sources
    /// credentials — see the module documentation for why that is a boundary
    /// rather than an omission.
    pub auth: Auth,
}

/// A repository an operator named, as `oci://HOST/PATH`.
///
/// The `oci://` scheme is not this project's invention — it is how Helm, ORAS
/// and the rest of the ecosystem spell "this URL names an OCI registry
/// reference" — so parsing it belongs here, beside the client, rather than in
/// every command that takes one from a human.
///
/// **Everything about the name is decided here, once.** That is the whole point
/// of the type: [`Registry::open`] used to take the host and the repository
/// path as two already-split strings, which meant every caller split the URL
/// itself and then handed back the pieces this module immediately glued
/// together again — while the only crate holding the grammar to split it
/// *correctly* was this one.
///
/// Three things are refused, and the third is the one a hand-written splitter
/// gets wrong:
///
/// - **A tag or a digest.** `oci://…/dolos:v1` names a stele, and which stele
///   is not part of naming the repository — a profile renders the tags, and a
///   caller that wants a particular one says so separately.
/// - **An empty host or path**, so the two halves a client needs both exist.
/// - **A host the distribution grammar would have inferred rather than read.**
///   [`Reference`]'s own parser applies registry defaults: a first component
///   with no dot and no colon is not a host at all, and `dolos/mainnet`
///   silently becomes `docker.io/dolos/mainnet`. Parsing and then checking that
///   the registry it reports is the text the operator actually wrote is what
///   turns that rewrite into a refusal. It also buys the rest of the grammar —
///   lowercase components, `.`/`_`/`-` separators, no empty segments — from the
///   parser that defines it rather than from a second copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repository {
    registry: String,
    repository: String,
}

impl Repository {
    /// The registry host, with its port if it has one.
    pub fn registry(&self) -> &str {
        &self.registry
    }

    /// The repository path within that registry.
    pub fn repository(&self) -> &str {
        &self.repository
    }
}

impl std::str::FromStr for Repository {
    type Err = Error;

    fn from_str(raw: &str) -> Result<Self, Error> {
        let bad = |why: &str| Error::InvalidRepository {
            value: raw.to_owned(),
            reason: why.to_owned(),
        };

        let rest = raw
            .strip_prefix(SCHEME)
            .ok_or_else(|| bad(&format!("it does not start with `{SCHEME}`")))?;

        let (registry, repository) = rest
            .split_once('/')
            .ok_or_else(|| bad("it names a registry but no repository path"))?;

        if registry.is_empty() {
            return Err(bad("it names no registry host"));
        }

        if repository.is_empty() || repository.ends_with('/') {
            return Err(bad("it names no repository path"));
        }

        // A host may carry a port, so only the path is asked about a reference.
        if repository.contains(':') || repository.contains('@') {
            return Err(bad(
                "it names a tag or a digest, and which stele to read is not part of naming \
                 the repository",
            ));
        }

        let reference: Reference = rest
            .parse()
            .map_err(|_| bad("its repository path is not a valid OCI name"))?;

        // The parser applies registry defaults, so a first component it did not
        // recognise as a host became part of the repository under `docker.io`.
        // Publishing to a registry the operator did not name is worse than
        // refusing, and this comparison is the only thing standing between the
        // two.
        if reference.registry() != registry {
            return Err(bad(&format!(
                "{registry:?} is not a registry host, so this would address \
                 {:?} instead",
                reference.registry(),
            )));
        }

        Ok(Self {
            registry: registry.to_owned(),
            repository: repository.to_owned(),
        })
    }
}

/// Back in the spelling it was read from, so an error message names what the
/// operator typed.
impl std::fmt::Display for Repository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{SCHEME}{}/{}", self.registry, self.repository)
    }
}

/// The URL scheme an OCI registry reference is named by.
///
/// The ecosystem's, not this protocol's: Helm, ORAS and others already use it
/// for exactly this.
pub const SCHEME: &str = "oci://";

/// A stele repository in an OCI registry.
///
/// Implements [`SteleWriter`], so a profile publishes into it exactly as it
/// would into a directory. Reading is [`Registry::pull`], which resolves a tag
/// into a [`Stele`] — the read handle, and the thing that implements
/// [`SteleReader`].
///
/// See the module documentation before calling any of this from async code.
pub struct Registry {
    shared: Arc<Shared>,
}

struct Shared {
    runtime: tokio::runtime::Runtime,
    client: Client,
    /// Registry and repository. The tag is a placeholder — blob operations do
    /// not use one, and manifest operations build their own.
    repository: Reference,
    /// The same repository in the spelling it was opened with, for a caller
    /// that has to write down where this transport publishes.
    name: Repository,
    auth: RegistryAuth,
    scratch_dir: Option<PathBuf>,
    state: Mutex<PushState>,
    /// Who is watching this connection, in either direction.
    ///
    /// Beside the push state rather than inside it, because a [`Stele`] shares
    /// this value and only ever reads: attaching an observer to the
    /// [`Registry`] is what makes the pull it resolves report too, which is the
    /// property a restore depends on — the reader is created inside the driver
    /// and there is no other moment to wire it.
    observer: Mutex<Observer>,
}

#[derive(Default)]
struct PushState {
    /// Layers finished since the last [`SteleWriter::seal`], in finish order.
    pending: Vec<WrittenLayer>,
    transfer: Transfer,
}

impl Registry {
    /// Open a repository — e.g. `oci://ghcr.io/txpipe/dolos-snapshots/mainnet`,
    /// already parsed into a [`Repository`].
    ///
    /// Takes the name as one value rather than as a pre-split pair, because
    /// splitting it correctly needs the distribution grammar and this is the
    /// only crate that has it. A caller holding a string an operator typed
    /// parses it into a [`Repository`] and hands that over; nothing outside
    /// this module needs to know where the host ends.
    ///
    /// Builds the current-thread runtime the whole transport runs on, and
    /// stores the credentials [`Options::auth`] carries so they never have to
    /// be threaded through a profile's call stack.
    ///
    /// # Panics
    ///
    /// If no process-default [`rustls`] `CryptoProvider` has been installed —
    /// see the module documentation. The panic comes from `reqwest`, which
    /// resolves its TLS backend when `oci-client` builds the HTTP client here,
    /// so [`Options::insecure`] does not avoid it.
    ///
    /// [`rustls`]: https://docs.rs/rustls
    pub fn open(repository: &Repository, options: Options) -> Result<Self, Error> {
        let protocol = if options.insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };

        let client = Client::new(ClientConfig {
            protocol,
            ..Default::default()
        });

        // The tag is never read: `Reference` is the client's way of naming a
        // repository, and every manifest operation below builds its own.
        let reference = Reference::with_tag(
            repository.registry.clone(),
            repository.repository.clone(),
            crate::MOVING_TAG.to_owned(),
        );

        let auth = options.auth.to_registry_auth();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        // Stored once rather than passed per call: the client's own
        // authenticated operations take credentials as an argument, but
        // `blob_exists`, `push_blob_stream` and `push_manifest_raw` do not —
        // they look them up here.
        runtime.block_on(client.store_auth_if_needed(reference.resolve_registry(), &auth));

        Ok(Self {
            shared: Arc::new(Shared {
                runtime,
                client,
                repository: reference,
                name: repository.clone(),
                auth,
                scratch_dir: options.scratch_dir,
                state: Mutex::new(PushState::default()),
                observer: Mutex::new(Observer::silent()),
            }),
        })
    }

    /// Where this transport stages the layers it moves, in either direction.
    ///
    /// [`Options::scratch_dir`] as it was given, so a caller that wants to
    /// size the staging volume asks the transport that will use it rather than
    /// re-deriving the path it handed over — two derivations of one directory
    /// is one more than can be kept in step. `None` is the platform temporary
    /// directory, which is not a path this can name because
    /// [`Shared::scratch`] never names one either.
    pub fn scratch_dir(&self) -> Option<&Path> {
        self.shared.scratch_dir.as_deref()
    }

    /// The repository this transport was opened on.
    ///
    /// Here for the reason [`Registry::scratch_dir`] is: a caller that has to
    /// write down where its layers went asks the transport that sent them,
    /// rather than carrying the name alongside the handle. Two spellings of one
    /// destination is one more than can be kept in step, and the one that
    /// drifts is the one a later run compares against.
    pub fn repository(&self) -> &Repository {
        &self.shared.name
    }

    /// What has been pushed through this transport since it was opened, or
    /// since the last [`Registry::take_transfer`].
    pub fn transfer(&self) -> Transfer {
        self.shared.locked().transfer
    }

    /// The same numbers, and reset — so a publisher pushing several steles
    /// through one transport reads each one's cost rather than a running total.
    pub fn take_transfer(&self) -> Transfer {
        std::mem::take(&mut self.shared.locked().transfer)
    }

    /// Resolve `tag` into a readable stele.
    ///
    /// The order is the specification, and every step is what makes the next
    /// one safe:
    ///
    /// 1. tag → manifest, which names the config blob;
    /// 2. config blob → its bytes, bounded by the size the manifest claims and
    ///    verified against the digest it is addressed by;
    /// 3. those bytes → the inscription, whose *own* digest must equal that
    ///    same config digest. That is the one place identity and transport are
    ///    held against each other, and it is what stops a manifest from
    ///    pointing at a document nobody signed;
    /// 4. [`Inscription::check_profile`] — **before any layer is fetched**, so
    ///    a stele of another profile costs one small GET and not a partial
    ///    restore. The read-side check, deliberately: a pull serves a reader,
    ///    and a caller that is about to *publish* on top of what it pulled owes
    ///    the stricter [`Inscription::check_profile_strict`] of its own;
    /// 5. manifest ↔ inscription cross-check, which yields the [`BlobIndex`].
    pub fn pull(&self, profile: &dyn Profile, tag: &str) -> Result<Stele, Error> {
        validate_tag(tag)?;

        let reference = self.shared.tagged(tag);

        let (manifest, _digest) = self.shared.runtime.block_on(
            self.shared
                .client
                .pull_image_manifest(&reference, &self.shared.auth),
        )?;

        check_envelope(&manifest)?;

        let raw = self.shared.pull_blob_bytes(&reference, &manifest.config)?;
        let inscription = Inscription::parse(&raw)?;

        if inscription.canonicalize()? != raw {
            return Err(Error::NonCanonicalInscription);
        }

        let config_digest = manifest.config.digest.parse::<Digest>()?;
        let identity = inscription.digest()?;

        if identity != config_digest {
            return Err(Error::DigestMismatch {
                subject: "inscription".to_owned(),
                expected: config_digest.to_string(),
                actual: identity.to_string(),
            });
        }

        inscription.check_profile(profile)?;

        let blobs = read_manifest(&manifest, &inscription)?;

        Ok(Stele {
            shared: Arc::clone(&self.shared),
            reference,
            manifest,
            inscription,
            blobs,
        })
    }

    /// Resolve the immutable tag `profile` renders for `sequence`.
    pub fn pull_sequence(&self, profile: &dyn Profile, sequence: u64) -> Result<Stele, Error> {
        let tag = checked_tag_for_sequence(profile, sequence)?;
        self.pull(profile, &tag)
    }

    /// Resolve the profile's moving tag — the most recent stele.
    pub fn pull_latest(&self, profile: &dyn Profile) -> Result<Stele, Error> {
        let tag = profile.moving_tag().to_owned();
        self.pull(profile, &tag)
    }

    /// The most recent stele, or `None` if this repository has never held one.
    ///
    /// The whole value of this over [`Registry::pull_latest`] is the
    /// distinction it draws, and the distinction is load-bearing rather than
    /// convenient. A publisher chains each stele to the one before it, so
    /// "there is nothing to chain to" starts a history and *anything else*
    /// must not: a timeout, a 500 or an expired token read as absence would
    /// silently restart the chain, which is the exact outcome an inscription's
    /// `history` exists to prevent. So only the shapes a registry uses to say
    /// "no such manifest" become `None`, and every other failure propagates.
    ///
    /// Those shapes are three, because `oci-client` reports a 404 in three
    /// ways depending on which layer of the client noticed it. Matching them
    /// here rather than at a caller is the point: this is the only module in
    /// the crate that has any business naming an `oci_client` error type.
    pub fn latest(&self, profile: &dyn Profile) -> Result<Option<Stele>, Error> {
        match self.pull_latest(profile) {
            Ok(stele) => Ok(Some(stele)),
            Err(Error::Registry(e)) if is_absent(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Carry a layer this repository already holds into the stele being
    /// written, without building it.
    ///
    /// This is the operation a content-addressed registry makes possible and a
    /// directory does not: the caller has established — by whatever rule its
    /// profile owns — that a layer it *would* write is the layer a previous
    /// stele already published, so the bytes need neither be produced nor sent,
    /// and the new manifest simply points at the blob the old one pointed at.
    ///
    /// **The new stele attests a layer it did not reproduce.** That is the
    /// trade, and it is the caller's to make: nothing here can check that the
    /// descriptor describes those bytes, because checking would mean reading
    /// them, which is the cost being avoided. What *is* checked is that the
    /// blob is still there — one `HEAD`, before the manifest is written —
    /// because a descriptor pointing at a blob the registry has reclaimed is a
    /// stele nobody can restore, and it would be published looking perfectly
    /// well-formed.
    ///
    /// The caller names the layer by its `descriptor` — identity, out of an
    /// inscription — and the stele it came from. The blob digest and the
    /// compressed size are read off *that stele's manifest*, by exactly the
    /// lookup [`SteleReader::stream_layer`] uses, rather than passed in beside
    /// the descriptor: they are transport facts, they belong to the manifest,
    /// and a caller assembling the pair by hand is a caller that can mismatch
    /// them.
    pub fn adopt_layer(&self, source: &Stele, descriptor: LayerDescriptor) -> Result<(), Error> {
        let missing = || Error::LayerNotFound {
            kind: descriptor.kind.clone(),
            diff_id: descriptor.diff_id.to_string(),
        };

        let blob = source
            .blobs
            .blob_for(&descriptor.diff_id)
            .ok_or_else(missing)?;
        let named = blob.to_string();

        let oci = source
            .manifest
            .layers
            .iter()
            .find(|layer| layer.digest == named)
            .ok_or_else(missing)?;

        // Refused rather than clamped. A descriptor's size is an `i64` and a
        // negative one is a manifest saying something impossible; clamping it
        // to zero would carry that zero into the new manifest, where it becomes
        // the ceiling a later reader holds the download to — so the stele would
        // publish looking well-formed and refuse to restore. Every other
        // malformed-manifest shape here is a refusal, and this is one too.
        let compressed_size = u64::try_from(oci.size).map_err(|_| {
            Error::ManifestMismatch(format!(
                "layer {:?} ({}) claims a compressed size of {}",
                descriptor.kind, descriptor.diff_id, oci.size,
            ))
        })?;

        let adopted = WrittenLayer {
            digests: LayerDigests {
                diff_id: descriptor.diff_id,
                blob_digest: blob,
                uncompressed_size: descriptor.uncompressed_size,
                compressed_size,
            },
            descriptor,
        };

        let kind = adopted.descriptor.kind.clone();
        let diff_id = adopted.descriptor.diff_id;

        // A manifest naming a blob the registry no longer has is a refusal and
        // not a miss: the stele that named it is published, and something has
        // reclaimed underneath it. See [`Registry::adopt_carried`] for the case
        // where the same absence is merely a rebuild.
        if !self.adopt_carried(adopted)? {
            return Err(Error::BlobMissing {
                kind,
                diff_id: diff_id.to_string(),
                blob: named,
            });
        }

        Ok(())
    }

    /// Carry a layer that is already in this repository, named in full.
    ///
    /// [`Registry::adopt_layer`] with the lookup already done — for a caller
    /// holding a [`WrittenLayer`] this transport produced earlier rather than a
    /// stele to read one out of. The pair is still not assembled by hand: it is
    /// the measurement [`RecordSink::finish`] returned when the blob went up,
    /// carried across whatever interruption the caller survived.
    ///
    /// **The blob check is a verdict, not an assertion.** `Ok(false)` means the
    /// registry does not hold it and nothing was carried, which is a caller's
    /// cue to build the layer after all. That is the difference from
    /// [`Registry::adopt_layer`], where the same answer is an error: a
    /// published manifest that names a reclaimed blob is a stele nobody can
    /// restore, while a caller's own note that has gone stale costs a rebuild
    /// and nothing else.
    ///
    /// Nothing here checks that the descriptor describes those bytes, for the
    /// reason [`Registry::adopt_layer`] gives: checking would mean reading
    /// them, which is the cost being avoided.
    pub fn adopt_carried(&self, layer: WrittenLayer) -> Result<bool, Error> {
        if !self.shared.blob_exists(&layer.digests.blob_digest)? {
            return Ok(false);
        }

        let mut state = self.shared.locked();
        state.transfer.layers_reused += 1;
        state.transfer.bytes_reused += layer.digests.compressed_size;
        state.pending.push(layer);

        Ok(true)
    }

    /// The layer this transport is carrying for the next seal under `diff_id`.
    ///
    /// What [`SteleWriter::seal`] would put in the manifest, asked for one
    /// layer at a time — so a caller recording what it has finished records
    /// the transport's own measurement rather than a reconstruction of it.
    /// `None` once the layers have been spent by a seal, and for a `diffId`
    /// this transport never wrote.
    pub fn carried(&self, diff_id: &Digest) -> Option<WrittenLayer> {
        self.shared
            .locked()
            .pending
            .iter()
            .find(|layer| layer.digests.diff_id == *diff_id)
            .cloned()
    }
}

/// Whether a registry error means "no such manifest" rather than "something
/// went wrong".
///
/// `oci-client` does not normalize this, and the three shapes are not
/// interchangeable in practice: `distribution` answers a missing tag with a
/// `MANIFEST_UNKNOWN` envelope, a repository that has never existed with
/// `NAME_UNKNOWN`, and some registries answer with a bare 404 that the client
/// turns into `ImageManifestNotFoundError` or a `ServerError`. The client's own
/// referrers fallback matches the same set, for the same reason.
fn is_absent(error: &oci_client::errors::OciDistributionError) -> bool {
    use oci_client::errors::{OciDistributionError as E, OciErrorCode};

    match error {
        E::ImageManifestNotFoundError(_) => true,
        E::ServerError { code: 404, .. } => true,
        E::RegistryError { envelope, .. } => envelope.errors.iter().any(|e| {
            matches!(
                e.code,
                OciErrorCode::ManifestUnknown | OciErrorCode::NameUnknown
            )
        }),
        _ => false,
    }
}

/// A staged file, created where [`Options::scratch_dir`] said.
///
/// Both calls that can fail against a *named* directory raise
/// [`Error::Scratch`], whose docstring carries the reason. The unnamed case
/// keeps the catch-all [`Error::Io`], because the platform temporary
/// directory is not a path anybody chose.
///
/// Creating the directory lazily, here, is load-bearing elsewhere: it is what
/// makes a staging directory that exists after a run evidence that the run
/// staged in it. Nothing else creates it.
///
/// A free function rather than a method so it can be tested against an
/// unusable directory without standing up a registry client — see
/// `an_unusable_staging_directory_names_itself`.
fn scratch_in(dir: Option<&Path>) -> Result<File, Error> {
    let Some(dir) = dir else {
        return Ok(tempfile::tempfile()?);
    };

    let staged = |source| Error::Scratch {
        dir: dir.to_path_buf(),
        source,
    };

    std::fs::create_dir_all(dir).map_err(staged)?;

    tempfile::tempfile_in(dir).map_err(staged)
}

impl Shared {
    fn locked(&self) -> std::sync::MutexGuard<'_, PushState> {
        // A poisoned lock means a push panicked while holding it. The counters
        // are plain integers and the pending list is append-only, so what is
        // behind the lock is still coherent; refusing to look at it would turn
        // one failed push into a transport nobody can use.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A handle on whoever is watching, taken once per operation.
    ///
    /// Cloned out from under the lock rather than emitted through it: a blob
    /// download reports a delta per write, and holding a mutex across a
    /// renderer's call would put this transport's byte loop behind whatever the
    /// binary does with the event.
    fn observer(&self) -> Observer {
        self.observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn watch(&self, observer: Observer) {
        *self
            .observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = observer;
    }

    fn tagged(&self, tag: &str) -> Reference {
        Reference::with_tag(
            self.repository.registry().to_owned(),
            self.repository.repository().to_owned(),
            tag.to_owned(),
        )
    }

    fn scratch(&self) -> Result<File, Error> {
        scratch_in(self.scratch_dir.as_deref())
    }

    fn blob_exists(&self, digest: &Digest) -> Result<bool, Error> {
        Ok(self.runtime.block_on(
            self.client
                .blob_exists(&self.repository, &digest.to_string()),
        )?)
    }

    /// Upload a staged layer, unless the registry already has it.
    ///
    /// The existence check *is* the blob-skip — the whole delta-transfer claim
    /// reduces to this one `HEAD` per layer — and both outcomes are counted.
    fn put_layer(&self, layer: &WrittenLayer, staged: File) -> Result<(), Error> {
        let digest = layer.digests.blob_digest;
        let size = layer.digests.compressed_size;
        let observer = self.observer();

        if self.blob_exists(&digest)? {
            // Announced even though nothing moves: "the registry already had
            // this one" is the blob-skip working, and a watcher that only heard
            // about uploads would read the whole point of a content-addressed
            // registry as a stall.
            observer.emit(Event::Blob {
                moved: false,
                bytes: size,
            });

            let mut state = self.locked();
            state.transfer.layers_skipped += 1;
            state.transfer.bytes_skipped += size;
            state.pending.push(layer.clone());

            return Ok(());
        }

        // Before the upload rather than after it, so a watcher knows how big
        // the transfer it is about to see is while it is still happening.
        observer.emit(Event::Blob {
            moved: true,
            bytes: size,
        });

        self.runtime.block_on(self.client.push_blob_stream(
            &self.repository,
            blob_stream(staged, observer),
            &digest.to_string(),
        ))?;

        let mut state = self.locked();
        state.transfer.layers_uploaded += 1;
        state.transfer.bytes_uploaded += size;
        state.pending.push(layer.clone());

        Ok(())
    }

    /// Upload a small blob a caller already holds — the inscription, and
    /// nothing else.
    fn put_bytes(&self, digest: &Digest, bytes: Vec<u8>) -> Result<(), Error> {
        if self.blob_exists(digest)? {
            return Ok(());
        }

        self.runtime.block_on(self.client.push_blob(
            &self.repository,
            bytes,
            &digest.to_string(),
        ))?;

        Ok(())
    }

    /// Fetch a small blob into memory, bounded by the size its descriptor
    /// claims.
    ///
    /// Only the config blob comes back this way. A layer never does — see
    /// [`Shared::pull_blob_file`].
    fn pull_blob_bytes(
        &self,
        reference: &Reference,
        descriptor: &OciDescriptor,
    ) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(descriptor.size.max(0) as usize);

        // No observer: the config blob is the inscription, not a layer, and a
        // watcher summing byte deltas against a layer total would find them
        // disagreeing by however large the document is.
        self.runtime.block_on(self.client.pull_blob(
            reference,
            descriptor,
            Blocking::new(
                &mut buffer,
                descriptor.size,
                &descriptor.digest,
                Observer::silent(),
            ),
        ))?;

        Ok(buffer)
    }

    /// Fetch a layer blob into a temporary file, ready to be read back.
    ///
    /// `pull_blob` verifies the blob digest as the bytes go past, which is the
    /// transport half of the check; the identity half is the `diffId`, and
    /// belongs to [`LayerReader::finish`].
    fn pull_blob_file(
        &self,
        reference: &Reference,
        descriptor: &OciDescriptor,
    ) -> Result<File, Error> {
        let mut file = self.scratch()?;
        let observer = self.observer();

        // The whole layer lands here before `stream_layer` yields one record,
        // so this loop is where a restore spends nearly all of its time and the
        // only place it can report from.
        observer.emit(Event::Blob {
            moved: true,
            bytes: descriptor.size.max(0) as u64,
        });

        self.runtime.block_on(self.client.pull_blob(
            reference,
            descriptor,
            Blocking::new(&mut file, descriptor.size, &descriptor.digest, observer),
        ))?;

        file.seek(SeekFrom::Start(0))?;

        Ok(file)
    }
}

impl SteleWriter for Registry {
    type Sink = RegistrySink;

    fn layer_sink(
        &self,
        profile: &dyn Profile,
        spec: &LayerSpec,
        level: i32,
    ) -> Result<RegistrySink, Error> {
        let (sequence, media_type) = open_layer(profile, spec, level, || self.shared.scratch())?;

        Ok(RegistrySink {
            shared: Arc::clone(&self.shared),
            sequence,
            kind: spec.kind.clone(),
            media_type,
            scope: spec.scope.clone(),
        })
    }

    /// Put the second descriptor in the list [`SteleWriter::seal`] builds the
    /// manifest from.
    ///
    /// The override the default's documentation asks for: this transport pairs
    /// every layer the inscription describes against a layer it wrote, and a
    /// descriptor with nothing beside it fails the seal. Nothing is uploaded
    /// and nothing is checked — the blob went up when the first descriptor's
    /// sink finished, in this same publish, so there is no state of the
    /// registry under which it is there for one name and absent for the other.
    ///
    /// Counted as **skipped** rather than reused, by the distinction
    /// [`Transfer::layers_reused`] draws: these bytes were built out of a
    /// store, hashed, and then not uploaded again — which is a blob-skip
    /// exactly, and not a layer that was never read.
    ///
    /// ## And silent, unlike the blob-skip on the upload path
    ///
    /// That path emits [`Event::Blob`] with `moved: false` because a watcher
    /// hearing only about uploads would read the skip as a stall. Nothing
    /// stalls here: the caller closes the second descriptor's layer as
    /// `Transferred`, so the layer cursor advances on its own, and the blob
    /// this describes finished uploading moments ago in this same publish.
    /// The event drives a per-blob bar, so emitting one would reset it to
    /// "already in the registry" for bytes that had just crossed the wire —
    /// which reads as a redundant upload rather than as one blob acquiring a
    /// second name.
    ///
    /// The counters above do record it, and there the double count is the
    /// reading that is wanted: a publisher comparing two publishes wants the
    /// dump's bytes to appear as bytes it did not pay to move again.
    fn carry_again(
        &self,
        written: &WrittenLayer,
        scope: serde_json::Value,
    ) -> Result<WrittenLayer, Error> {
        let again = crate::transport::again(written, scope);

        let mut state = self.shared.locked();
        state.transfer.layers_skipped += 1;
        state.transfer.bytes_skipped += again.digests.compressed_size;
        state.pending.push(again.clone());

        Ok(again)
    }

    /// Publish the manifest, and with it the stele.
    ///
    /// The order is the whole of the safety argument:
    ///
    /// 1. every layer blob is already up — that happened in
    ///    [`RecordSink::finish`], one blob at a time;
    /// 2. the inscription goes up as the config blob;
    /// 3. the manifest is tagged with the immutable tag the profile renders for
    ///    this sequence;
    /// 4. and **only then** the moving tag moves.
    ///
    /// Step 4 is last so that a reader following `latest` never resolves to a
    /// stele whose blobs are still uploading. A push that dies in the middle
    /// leaves untagged blobs the registry will reclaim, and a `latest` that
    /// still points at the previous stele — which is a stele, and restores.
    ///
    /// **A seal that succeeds consumes the layers finished since the last
    /// one.** One transport can therefore publish several steles in turn —
    /// which is what a publisher chaining a `history` does — and a second seal
    /// of the same inscription is refused rather than republishing a manifest
    /// over layers that are no longer accounted for.
    ///
    /// **A seal that fails consumes nothing.** Every fallible step runs before
    /// the layers are taken, so a registry that answers a manifest push with a
    /// 500 leaves a transport the caller can seal again — the blobs are
    /// already up, and re-exporting a stele to recover from a transient error
    /// is not a price this owes anyone.
    fn seal(&self, profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        // Both tags before either push. Validating the moving tag after the
        // sequence manifest is already public would make a bad tag something
        // the registry finds out about half way through.
        let sequence_tag = checked_tag_for_sequence(profile, inscription.sequence)?;
        let moving_tag = profile.moving_tag().to_owned();
        validate_tag(&moving_tag)?;

        // Scoped so the guard is gone before anything touches the network.
        let (body, config) = {
            let state = self.shared.locked();
            let (manifest, config) = build_manifest(inscription, &state.pending)?;
            (manifest_bytes(&manifest)?, config)
        };

        let identity = Digest::compute(&config);
        self.shared.put_bytes(&identity, config)?;

        self.shared.push_manifest(&sequence_tag, body.clone())?;
        self.shared.push_manifest(&moving_tag, body)?;

        // Only here, with nothing fallible left, are the layers spent.
        self.shared.locked().pending.clear();

        Ok(identity)
    }

    /// Report every blob this connection uploads.
    ///
    /// One of the two implementations that override the default — the other is
    /// [`Stele`], and it shares this connection's state, so an observer
    /// attached here is also attached to whatever this registry pulls.
    fn observe(&self, observer: Observer) {
        self.shared.watch(observer);
    }
}

impl Shared {
    fn push_manifest(&self, tag: &str, body: Vec<u8>) -> Result<(), Error> {
        let reference = self.tagged(tag);

        self.runtime.block_on(self.client.push_manifest_raw(
            &reference,
            body,
            http::HeaderValue::from_static(OCI_IMAGE_MEDIA_TYPE),
        ))?;

        Ok(())
    }
}

/// A layer being written into a registry, one record at a time.
///
/// Staged into a temporary file for a reason that is not a limitation of this
/// implementation: `push_blob_stream` takes the digest up front, and a layer's
/// digest is the digest of its own compressed bytes. There is no ordering of
/// the operations in which a streaming upload learns the name first.
///
/// The staging file is unlinked at creation, so a sink dropped without
/// [`RecordSink::finish`] — an export that fails halfway with sixteen shards
/// open — leaves nothing to clean up and nothing to mistake for a blob.
pub struct RegistrySink {
    shared: Arc<Shared>,
    sequence: SeqWriter<LayerWriter<File>>,
    kind: String,
    media_type: String,
    scope: serde_json::Value,
}

impl RecordSink for RegistrySink {
    fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), Error> {
        self.sequence.write_record(record)
    }

    fn records(&self) -> u64 {
        self.sequence.count()
    }

    /// Close the layer and upload it — unless the registry has it already.
    fn finish(self) -> Result<WrittenLayer, Error> {
        let Self {
            sequence,
            shared,
            kind,
            media_type,
            scope,
        } = self;

        let count = sequence.count();
        let (mut staged, digests) = sequence.into_inner().finish()?;

        staged.flush()?;
        staged.seek(SeekFrom::Start(0))?;

        let written = WrittenLayer {
            descriptor: LayerDescriptor {
                kind,
                media_type,
                diff_id: digests.diff_id,
                records: count,
                uncompressed_size: digests.uncompressed_size,
                scope,
            },
            digests,
        };

        shared.put_layer(&written, staged)?;

        Ok(written)
    }
}

/// A stele pulled from a registry, and the read handle over it.
///
/// Everything cheap has already happened by the time this exists: the manifest
/// and the inscription are in hand, verified against each other, and the
/// `diffId`→blob map came off the manifest rather than out of a scan. What is
/// left is the layers, and those are fetched one at a time as
/// [`SteleReader::stream_layer`] is called.
pub struct Stele {
    shared: Arc<Shared>,
    reference: Reference,
    manifest: OciImageManifest,
    inscription: Inscription,
    blobs: BlobIndex,
}

/// What was resolved, and nothing about the connection that resolved it.
impl std::fmt::Debug for Stele {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stele")
            .field("reference", &self.reference.whole())
            .field("sequence", &self.inscription.sequence)
            .field("layers", &self.manifest.layers.len())
            .finish_non_exhaustive()
    }
}

impl Stele {
    /// The OCI manifest this stele was read from.
    pub fn manifest(&self) -> &OciImageManifest {
        &self.manifest
    }

    /// Compressed bytes across every layer, as the manifest reports them.
    ///
    /// The whole-stele case of [`SteleReader::compressed_size`], for a caller
    /// that wants the total without walking the inscription — a publisher
    /// reporting what a repository holds, above all. A restore wants the
    /// per-layer figure, because what it is going to fetch is a subset.
    pub fn total_compressed_size(&self) -> u64 {
        self.manifest
            .layers
            .iter()
            .map(|layer| layer.size.max(0) as u64)
            .sum()
    }

    /// The manifest's own descriptor for a layer, by identity.
    ///
    /// One lookup, shared by the read path and the size estimate, so the two
    /// cannot come to disagree about which blob holds a layer. Two steps and
    /// both are needed: the [`BlobIndex`] maps identity to a blob digest, and
    /// the manifest maps that digest to the descriptor carrying the compressed
    /// size the download is held to.
    fn layer_of(&self, index: &BlobIndex, descriptor: &LayerDescriptor) -> Option<&OciDescriptor> {
        let blob = index.blob_for(&descriptor.diff_id)?.to_string();

        self.manifest
            .layers
            .iter()
            .find(|layer| layer.digest == blob)
    }
}

impl SteleReader for Stele {
    type Blob = File;

    fn read_inscription(&self) -> Result<Inscription, Error> {
        Ok(self.inscription.clone())
    }

    fn blob_index(&self) -> Result<BlobIndex, Error> {
        Ok(self.blobs.clone())
    }

    /// Read the layer's compressed size off the manifest.
    ///
    /// Free, and the reason a registry restore can report a correct total
    /// before it fetches anything: the manifest is already in hand by the time
    /// a [`Stele`] exists. A negative size — a manifest claiming something
    /// impossible — reads as `None` rather than as a number, so it widens the
    /// estimate's stated uncertainty instead of shrinking its total.
    fn compressed_size(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<Option<u64>, Error> {
        Ok(self
            .layer_of(index, descriptor)
            .and_then(|oci| u64::try_from(oci.size).ok()))
    }

    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<File>, Error> {
        // The manifest's own descriptor, not one built here: it carries the
        // compressed size, which is the ceiling the download is held to.
        let oci = self
            .layer_of(index, descriptor)
            .ok_or_else(|| Error::LayerNotFound {
                kind: descriptor.kind.clone(),
                diff_id: descriptor.diff_id.to_string(),
            })?;

        let file = self.shared.pull_blob_file(&self.reference, oci)?;

        LayerReader::new(file, profile, descriptor, limits)
    }

    /// Report every blob this connection pulls.
    ///
    /// A restore resolves its [`Stele`] inside the driver, so this is the
    /// spelling a caller reaches when it holds the reader; attaching to the
    /// [`Registry`] the stele came from does the same thing, because both write
    /// the same connection state.
    fn observe(&self, observer: Observer) {
        self.shared.watch(observer);
    }
}

/// Build the manifest for a stele whose layers are already written.
///
/// Returns it together with the canonical inscription bytes, so the config
/// descriptor and the blob that is pushed under it cannot be computed from two
/// different encodings of the same document.
///
/// Layers are listed in **inscription order**, matched by `diffId`: the
/// canonical document fixes the order, and the manifest follows it rather than
/// the order the sinks happened to finish in. Anything that does not match both
/// ways is a refusal — see the module documentation.
///
/// Pure, so the shape of the artifact is frozen by a golden that needs no
/// network.
pub fn build_manifest(
    inscription: &Inscription,
    layers: &[WrittenLayer],
) -> Result<(OciImageManifest, Vec<u8>), Error> {
    let config = inscription.canonicalize()?;

    let mut taken = vec![false; layers.len()];
    let mut descriptors = Vec::with_capacity(inscription.layers.len());

    for described in &inscription.layers {
        // Matched by identity, and the first unclaimed one wins: two layers
        // with the same `diffId` are the same bytes, so which of them a
        // descriptor points at cannot be observed.
        let found = layers
            .iter()
            .enumerate()
            .find(|(index, layer)| !taken[*index] && layer.descriptor.diff_id == described.diff_id);

        let Some((index, layer)) = found else {
            return Err(Error::ManifestMismatch(format!(
                "the inscription describes a {:?} layer ({}) that was never written",
                described.kind, described.diff_id,
            )));
        };

        taken[index] = true;
        descriptors.push(layer_descriptor(described, layer)?);
    }

    if let Some(orphan) = taken.iter().position(|used| !used) {
        let layer = &layers[orphan].descriptor;

        return Err(Error::ManifestMismatch(format!(
            "a {:?} layer ({}) was written but the inscription does not describe it; \
             a blob nothing attests would be published",
            layer.kind, layer.diff_id,
        )));
    }

    let manifest = OciImageManifest {
        schema_version: 2,
        media_type: Some(OCI_IMAGE_MEDIA_TYPE.to_owned()),
        artifact_type: Some(ARTIFACT_TYPE.to_owned()),
        config: OciDescriptor {
            media_type: INSCRIPTION_MEDIA_TYPE.to_owned(),
            digest: Digest::compute(&config).to_string(),
            size: config.len() as i64,
            ..Default::default()
        },
        layers: descriptors,
        subject: None,
        annotations: None,
    };

    Ok((manifest, config))
}

fn layer_descriptor(
    described: &LayerDescriptor,
    written: &WrittenLayer,
) -> Result<OciDescriptor, Error> {
    let scope = String::from_utf8(canonical_json(&described.scope)?)
        .map_err(|e| Error::Canonicalization(e.to_string()))?;

    let annotations = BTreeMap::from([
        (KIND_ANNOTATION.to_owned(), described.kind.clone()),
        (DIFF_ID_ANNOTATION.to_owned(), described.diff_id.to_string()),
        (SCOPE_ANNOTATION.to_owned(), scope),
    ]);

    Ok(OciDescriptor {
        media_type: described.media_type.clone(),
        digest: written.digests.blob_digest.to_string(),
        size: written.digests.compressed_size as i64,
        annotations: Some(annotations),
        ..Default::default()
    })
}

/// The exact bytes of a manifest, canonicalized and held to the size ceiling.
///
/// RFC 8785 through the same canonicalizer the inscription uses, so this crate
/// has one answer to "what are the bytes of this JSON document" rather than two
/// that agree until they do not.
///
/// The ceiling is [`MANIFEST_SIZE_LIMIT`]. What it refuses is a stele with too
/// many layers: at roughly 350 bytes of descriptor and annotations apiece, a
/// manifest reaches 4 MiB somewhere around twelve thousand of them — nearly
/// seven times a mainnet stele's ~1,816. The comparison is in layers because
/// layers are what the ceiling counts; ADR-004's ~600 is a count of *epochs*,
/// and a mainnet stele carries three layers per epoch plus sixteen state
/// shards. It is not a limit anything is expected to reach; it is the limit
/// that turns "the registry answered 413" into a refusal that names the
/// document and the number of layers in it.
///
/// `a_manifest_past_the_size_ceiling_is_refused` in `tests/oci.rs` measures
/// those figures rather than asserting them; keep the two in step.
pub fn manifest_bytes(manifest: &OciImageManifest) -> Result<Vec<u8>, Error> {
    let body = canonical_json(&serde_json::to_value(manifest)?)?;

    if body.len() > MANIFEST_SIZE_LIMIT {
        return Err(Error::ManifestTooLarge {
            size: body.len(),
            layers: manifest.layers.len(),
        });
    }

    Ok(body)
}

/// Check that a manifest is a stele's before anything inside it is trusted.
fn check_envelope(manifest: &OciImageManifest) -> Result<(), Error> {
    match manifest.artifact_type.as_deref() {
        Some(ARTIFACT_TYPE) => {}
        Some(other) => {
            return Err(Error::ManifestMismatch(format!(
                "artifactType is {other:?}, not {ARTIFACT_TYPE:?}"
            )))
        }
        // Fail closed. A registry that strips `artifactType` — the OCI 1.1
        // field this artifact is discovered by — has published something this
        // client cannot recognise as a stele, and reading it anyway would make
        // the discovery contract advisory.
        None => {
            return Err(Error::ManifestMismatch(format!(
                "no artifactType; a stele's manifest carries {ARTIFACT_TYPE:?}"
            )))
        }
    }

    if manifest.config.media_type != INSCRIPTION_MEDIA_TYPE {
        return Err(Error::ManifestMismatch(format!(
            "config blob is {:?}, not the inscription's {INSCRIPTION_MEDIA_TYPE:?}",
            manifest.config.media_type,
        )));
    }

    Ok(())
}

/// Read the identity→blob map off a manifest, holding it against the
/// inscription.
///
/// This is the function that replaces a directory's `blob_index` scan, and the
/// reason a registry restore reads every blob once instead of twice.
///
/// Both correspondences are checked, and they are not the same check: the
/// `diffId` annotation is what the map is *built* from, and positional
/// correspondence with `inscription.layers` is what proves the manifest
/// describes this document's layers and not some other stele's. A manifest that
/// carried the right blobs in the wrong order would pass the first and fail the
/// second.
///
/// Pure, so the parsing half of the artifact is frozen by the same golden as
/// the building half.
pub fn read_manifest(
    manifest: &OciImageManifest,
    inscription: &Inscription,
) -> Result<BlobIndex, Error> {
    check_envelope(manifest)?;

    if manifest.layers.len() != inscription.layers.len() {
        return Err(Error::ManifestMismatch(format!(
            "the manifest carries {} layer(s) and the inscription describes {}",
            manifest.layers.len(),
            inscription.layers.len(),
        )));
    }

    let mut blobs = BlobIndex::default();

    for (position, (oci, described)) in manifest
        .layers
        .iter()
        .zip(inscription.layers.iter())
        .enumerate()
    {
        let annotation = oci
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(DIFF_ID_ANNOTATION))
            .ok_or_else(|| {
                Error::ManifestMismatch(format!(
                    "layer {position} carries no {DIFF_ID_ANNOTATION} annotation, \
                     so nothing says which layer it holds"
                ))
            })?;

        let diff_id = annotation.parse::<Digest>()?;

        if diff_id != described.diff_id {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is annotated {diff_id} and the inscription describes \
                 {} there",
                described.diff_id,
            )));
        }

        if oci.media_type != described.media_type {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is {:?} in the manifest and {:?} in the inscription",
                oci.media_type, described.media_type,
            )));
        }

        blobs.insert(diff_id, oci.digest.parse::<Digest>()?);
    }

    Ok(blobs)
}

/// A staged layer, as a stream of chunks.
///
/// Reads from the staging file synchronously, which is safe precisely because
/// the runtime under it is this transport's own and has nothing else to do: the
/// read is not waiting on anything the runtime is responsible for driving.
///
/// One chunk is allocated at a time and handed over, so what the upload holds
/// is [`UPLOAD_CHUNK`] and not the layer.
/// Each chunk is reported as it is handed over, which is the only resolution
/// this loop has: a `PATCH` either went out or it did not, and the client does
/// not say how much of one has reached the wire.
fn blob_stream(
    file: File,
    observer: Observer,
) -> impl Stream<Item = oci_client::errors::Result<bytes::Bytes>> {
    futures_util::stream::unfold(Some(file), move |state| {
        let observer = observer.clone();

        async move {
            let mut file = state?;
            let mut chunk = vec![0u8; UPLOAD_CHUNK];
            let mut filled = 0usize;

            while filled < chunk.len() {
                match file.read(&mut chunk[filled..]) {
                    Ok(0) => break,
                    Ok(read) => filled += read,
                    Err(e) => return Some((Err(e.into()), None)),
                }
            }

            if filled == 0 {
                return None;
            }

            chunk.truncate(filled);
            observer.emit(Event::Bytes(filled as u64));

            Some((Ok(bytes::Bytes::from(chunk)), Some(file)))
        }
    })
}

/// A synchronous writer dressed as an asynchronous one, with a ceiling.
///
/// [`oci_client::Client::pull_blob`] writes into a [`tokio::io::AsyncWrite`],
/// and everything this transport writes to is a file or a buffer. Rather than
/// take a dependency on `tokio`'s filesystem layer to get an async file that
/// would immediately be handed back to a blocking pool, the write happens where
/// it is: on the runtime's only thread, which is doing nothing else.
///
/// The ceiling is not redundant with `pull_blob`'s digest check. That check
/// fails at the *end*, after every byte has been written; the ceiling fails as
/// soon as the stream exceeds what its descriptor claims, so a blob that lies
/// about its size costs its size and not the disk.
///
/// A negative size clamps to a ceiling of zero here, and that is deliberate
/// rather than an oversight — unlike [`Registry::adopt_layer`], which refuses
/// one. The directions differ: a zero ceiling refuses every non-empty blob,
/// which is the safe answer to a manifest claiming something impossible, while
/// clamping on the way *into* a manifest would publish that impossible claim
/// forward as a number a later reader trusts.
struct Blocking<'a, W: Write> {
    inner: &'a mut W,
    written: u64,
    limit: u64,
    digest: String,
    observer: Observer,
}

impl<'a, W: Write> Blocking<'a, W> {
    fn new(inner: &'a mut W, limit: i64, digest: &str, observer: Observer) -> Self {
        Self {
            inner,
            written: 0,
            limit: limit.max(0) as u64,
            digest: digest.to_owned(),
            observer,
        }
    }
}

impl<W: Write + Unpin> tokio::io::AsyncWrite for Blocking<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        // Counted on what was *written*, never on what was offered. A `Write`
        // may accept less than it was given, and the caller then offers the
        // remainder — so counting the offer would tally those bytes twice and
        // trip a ceiling the blob never reached.
        let room = usize::try_from(this.limit - this.written).unwrap_or(usize::MAX);

        if room == 0 && !buf.is_empty() {
            return Poll::Ready(Err(std::io::Error::other(format!(
                "blob {} is larger than the {} bytes its descriptor claims",
                this.digest, this.limit,
            ))));
        }

        // Truncated to the room left, so the ceiling is exact rather than "one
        // buffer past": the byte that exceeds it is refused on the next call,
        // with nothing over-written in between.
        match this.inner.write(&buf[..buf.len().min(room)]) {
            Ok(written) => {
                this.written += written as u64;
                // On what was written, for the same reason the ceiling is: a
                // partial write's remainder is offered again, and reporting the
                // offer would count those bytes twice.
                this.observer.emit(Event::Bytes(written as u64));
                Poll::Ready(Ok(written))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.get_mut().inner.flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(self.get_mut().inner.flush())
    }
}

#[cfg(test)]
mod tests {
    use oci_client::errors::{OciDistributionError, OciEnvelope, OciError, OciErrorCode};

    use super::*;

    fn envelope(code: OciErrorCode) -> OciDistributionError {
        OciDistributionError::RegistryError {
            envelope: OciEnvelope {
                errors: vec![OciError {
                    code,
                    message: String::new(),
                    detail: serde_json::Value::Null,
                }],
            },
            url: "https://registry.invalid/v2/x/manifests/latest".to_owned(),
        }
    }

    fn server_error(code: u16) -> OciDistributionError {
        OciDistributionError::ServerError {
            code,
            url: "https://registry.invalid/v2/x/manifests/latest".to_owned(),
            message: String::new(),
        }
    }

    /// The three shapes a registry uses to say "no such manifest".
    #[test]
    fn absence_is_the_three_shapes_of_a_missing_manifest() {
        assert!(is_absent(
            &OciDistributionError::ImageManifestNotFoundError("latest".to_owned())
        ));
        assert!(is_absent(&server_error(404)));
        assert!(is_absent(&envelope(OciErrorCode::ManifestUnknown)));
        assert!(is_absent(&envelope(OciErrorCode::NameUnknown)));
    }

    fn repository(raw: &str) -> Result<Repository, Error> {
        raw.parse()
    }

    #[test]
    fn a_repository_splits_into_a_registry_and_a_path() {
        let parsed = repository("oci://ghcr.io/txpipe/dolos-snapshots/mainnet").unwrap();

        assert_eq!(parsed.registry(), "ghcr.io");
        assert_eq!(parsed.repository(), "txpipe/dolos-snapshots/mainnet");

        // A port belongs to the host, which is what makes the tag check safe to
        // run on the path alone.
        let local = repository("oci://127.0.0.1:5000/dolos").unwrap();

        assert_eq!(local.registry(), "127.0.0.1:5000");
        assert_eq!(local.repository(), "dolos");

        // And what it prints is what it parsed, so a message naming a
        // repository names the one the operator typed.
        assert_eq!(local.to_string(), "oci://127.0.0.1:5000/dolos");
    }

    #[test]
    fn ordinary_repositories_parse() {
        for raw in [
            "oci://ghcr.io/txpipe/dolos-snapshots/mainnet",
            "oci://ghcr.io/txpipe/dolos_snapshots",
            "oci://ghcr.io/txpipe/dolos.snapshots",
            "oci://localhost:5000/dolos/mainnet",
            "oci://127.0.0.1:5000/dolos",
        ] {
            assert!(repository(raw).is_ok(), "{raw}");
        }
    }

    #[test]
    fn a_name_that_cannot_address_a_repository_is_refused() {
        for raw in [
            "ghcr.io/txpipe/dolos",                  // no scheme
            "https://ghcr.io/txpipe/dolos",          // the wrong scheme
            "oci://ghcr.io",                         // no repository path
            "oci://ghcr.io/",                        // still no repository path
            "oci:///txpipe/dolos",                   // no host
            "oci://ghcr.io/txpipe/dolos/",           // a trailing slash
            "oci://ghcr.io/txpipe/dolos:v1",         // a tag names a stele
            "oci://ghcr.io/txpipe/dolos@sha256:abc", // and so does a digest
            "",
        ] {
            assert!(repository(raw).is_err(), "{raw:?}");
        }
    }

    /// Names the distribution grammar refuses, which a split on `/` alone
    /// cannot see.
    ///
    /// Each of these reaches the registry as part of the request path, so
    /// accepting them buys an opaque error from someone else's server at the
    /// end of a publish rather than a sentence at the start of one.
    #[test]
    fn a_path_outside_the_grammar_is_refused() {
        for raw in [
            "oci://ghcr.io//txpipe/dolos",      // an empty component
            "oci://ghcr.io/txpipe//dolos",      // an empty component, inside
            "oci://ghcr.io/TxPipe/dolos",       // uppercase; names are lowercase
            "oci://ghcr.io/txpipe/dolos?x=1",   // a query
            "oci://ghcr.io/txpipe/dolos#frag",  // a fragment
            "oci://ghcr.io/txpipe/dolos snaps", // whitespace
            "oci://ghcr.io/txpipe/-dolos",      // a component opening on a separator
        ] {
            assert!(repository(raw).is_err(), "{raw:?}");
        }
    }

    /// The refusal a hand-written splitter cannot make.
    ///
    /// `Reference`'s parser treats a first component with no dot and no colon
    /// as part of the repository rather than as a host, so `dolos/mainnet`
    /// resolves to `docker.io/dolos/mainnet`. An operator who wrote
    /// `oci://dolos/mainnet` meant a registry called `dolos`, and publishing to
    /// Docker Hub instead is the one outcome worse than refusing.
    #[test]
    fn a_host_the_parser_would_have_invented_is_refused() {
        let err = repository("oci://dolos/mainnet").unwrap_err();

        let message = err.to_string();
        assert!(message.contains("docker.io"), "{message}");
        assert!(message.contains("dolos"), "{message}");

        // `localhost` is the one bare name the grammar does treat as a host, so
        // it must still work — the check is against inference, not against
        // hosts that happen to have no dot.
        assert_eq!(
            repository("oci://localhost:5000/dolos").unwrap().registry(),
            "localhost:5000"
        );
    }

    /// The half that carries the weight: a registry that failed is not a
    /// registry that is empty.
    ///
    /// [`Registry::latest`] turns absence into `None`, and a publisher reads
    /// `None` as "nothing to chain to" and starts a fresh history. So a
    /// timeout, a 500 or an expired token widening into absence would silently
    /// restart the attestation chain — which is the outcome an inscription's
    /// `history` exists to prevent, arrived at without anything looking wrong.
    #[test]
    fn a_failed_request_is_never_absence() {
        assert!(!is_absent(&server_error(500)));
        assert!(!is_absent(&server_error(503)));
        assert!(!is_absent(&envelope(OciErrorCode::Unauthorized)));
        assert!(!is_absent(&envelope(OciErrorCode::Denied)));
        assert!(!is_absent(&OciDistributionError::UnauthorizedError {
            url: "https://registry.invalid/v2/x/manifests/latest".to_owned(),
        }));
        assert!(!is_absent(&OciDistributionError::GenericError(None)));
    }

    /// A password never reaches a log through this type.
    ///
    /// [`Options`] derives `Debug` and error context is printed freely, so this
    /// redaction is what stands between a publisher's credentials and the first
    /// backtrace anybody pastes into an issue.
    #[test]
    fn credentials_are_redacted_in_debug_output() {
        let basic = Auth::Basic {
            user: "reader".to_owned(),
            password: "hunter2".to_owned(),
        };

        let printed = format!("{basic:?}");
        assert!(printed.contains("reader"), "{printed}");
        assert!(!printed.contains("hunter2"), "{printed}");

        let printed = format!("{:?}", Auth::Bearer("ghp_x".to_owned()));
        assert!(!printed.contains("ghp_x"), "{printed}");

        // And through the structure a caller actually holds, which is where it
        // would leak from.
        let printed = format!(
            "{:?}",
            Options {
                auth: basic,
                ..Default::default()
            }
        );
        assert!(!printed.contains("hunter2"), "{printed}");
    }

    /// A staging directory that cannot be used says which one, and why.
    ///
    /// The registry suite covers the same ground through a real publish, but
    /// it needs a container and is `#[ignore]`d for it; this is the claim
    /// under plain `cargo test`. The path names an existing regular file,
    /// which `create_dir_all` cannot turn into a directory for anybody, `root`
    /// included — so it is the one unusable directory that reproduces on every
    /// platform and under every user the suite might run as.
    #[test]
    fn an_unusable_staging_directory_names_itself() {
        let root = tempfile::tempdir().unwrap();
        let occupied = root.path().join("not-a-directory");
        std::fs::write(&occupied, b"").unwrap();

        let error = scratch_in(Some(&occupied)).expect_err("staged in a regular file");

        assert!(
            matches!(&error, Error::Scratch { dir, .. } if dir == &occupied),
            "fell through to the catch-all: {error:?}",
        );

        // The two halves the old `io error: File exists (os error 17)` had
        // neither of: which directory, and that it was the staging one.
        let message = error.to_string();
        assert!(
            message.contains(&occupied.display().to_string()),
            "{message}",
        );
        assert!(message.contains("staging directory"), "{message}");

        // And what the operating system said, exactly once, one line down the
        // chain rather than repeated into the message above it.
        let source = std::error::Error::source(&error).expect("no cause to render");
        assert!(!message.contains(&source.to_string()), "{message}");
    }

    /// The unnamed case keeps the catch-all, and still works.
    #[test]
    fn an_unnamed_staging_directory_still_stages() {
        scratch_in(None).expect("the platform temporary directory is unusable");
    }
}
