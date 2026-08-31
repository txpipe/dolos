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
//! has to write the same map down for itself in
//! [`crate::dir::BLOB_INDEX_FILE`] — or, lacking one, reconstruct by brute
//! force what a manifest states.
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
//! Both push paths need the blob's digest *up front*, and a layer's digest is
//! only known once its last record has been written. So a layer is staged into
//! a temporary file exactly as a directory stages one, and then sent up from
//! it. A pulled layer is streamed to a temporary file and read back
//! synchronously.
//!
//! Neither direction ever holds a whole stele. The staging files are unlinked
//! at creation, so an abandoned push or a failed pull leaves nothing behind and
//! needs no cleanup path of its own.
//!
//! What the *upload* holds is decided by [`Options::monolithic_max`]. Above it,
//! a layer is streamed and one [`UPLOAD_CHUNK`] is resident at a time. At or
//! below it, the layer goes up as one request — a `POST` and a `PUT` carrying
//! the whole body — and is resident in full while it does, because the client
//! takes the body as bytes and there is no ordering in which it does not.
//!
//! That is bought deliberately: a `PATCH` costs the registry about three
//! seconds whatever it carries, so a publish's wall clock is its request count,
//! and 79 of mainnet's 81 layers fit under the threshold. What keeps the price
//! bounded is [`Options::upload_memory`] — a budget in *bytes*, spent by the
//! layers actually in flight rather than inferred from how many of them there
//! are. See [`Shared::resident`].
//!
//! ## The async boundary, and the one rule it comes with
//!
//! `oci-client` is async and this crate is not: `export` and `restore` are
//! synchronous iterator code driving fallible store iterators, and threading a
//! runtime through them would change every profile's shape for the benefit of
//! one transport. So the transport owns **one runtime** and enters it with
//! `block_on` at each call — the idiom `dolos bootstrap mithril` already uses.
//!
//! **A [`Registry`] must never be used from inside an async context, and must
//! never be dropped inside one.** `Runtime::block_on` panics when called from a
//! runtime thread, and dropping a runtime from inside one panics too. Every
//! caller today is a synchronous CLI path, which is what makes this safe; a
//! caller that is not is a design question, and the answer is not a second
//! runtime.
//!
//! Several synchronous caller threads at once are fine, and the publish path
//! uses them: a profile driver may open and finish sinks from a pool of its
//! own producer threads. Everything those calls share — the push state, the
//! in-flight list, the permits — is behind its own lock, and `block_on` from
//! many non-runtime threads is exactly what a runtime is for.
//!
//! ## Why the publish path is concurrent, and where it joins again
//!
//! A publish is not one transfer; it is a few hundred small ones. A stele's
//! layers are cut at record-type and epoch granularity, so a mainnet publish
//! moves tens of new blobs of half a megabyte each and carries hundreds of
//! older ones forward — and every one of those, new or carried, costs at least
//! one round trip to a registry that may be an ocean away. Run in sequence, the
//! path spends nearly all of its wall clock waiting on a socket with the CPU
//! and the link both idle, and the carried-forward half makes it *worse every
//! epoch*: the stele gains layers, so the publish gains round trips, so the
//! cycle time grows linearly with the history behind it.
//!
//! Nothing about that is a bandwidth problem, so the answer is not bigger
//! layers — the cut geometry is the profile's, and it is deliberate. The answer
//! is to stop doing one round trip at a time. Every layer's round trips are
//! independent of every other layer's: a blob is addressed by its own content,
//! and no blob's upload observes another's. So they are **deferred onto the
//! runtime and run concurrently**, bounded by [`Options::concurrency`], and the
//! caller's thread goes back to reading the store rather than waiting on a
//! `PATCH`.
//!
//! What is *not* independent is the manifest, and that is the whole of the
//! safety argument this concurrency has to preserve:
//!
//! > **A manifest must never name a blob the registry has not committed.**
//!
//! So [`SteleWriter::seal`] is the join. Every deferred round trip is awaited
//! there — before the manifest is built, before the config blob goes up, before
//! either tag is written — and the first failure among them fails the seal, in
//! the state a failed seal has always left behind: layers unspent, nothing
//! tagged, and the caller free to seal again. A publish that dies mid-flight
//! leaves untagged blobs the registry reclaims, exactly as a serial one did.
//!
//! The bound is a permit taken *before* the staged layer is handed over rather
//! than inside the task, so it is also what keeps the scratch directory from
//! filling: at most [`Options::concurrency`] staged layers exist at once,
//! whatever order the sinks finish in.
//!
//! It is not the bound on memory, and reading it as one is the mistake this
//! paragraph exists to prevent. A layer count says nothing about bytes when the
//! layers differ in size by three orders of magnitude — mainnet's median layer
//! is 0.41 MB and its largest is 231 MB — and the single-request path spends
//! bytes. So there is a second permit, taken in the task where the size is
//! finally known, one per resident byte, against [`Options::upload_memory`].
//!
//! ## A round trip nobody answered is made again
//!
//! Concurrency raised the number of round trips in flight; it did nothing about
//! the ones that fail for no reason and would have worked a second later. Over
//! eleven hours of the mainnet backfill the registry answered a create-session
//! `POST` with a bare `500` eight times, each lasting the milliseconds it took
//! to ask again — and each one cost the whole epoch, because a publish that
//! could not seal took the driver down with it, and the driver's recovery is to
//! restore its stores and replay the epoch it lost.
//!
//! That is the most expensive recovery in the system, bought for the cheapest
//! failure there is. So a round trip is **attempted [`Options::attempts`]
//! times**, with a doubling wait between them, and the failure the caller keeps
//! is the last attempt's:
//!
//! - **a `5xx`** — the registry answered, and what it said was about itself
//!   rather than about the request. Asking again is the whole remedy;
//! - **no answer at all** — a connection refused, a request that timed out, a
//!   socket that went away mid-body.
//!
//! Nothing else. A `4xx` is the registry saying something true about *this*
//! request — the credential, the digest, the name — and repeating it four times
//! only makes the diagnosis take longer to arrive. `429` is excluded on purpose
//! and not by omission: a registry rationing this publisher is a fact its
//! operator has to see, and a client that absorbed it would report the ration
//! as slowness.
//!
//! This is safe to do at every seam because every one of them is idempotent by
//! construction. A blob is addressed by the digest of its own content, so an
//! upload that half-happened and an upload that fully happened both converge on
//! the same blob when it is sent again; a `HEAD` and a manifest `GET` are
//! reads; and the manifest `PUT` writes bytes that are a pure function of the
//! stele. The one thing a retry needs and the serial path did not is the
//! staging file back at its first byte, which is why it is rewound per attempt
//! rather than consumed once.
//!
//! Every retry is announced through [`Event::Retry`], because a transport that
//! silently absorbed the failure class would have hidden the measurement that
//! motivated absorbing it.
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
    time::Duration,
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
    digest::{read_uninterrupted, LayerDigests, LayerWriter},
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

/// How much of a *streamed* layer is held in memory on the way up.
///
/// The layers too large for [`DEFAULT_MONOLITHIC_MAX`], and nothing else: a
/// layer that fits goes up as one request and is resident in full while it
/// does. What is left here is one chunk at a time, allocated and handed to the
/// client, which sends it as one `PATCH` — and a `PATCH` costs the registry
/// about three seconds *whatever it carries*, flat across an eightfold change
/// in chunk size, because what it is spent on is the upload state the worker
/// round-trips through its object store rather than the bytes. So the chunk
/// count is the publish's wall clock, and this constant is what sets it:
/// mainnet's largest layer is 231 MB, which at 1 MiB was 221 round trips and
/// eleven minutes of a fifteen-minute publish.
///
/// Concurrency is not the alternative. A `PATCH` answers with a `Location`
/// carrying the session's state hash and the next one is refused unless it
/// presents the current value, so a blob is one serial chain —
/// [`Options::concurrency`] bounds layers in flight, never chunks within a
/// layer.
///
/// 4 MiB and not more because `oci-client` re-splits whatever this stream hands
/// it at its own `PUSH_CHUNK_MAX_SIZE`, which is 4 MiB and has no setter.
/// Anything larger here would go out as 4 MiB anyway, having cost the memory.
const UPLOAD_CHUNK: usize = 4 * 1024 * 1024;

/// The largest layer this transport will push as one request.
///
/// A `POST` followed by a `PUT` carrying the whole body skips the chunked
/// session entirely — no `PATCH` chain, no upload state round-tripped through
/// the registry's object store, no recombination — and measured against the
/// live registry it moves **3.29 MB/s against 0.27**. It also covers most of a
/// publish: 79 of mainnet's 81 layers are under this number, and the median
/// layer is 0.41 MB.
///
/// 100 MB because that is what this registry advertises as
/// `OCI-Chunk-Max-Length`, decimal as the header is
/// (`registry/vendor/src/chunk.ts`, `MAXIMUM_CHUNK_UPLOAD_SIZE`). It is a
/// property of *that* registry and not of registries in general, which is why
/// it is [`Options::monolithic_max`] rather than a literal in the push path —
/// but it cannot be read from the wire: the header rides on the upload
/// session's response and `oci-client` extracts only the `Location` from it
/// (`client.rs`, `extract_location_header`), so nothing this crate calls ever
/// sees it. A default a caller can override is the whole of the honesty
/// available here.
pub const DEFAULT_MONOLITHIC_MAX: u64 = 1000 * 1000 * 100;

/// How many bytes of layer a publish may hold in memory at once.
///
/// One gibibyte, and it exists because a monolithic push turns
/// [`Options::concurrency`] into a claim on *memory* and not just on the
/// scratch directory. A layer count is the wrong unit for that: thirty-two
/// permits against a 100 MB threshold is 3.2 GB worst case, on a publisher pod
/// requesting 12 GiB and already sitting at seven.
///
/// So the resident bytes are bounded directly rather than inferred from a
/// layer count — see [`Shared::resident`]. At the default threshold this is ten
/// large layers in flight at once, whatever the concurrency is set to, while
/// the median 0.41 MB layer costs a permit it will never wait for.
pub const DEFAULT_UPLOAD_MEMORY: u64 = 1024 * 1024 * 1024;

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

/// How many of a publish's layer round trips run at once.
///
/// Eight, and the number is a floor on the registry's side of the trade rather
/// than a ceiling on this one's: the round trips are latency, not bandwidth, so
/// the transport would happily run more, and what stops it is that a stele's
/// blobs go to *one* repository behind one origin. A publisher that opens
/// thirty-two upload sessions at once against a registry sized for a container
/// image is a publisher that finds the registry's limits rather than its own.
///
/// Eight moves the mainnet publish path off its serial floor by most of an
/// order of magnitude while staying inside what a modest origin answers without
/// complaint. [`Options::concurrency`] is there for an operator who has
/// measured their own.
pub const DEFAULT_CONCURRENCY: usize = 8;

/// How many times one of a publish's round trips is made before the failure is
/// the caller's.
///
/// Four, and the number is read off the failure it absorbs rather than chosen:
/// the registry's transient `500`s arrive alone and clear in the time it takes
/// to ask again, so the first retry is the one that does the work and the rest
/// are there for the case where it is a second longer than that. Bounded for
/// the same reason [`crate::Error::LayerNotWritten`] exists — a registry that
/// is *actually* refusing has to keep refusing, out loud, while whoever
/// launched the publish is still watching.
pub const DEFAULT_ATTEMPTS: u32 = 4;

/// How long the transport waits after the first failed attempt; each later wait
/// doubles it.
///
/// Three waits of 500ms, 1s and 2s put the ceiling at three and a half seconds
/// of patience per round trip — under the cost of one lost epoch by four orders
/// of magnitude, and small enough that a publish absorbing a handful of them a
/// night does not show up as a slower publish.
const RETRY_DELAY: Duration = Duration::from_millis(500);

/// How to reach a registry.
#[derive(Debug, Clone)]
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

    /// How many layer round trips a publish runs at once.
    ///
    /// Defaults to [`DEFAULT_CONCURRENCY`]. `1` restores the strictly serial
    /// path — an escape hatch for a registry that answers concurrency badly,
    /// not a mode anything should want — and `0` is read as `1` rather than
    /// refused, because a transport that could move nothing is not a
    /// configuration anybody means.
    ///
    /// It bounds the staging directory as well as the wire: see the module
    /// documentation. It does not bound memory — [`Options::upload_memory`]
    /// does, and in the unit that one is spent in.
    pub concurrency: usize,

    /// Re-prove that the registry still holds a blob being adopted out of a
    /// manifest this transport pulled.
    ///
    /// Off, and that is the plain reading of the distribution specification
    /// rather than an optimism: a blob referenced by a manifest under a live
    /// tag is not garbage, and a registry that reclaims one has broken the
    /// contract that makes *the stele the manifest came from* restorable —
    /// which the `HEAD` would not have saved either. Paying a round trip per
    /// carried layer to re-establish that is what made the publish path's cost
    /// grow with the history behind it, for a check whose failure means the
    /// repository is already unusable.
    ///
    /// On, [`Registry::adopt_layer`] proves each blob before the manifest names
    /// it, concurrently with everything else the publish is doing — so the
    /// check costs latency it can hide rather than latency it serializes. For
    /// an operator publishing into a registry whose retention they do not
    /// trust.
    pub verify_adopted: bool,

    /// How many times a round trip is made before its failure is the caller's.
    ///
    /// Defaults to [`DEFAULT_ATTEMPTS`]. `0` and `1` both mean one attempt and
    /// no retry — `0` is read as `1` rather than refused, for the reason
    /// [`Options::concurrency`] reads it that way: a transport that would make
    /// no attempt at all is not a configuration anybody means.
    ///
    /// Only the failures the module documentation lists are retried; a
    /// registry's refusal of *this* request is never one of them, so raising
    /// this does not slow down a publish that was going to fail anyway.
    pub attempts: u32,

    /// The largest layer to push as one request rather than as a `PATCH` chain.
    ///
    /// Defaults to [`DEFAULT_MONOLITHIC_MAX`], which is what the registry this
    /// was measured against advertises. A registry that accepts less is a
    /// registry an operator has to say so about, because the advertisement is
    /// not reachable from here — see the constant.
    ///
    /// Clamped down to [`Options::upload_memory`] when it is larger, so that a
    /// layer at the threshold always fits the budget that admits it. `0` is
    /// read as "never", not as "layers of no bytes": it streams everything,
    /// which is the escape hatch for a registry that answers a monolithic
    /// `PUT` badly.
    pub monolithic_max: u64,

    /// How many bytes of layer this transport may hold in memory at once.
    ///
    /// Defaults to [`DEFAULT_UPLOAD_MEMORY`]. It bounds the single-request path
    /// and nothing else — a streamed layer holds one [`UPLOAD_CHUNK`] whatever
    /// this says — and it is a budget rather than a limit on any one layer:
    /// several small layers share it, and a layer larger than the whole budget
    /// cannot exist because the threshold is clamped to it.
    pub upload_memory: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            insecure: false,
            scratch_dir: None,
            auth: Auth::default(),
            concurrency: DEFAULT_CONCURRENCY,
            verify_adopted: false,
            attempts: DEFAULT_ATTEMPTS,
            monolithic_max: DEFAULT_MONOLITHIC_MAX,
            upload_memory: DEFAULT_UPLOAD_MEMORY,
        }
    }
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
    /// Behind its own [`Arc`] rather than inside this one, because a deferred
    /// layer round trip has to reach it and must **not** reach the runtime: the
    /// task would then hold the thing that is driving it, and the last handle
    /// dropping inside a worker thread would drop a runtime from inside itself,
    /// which panics. Every deferred task below is built out of cheap clones —
    /// the client, the reference, this handle, the observer — and never out of
    /// a `Shared`.
    state: Arc<Mutex<PushState>>,
    /// How many layer round trips may be outstanding at once, and with them how
    /// many staged layers may exist at once. A permit is taken on the caller's
    /// thread before the staging file is handed over and released when the
    /// round trip is done. See [`Options::concurrency`].
    ///
    /// Layers, not bytes: what this bounds is the scratch directory. Memory is
    /// `resident`, below.
    permits: Arc<tokio::sync::Semaphore>,
    /// The deferred layer round trips, waiting to be joined by
    /// [`SteleWriter::seal`].
    ///
    /// Not in [`PushState`], for the reason `state` is not in `Shared`: the
    /// tasks reach the state and must never reach the handles that own them.
    inflight: Mutex<Vec<tokio::task::JoinHandle<Result<(), Error>>>>,
    /// Who is watching this connection, in either direction.
    ///
    /// Beside the push state rather than inside it, because a [`Stele`] shares
    /// this value and only ever reads: attaching an observer to the
    /// [`Registry`] is what makes the pull it resolves report too, which is the
    /// property a restore depends on — the reader is created inside the driver
    /// and there is no other moment to wire it.
    observer: Mutex<Observer>,
    /// [`Options::verify_adopted`], as it was given.
    verify_adopted: bool,
    /// [`Options::concurrency`], as it was resolved — the permit count, kept
    /// beside the semaphore because the semaphore's own count is whatever is
    /// free at the moment it is asked.
    concurrency: usize,
    /// [`Options::attempts`], as it was resolved.
    attempts: u32,
    /// The resident-byte budget, one permit per byte.
    ///
    /// A second bound beside `permits`, and a different unit on purpose. That
    /// one counts layers and bounds the scratch directory; this one counts
    /// bytes and bounds *memory*, which is what the single-request path spends
    /// and what a layer count cannot express — the layers differ in size by
    /// three orders of magnitude.
    ///
    /// Taken inside the deferred task rather than on the caller's thread,
    /// because that is where a layer's size is known: the permit in
    /// [`RecordSink::finish`] is taken before the layer is closed, and its
    /// size does not exist yet. Held for the upload and released before the
    /// backoff between attempts, so a publish waiting on a failing registry
    /// holds no bytes at all.
    ///
    /// Cannot deadlock: `monolithic_max` is clamped to the budget, so any
    /// single acquisition can be satisfied by an empty semaphore, and
    /// `tokio`'s is fair — a large waiter is not overtaken by the small ones
    /// behind it.
    resident: Arc<tokio::sync::Semaphore>,
    /// [`Options::monolithic_max`], as it was resolved against
    /// [`Options::upload_memory`].
    monolithic_max: u64,
}

#[derive(Default)]
struct PushState {
    /// Layers finished since the last [`SteleWriter::seal`], in finish order.
    pending: Vec<WrittenLayer>,
    transfer: Transfer,
    /// The first deferred round trip that failed, rendered.
    ///
    /// Sticky, and that is the point. A failed [`Shared::join_layers`] empties
    /// the handles it awaited, so without this a second seal would find nothing
    /// in flight, agree that every layer was up, and publish a manifest naming
    /// a blob that never landed — the one document this transport must never
    /// write. See [`Error::LayerNotWritten`].
    failed: Option<String>,
}

/// One layer's share of [`Options::concurrency`], held for as long as its round
/// trips are outstanding.
///
/// Named because it is passed hand to hand — taken on the caller's thread by
/// whoever is about to defer, moved into the task, and dropped when the task
/// ends — and a bare `OwnedSemaphorePermit` in three signatures says nothing
/// about which of those it is.
type Permit = tokio::sync::OwnedSemaphorePermit;

/// The push state, for a deferred task that holds the state and not the
/// transport.
///
/// A poisoned lock means a push panicked while holding it. The counters are
/// plain integers and the pending list is append-only, so what is behind the
/// lock is still coherent; refusing to look at it would turn one failed push
/// into a transport nobody can use.
fn lock(state: &Mutex<PushState>) -> std::sync::MutexGuard<'_, PushState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    /// Builds the runtime the whole transport runs on, and stores the
    /// credentials [`Options::auth`] carries so they never have to be threaded
    /// through a profile's call stack.
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

        // `use_monolithic_push` is what makes `push_blob` send a `POST` and
        // then one `PUT` carrying the whole body instead of opening a chunked
        // session. It reaches two paths: the layers under `monolithic_max`
        // below, which is the point, and `Shared::put_bytes`, which was already
        // calling `push_blob` for the inscription — a document of a few
        // kilobytes that now costs one round trip fewer.
        //
        // The flag also removes `push_blob`'s fallback: without it a chunked
        // push that trips a spec violation retries monolithically, and with it
        // there is nothing to fall back *from*. That is the trade this whole
        // change is, and the single-request path is the one measured against
        // the registry this publishes to.
        let client = Client::new(ClientConfig {
            protocol,
            use_monolithic_push: true,
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

        // One worker per permit, and the two numbers are one decision. A
        // deferred upload reads its staged layer with a *blocking* file read —
        // see `blob_stream`, where that is deliberate — so a worker driving one
        // upload is unavailable to another for the length of a read. Sizing the
        // pool to the bound is what keeps that from turning a concurrency of
        // eight into a concurrency of one on a slow volume.
        let concurrency = options.concurrency.max(1);

        // The budget first, then the threshold against it: a layer at the
        // threshold has to be admissible on an empty semaphore, or the task
        // holding a layer larger than the whole budget would wait forever.
        let upload_memory = options
            .upload_memory
            .min(tokio::sync::Semaphore::MAX_PERMITS as u64);
        // `u32` because that is what one `acquire_many` can ask for; no layer
        // this format cuts comes near it, and a threshold that did would be
        // asking for four gibibytes of one blob in memory.
        let monolithic_max = options
            .monolithic_max
            .min(upload_memory)
            .min(u32::MAX as u64);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(concurrency)
            .thread_name("stelae-oci")
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
                state: Arc::new(Mutex::new(PushState::default())),
                permits: Arc::new(tokio::sync::Semaphore::new(concurrency)),
                inflight: Mutex::new(Vec::new()),
                observer: Mutex::new(Observer::silent()),
                verify_adopted: options.verify_adopted,
                concurrency,
                attempts: options.attempts.max(1),
                resident: Arc::new(tokio::sync::Semaphore::new(upload_memory as usize)),
                monolithic_max,
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

    /// [`Options::concurrency`], as it was resolved.
    ///
    /// Asked by a caller sizing the volume this transport stages on, for the
    /// reason [`Registry::scratch_dir`] is asked for the directory: the number
    /// that bounds how many layers are staged at once is the transport's, and a
    /// caller re-deriving it from its own configuration is a second copy of a
    /// number that has to stay in step.
    pub fn concurrency(&self) -> usize {
        self.shared.concurrency
    }

    /// What has been pushed through this transport since it was opened, or
    /// since the last [`Registry::take_transfer`].
    ///
    /// **What has *finished*.** Layer round trips are deferred and joined at
    /// the seal, so a publish still in flight is a publish still counting, and
    /// the moment these numbers are the whole story is after
    /// [`SteleWriter::seal`] returns. Asked earlier they are a progress
    /// reading; asked there they are the cost of the stele. A caller that wants
    /// the second one does not have to do anything to get it — a publish ends
    /// at a seal — and this does not block to manufacture it, because a
    /// counter that waited for the network would be a strange thing for a
    /// progress renderer to call.
    pub fn transfer(&self) -> Transfer {
        self.shared.locked().transfer
    }

    /// The same numbers, and reset — so a publisher pushing several steles
    /// through one transport reads each one's cost rather than a running total.
    ///
    /// Read it after the seal, for the reason [`Registry::transfer`] gives, and
    /// with one more of its own: this one clears what it read, so a call made
    /// while round trips are still in flight does not merely see a partial
    /// figure, it takes the figure away from the seal that was going to
    /// complete it.
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

        let (manifest, _digest) = self.shared.retrying(|| {
            Ok(self.shared.runtime.block_on(
                self.shared
                    .client
                    .pull_image_manifest(&reference, &self.shared.auth),
            )?)
        })?;

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
    /// them, which is the cost being avoided.
    ///
    /// ## What is not checked, and why that is the default
    ///
    /// `source` is a stele this transport *pulled*: its manifest is live in
    /// this repository under a tag, and it names this blob. A registry is not
    /// permitted to reclaim a blob in that position, and one that does has
    /// already broken the stele the descriptor came from — a `HEAD` here would
    /// find the damage a publish too late and could not have prevented it.
    ///
    /// Re-establishing it per carried layer per publish is what made this path
    /// cost a round trip for every layer of history behind the stele, and made
    /// each publish slower than the one before it for a reason that had nothing
    /// to do with what it was publishing. So it is not paid by default.
    /// [`Options::verify_adopted`] restores the proof for an operator who wants
    /// it, and pays for it concurrently rather than in sequence: the `HEAD`
    /// still lands before the manifest names the blob, which is the only
    /// ordering that was ever load-bearing.
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

        // A manifest naming a blob the registry no longer has is a refusal and
        // not a miss: the stele that named it is published, and something has
        // reclaimed underneath it. See [`Registry::adopt_carried`] for the case
        // where the same absence is merely a rebuild — and `Options` for why
        // this transport does not go looking for it by default.
        if self.shared.verify_adopted {
            let permit = self.shared.permit();

            self.shared.prove_blob(&adopted, permit);
        }

        let mut state = self.shared.locked();
        state.transfer.layers_reused += 1;
        state.transfer.bytes_reused += adopted.digests.compressed_size;
        state.pending.push(adopted);

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

/// Whether a failure is one that asking again can fix.
///
/// The counterpart of [`is_absent`], and narrow for the same reason that one
/// is: a classification that guesses wide turns a registry's considered refusal
/// into four of them spread over three and a half seconds, and the caller reads
/// the last copy. Two shapes qualify.
///
/// **A `5xx`.** The registry answered, and what it said was about itself. This
/// is the measured class — the create-session `POST` that returns
/// `500 INTERNAL_ERROR` and succeeds on the next ask.
///
/// **No answer at all.** A connection refused, a request that timed out, a
/// socket closed mid-body. `oci-client` reports these through `reqwest`
/// untouched, and [`oci_client::Client::blob_exists`] reports a `5xx` that way
/// too — it asks `reqwest` for the status rather than mapping it — so both
/// shapes have to be read out of the same variant.
///
/// Everything else propagates on the first attempt:
///
/// - **a `4xx`** is the registry saying something true about *this* request.
///   The credential is wrong, the digest does not match what arrived, the
///   repository is not there. Repetition does not change any of those, it only
///   delays the report;
/// - **`429` in particular**, and that is a decision rather than an oversight.
///   A registry rationing this publisher is a fact its operator needs, and a
///   client that waited it out would deliver the ration as unexplained
///   slowness;
/// - **anything local** — a staging file that would not read, a manifest that
///   would not parse. Nothing on the far side is involved and nothing about
///   waiting helps.
fn is_transient(error: &Error) -> bool {
    use oci_client::errors::OciDistributionError as E;

    let Error::Registry(error) = error else {
        return false;
    };

    match error {
        E::ServerError { code, .. } => (500..600).contains(code),
        E::RequestError(source) => match source.status() {
            Some(status) => status.is_server_error(),
            None => source.is_timeout() || source.is_connect() || source.is_request(),
        },
        _ => false,
    }
}

/// What a bounded retry keeps between attempts: how many are left, and how long
/// the next wait is.
///
/// A value rather than a loop because there are two loops — one on the caller's
/// thread around a `block_on`, one inside a deferred task around an `await` —
/// and the decision they share is this and not the sleeping. Splitting it here
/// is what keeps the policy in one place while each loop waits the way its own
/// thread has to.
struct Backoff {
    attempted: u32,
    attempts: u32,
    delay: Duration,
}

impl Backoff {
    fn new(attempts: u32, delay: Duration) -> Self {
        Self {
            attempted: 0,
            attempts: attempts.max(1),
            delay,
        }
    }

    /// How long to wait before making the round trip again, or `None` if this
    /// failure is the caller's.
    ///
    /// Announces the retry it is about to allow, before the wait rather than
    /// after it, so a watcher hears about a registry misbehaving while it is
    /// still misbehaving.
    fn wait_after(&mut self, error: &Error, observer: &Observer) -> Option<Duration> {
        self.attempted += 1;

        let remaining = self.attempts - self.attempted;

        if remaining == 0 || !is_transient(error) {
            return None;
        }

        observer.emit(Event::Retry {
            attempt: self.attempted,
            remaining,
            reason: &error.to_string(),
        });

        let waiting = self.delay;
        self.delay = self.delay.saturating_mul(2);

        Some(waiting)
    }
}

/// Run a round trip on the caller's thread, making it again while
/// [`is_transient`] says it is worth it.
///
/// `op` does its own `block_on` and is run from a thread that is not the
/// transport's runtime — the rule the module documentation states — so the wait
/// is a plain thread sleep. `op` is called again from scratch, which is what
/// puts any resetting a second attempt needs inside it rather than around it.
fn retrying<T>(
    attempts: u32,
    observer: &Observer,
    mut op: impl FnMut() -> Result<T, Error>,
) -> Result<T, Error> {
    let mut backoff = Backoff::new(attempts, RETRY_DELAY);

    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(error) => match backoff.wait_after(&error, observer) {
                Some(waiting) => std::thread::sleep(waiting),
                None => return Err(error),
            },
        }
    }
}

/// [`retrying`], for a round trip already inside the runtime.
///
/// The deferred layer tasks, where the wait must yield the worker rather than
/// hold it: a thread sleeping here is one of [`Options::concurrency`] threads,
/// and parking it would stall an upload that has nothing wrong with it.
///
/// `op` returns a future that owns everything it touches, for the reason
/// [`Shared::defer`] gives — and here for a second one: a future that borrowed
/// the closure could not be built twice.
async fn retrying_async<T, F, Fut>(
    attempts: u32,
    observer: &Observer,
    mut op: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut backoff = Backoff::new(attempts, RETRY_DELAY);

    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => match backoff.wait_after(&error, observer) {
                Some(waiting) => tokio::time::sleep(waiting).await,
                None => return Err(error),
            },
        }
    }
}

/// A staging file, back at its first byte and ready to be streamed.
///
/// A `dup` rather than the file itself, because a stream consumes what it is
/// given and a second attempt has to read the same bytes again. The two
/// descriptors share an offset, so the seek here is what rewinds *the* file
/// however many handles are outstanding — which is sound because only one
/// attempt of one layer ever reads it at a time.
fn rewound(staged: &File) -> Result<File, Error> {
    let mut again = staged.try_clone()?;

    again.seek(SeekFrom::Start(0))?;

    Ok(again)
}

/// A staged layer, whole, for the single-request path.
///
/// Read on the runtime's own worker thread and synchronously, for the reason
/// [`blob_stream`] reads that way: the file is local, the runtime is this
/// transport's own, and it is sized one worker per permit so a thread inside a
/// read cannot starve another upload.
///
/// Read fresh per attempt rather than held across the backoff — the caller has
/// already taken the resident-byte permit that admits it, and holding the bytes
/// through a wait would multiply the budget by the retry window at exactly the
/// moment the registry is failing.
///
/// `size` is the compressed size the digest pipeline reported as it wrote this
/// file, and the read is exact rather than to the end. One allocation of
/// exactly the layer, so what the resident-byte permit admitted is what the
/// upload actually holds — `read_to_end` would probe past the end and can grow
/// the buffer past the permit that paid for it. A file that does not hold
/// `size` bytes is a bug in this process and fails here rather than as a digest
/// the registry rejects.
fn staged_bytes(mut staged: File, size: u64) -> Result<bytes::Bytes, Error> {
    let mut body = vec![0u8; size as usize];

    staged.read_exact(&mut body)?;

    Ok(bytes::Bytes::from(body))
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
        lock(&self.state)
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

    /// [`retrying`], with this transport's own bound and observer.
    ///
    /// Every round trip made on a caller's thread goes through here, so the
    /// policy is stated once and no seam is left out by having been written
    /// before the policy existed.
    fn retrying<T>(&self, op: impl FnMut() -> Result<T, Error>) -> Result<T, Error> {
        retrying(self.attempts, &self.observer(), op)
    }

    fn blob_exists(&self, digest: &Digest) -> Result<bool, Error> {
        let named = digest.to_string();

        self.retrying(|| {
            Ok(self
                .runtime
                .block_on(self.client.blob_exists(&self.repository, &named))?)
        })
    }

    /// Take a permit, on the caller's thread.
    ///
    /// This is the back pressure, and taking it *here* rather than inside the
    /// task is what makes it back pressure at all: the caller does not get to
    /// stage a ninth layer while eight are still moving, so the scratch
    /// directory is bounded by the same number as the wire.
    ///
    /// The resident-byte permit cannot be taken here — the layer is not closed
    /// yet and has no size — which is why there are two of them and why they
    /// are taken in different places. See [`Shared::resident`].
    fn permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        // The semaphore is never closed — nothing here closes one — so the only
        // error this call has is unreachable.
        self.runtime
            .block_on(Arc::clone(&self.permits).acquire_owned())
            .expect("the transport's semaphore is never closed")
    }

    /// Run one layer's round trips off the caller's thread.
    ///
    /// The future is built by the caller out of clones and owns everything it
    /// touches, which is the invariant that keeps a task from holding the
    /// runtime driving it — see [`Shared::state`].
    fn defer(&self, task: impl std::future::Future<Output = Result<(), Error>> + Send + 'static) {
        let handle = self.runtime.spawn(task);

        self.inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(handle);
    }

    /// Wait for every deferred round trip, and report the first failure.
    ///
    /// **Every** one, including after a failure has already been seen: a task
    /// left running is a task still writing to a registry the caller is about
    /// to be told nothing was written to, and — on the error path — one that
    /// would be cancelled by the runtime shutting down under it. The first
    /// error is the one reported because the others are usually the same
    /// network saying the same thing twice.
    ///
    /// A panicking task is re-raised on this thread rather than folded into an
    /// error: a panic in an upload is a bug in this module, and turning it into
    /// a returned `Err` would file it under "the registry refused".
    ///
    /// **A failure here is remembered.** The first call reports the cause
    /// itself; every call after it reports [`Error::LayerNotWritten`], because
    /// the handles are gone and a join that found nothing outstanding would
    /// otherwise read as "everything landed".
    ///
    /// A failure that arrives here has already been retried — each round trip
    /// was made [`Options::attempts`] times while its staged bytes were still
    /// in hand, which is the only moment at which the transport can do anything
    /// about it. So what reaches this point is a registry that means it, and
    /// permanence is the right answer to it rather than a harsh one.
    fn join_layers(&self) -> Result<(), Error> {
        let handles: Vec<_> = std::mem::take(
            &mut *self
                .inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );

        let (first, panicked) = self.runtime.block_on(async {
            let mut first: Option<Error> = None;
            let mut panicked: Option<Box<dyn std::any::Any + Send>> = None;

            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => first = first.or(Some(error)),
                    Err(join) if join.is_panic() => panicked = panicked.or(Some(join.into_panic())),
                    // Nothing here cancels a task, so this arm is the runtime
                    // shutting down mid-join, which cannot happen while this
                    // call holds it.
                    Err(_) => {}
                }
            }

            (first, panicked)
        });

        if let Some(payload) = panicked {
            std::panic::resume_unwind(payload);
        }

        let mut state = self.locked();

        if let Some(error) = first {
            state.failed.get_or_insert_with(|| error.to_string());

            return Err(error);
        }

        match &state.failed {
            Some(why) => Err(Error::LayerNotWritten(why.clone())),
            None => Ok(()),
        }
    }

    /// Upload a staged layer, unless the registry already has it — later.
    ///
    /// The existence check *is* the blob-skip — the whole delta-transfer claim
    /// reduces to this one `HEAD` per layer — and both outcomes are counted.
    ///
    /// The layer is added to the pending list here, on the caller's thread and
    /// in finish order, so [`Registry::carried`] answers for it the moment its
    /// sink returns and the manifest is built out of the order the inscription
    /// was. What is deferred is the two round trips, and their failure, which
    /// [`Shared::join_layers`] collects at the seal.
    fn put_layer(&self, layer: &WrittenLayer, staged: File, permit: Permit) {
        let digest = layer.digests.blob_digest;
        let size = layer.digests.compressed_size;
        let observer = self.observer();
        let attempts = self.attempts;

        self.locked().pending.push(layer.clone());

        let client = self.client.clone();
        let repository = self.repository.clone();
        let state = Arc::clone(&self.state);
        let resident = Arc::clone(&self.resident);
        let monolithic_max = self.monolithic_max;

        self.defer(async move {
            let _permit = permit;
            let named = digest.to_string();

            let present = retrying_async(attempts, &observer, || {
                let client = client.clone();
                let repository = repository.clone();
                let named = named.clone();

                async move { Ok(client.blob_exists(&repository, &named).await?) }
            })
            .await?;

            if present {
                // Announced even though nothing moves: "the registry already
                // had this one" is the blob-skip working, and a watcher that
                // only heard about uploads would read the whole point of a
                // content-addressed registry as a stall.
                observer.emit(Event::Blob {
                    moved: false,
                    bytes: size,
                });

                let mut state = lock(&state);
                state.transfer.layers_skipped += 1;
                state.transfer.bytes_skipped += size;

                return Ok(());
            }

            // Before the upload rather than after it, so a watcher knows how
            // big the transfer it is about to see is while it is still
            // happening.
            //
            // Once, however many attempts the upload takes. What a lost attempt
            // moved is bytes that crossed the wire and are not coming back, so
            // the deltas can outrun this announcement — which is a fact about
            // the link, and [`Event::Bytes`] says so where a renderer will read
            // it.
            observer.emit(Event::Blob {
                moved: true,
                bytes: size,
            });

            // The staged bytes are still in hand here, which is the whole
            // reason this is the right place for the retry: past the join the
            // layer's only copy is the store it was built from, and the
            // recovery costs a publish. Rewound per attempt, because both paths
            // consume the handle they are given.
            if monolithic_max > 0 && size <= monolithic_max {
                retrying_async(attempts, &observer, || {
                    let client = client.clone();
                    let repository = repository.clone();
                    let named = named.clone();
                    let resident = Arc::clone(&resident);
                    let staged = rewound(&staged);

                    async move {
                        // Taken per attempt and released with the attempt, so
                        // the doubling wait between two of them holds no bytes:
                        // a registry answering `500` is exactly when this
                        // transport should be at its smallest.
                        let _bytes = resident
                            .acquire_many_owned(size as u32)
                            .await
                            .expect("the transport's semaphore is never closed");

                        client
                            .push_blob(&repository, staged_bytes(staged?, size)?, &named)
                            .await?;

                        Ok(())
                    }
                })
                .await?;

                // Once, at the end, because there is nothing finer to say: the
                // request either landed or it did not. A layer that took two
                // attempts reports what it *is* rather than what crossed the
                // wire, which is the opposite of the streamed path's answer and
                // the honest one for a transfer with no intermediate states.
                observer.emit(Event::Bytes(size));
            } else {
                retrying_async(attempts, &observer, || {
                    let client = client.clone();
                    let repository = repository.clone();
                    let named = named.clone();
                    let observer = observer.clone();
                    let staged = rewound(&staged);

                    async move {
                        client
                            .push_blob_stream(&repository, blob_stream(staged?, observer), &named)
                            .await?;

                        Ok(())
                    }
                })
                .await?;
            }

            let mut state = lock(&state);
            state.transfer.layers_uploaded += 1;
            state.transfer.bytes_uploaded += size;

            Ok(())
        });
    }

    /// Prove — later — that the registry still holds a blob being carried
    /// forward.
    ///
    /// [`Options::verify_adopted`] only. Deferred for the reason an upload is,
    /// and joined at the same point: what the check has to beat is the manifest
    /// naming the blob, not the descriptor being handed back.
    fn prove_blob(&self, layer: &WrittenLayer, permit: Permit) {
        let kind = layer.descriptor.kind.clone();
        let diff_id = layer.descriptor.diff_id;
        let blob = layer.digests.blob_digest;

        let client = self.client.clone();
        let repository = self.repository.clone();
        let observer = self.observer();
        let attempts = self.attempts;

        self.defer(async move {
            let _permit = permit;
            let named = blob.to_string();

            // The retry is over the round trip and not over the verdict: a
            // registry that answered "no" answered, and asking a second time is
            // how a publisher talks itself into carrying a blob that is gone.
            let present = retrying_async(attempts, &observer, || {
                let client = client.clone();
                let repository = repository.clone();
                let named = named.clone();

                async move { Ok(client.blob_exists(&repository, &named).await?) }
            })
            .await?;

            match present {
                true => Ok(()),
                false => Err(Error::BlobMissing {
                    kind,
                    diff_id: diff_id.to_string(),
                    blob: named,
                }),
            }
        });
    }

    /// Upload a small blob a caller already holds — the inscription, and
    /// nothing else.
    ///
    /// Goes up in one request like a small layer does, and for the same reason:
    /// `use_monolithic_push` is set on the client, so `push_blob` sends a
    /// `POST` and a `PUT` rather than opening a chunked session for a
    /// document of a few kilobytes. No resident-byte permit — the
    /// inscription is bounded by [`MANIFEST_SIZE_LIMIT`]'s order of
    /// magnitude and by being one document, not by a budget shared with the
    /// layers.
    fn put_bytes(&self, digest: &Digest, bytes: Vec<u8>) -> Result<(), Error> {
        if self.blob_exists(digest)? {
            return Ok(());
        }

        let named = digest.to_string();

        // Into `Bytes` once, so an attempt after the first re-sends the
        // document rather than re-allocating it: this is the inscription, and
        // the clone a retry costs should be a refcount.
        let bytes = bytes::Bytes::from(bytes);

        self.retrying(|| {
            self.runtime.block_on(self.client.push_blob(
                &self.repository,
                bytes.clone(),
                &named,
            ))?;

            Ok(())
        })
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
        self.retrying(|| {
            // Emptied rather than reused, so a second attempt writes the
            // document and not the document twice. `Blocking` counts from zero
            // per attempt for the same reason, which it gets by being built
            // here.
            buffer.clear();

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

            Ok(())
        })?;

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

        self.retrying(|| {
            // Back to empty before each attempt, for the reason a staged layer
            // is rewound before each of its own: what a half-finished download
            // left behind is not a prefix of what the next one writes, it is
            // bytes in front of it.
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;

            self.runtime.block_on(self.client.pull_blob(
                reference,
                descriptor,
                Blocking::new(
                    &mut file,
                    descriptor.size,
                    &descriptor.digest,
                    observer.clone(),
                ),
            ))?;

            Ok(())
        })?;

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
    /// and nothing is checked — the blob was handed to the upload pool when the
    /// first descriptor's sink finished, in this same publish, and the seal
    /// joins that upload before it names either descriptor. So there is no
    /// state of the registry under which it is there for one name and absent
    /// for the other.
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
    /// 1. **every layer blob is up.** The round trips were deferred by
    ///    [`RecordSink::finish`] and [`Registry::adopt_layer`] and ran
    ///    concurrently; this is where they are joined, and the first failure
    ///    among them is this call's failure;
    /// 2. the inscription goes up as the config blob;
    /// 3. the manifest is tagged with the immutable tag the profile renders for
    ///    this sequence;
    /// 4. and **only then** the moving tag moves.
    ///
    /// Step 1 is the serialization point the concurrency is arranged around:
    /// the manifest is built *after* it, so no document this transport writes
    /// can name a blob the registry has not committed. Step 4 is last so that a
    /// reader following `latest` never resolves to a stele whose blobs are
    /// still uploading. A push that dies in the middle leaves untagged blobs
    /// the registry will reclaim, and a `latest` that still points at the
    /// previous stele — which is a stele, and restores.
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
    ///
    /// **A seal that fails at step 1 is the exception, and it is permanent.** A
    /// layer that did not reach the repository cannot be sent again from here:
    /// its staging went with the round trip that lost it, and the bytes are
    /// only in the store the publisher built them from. So the failure is
    /// remembered, every later seal answers [`Error::LayerNotWritten`], and the
    /// recovery is another publish rather than another seal. Retrying the seal
    /// is what would produce the one document this transport must never
    /// write — see [`Shared::join_layers`].
    ///
    /// Which is why the retry that *can* help is not here but inside the round
    /// trip, where the staging file has not been spent yet: by the time a
    /// failure reaches this join it has already been asked
    /// [`Options::attempts`] times.
    fn seal(&self, profile: &dyn Profile, inscription: &Inscription) -> Result<Digest, Error> {
        // Both tags before either push. Validating the moving tag after the
        // sequence manifest is already public would make a bad tag something
        // the registry finds out about half way through.
        let sequence_tag = checked_tag_for_sequence(profile, inscription.sequence)?;
        let moving_tag = profile.moving_tag().to_owned();
        validate_tag(&moving_tag)?;

        // The join, and it is before the manifest is even built rather than
        // merely before it is pushed: a document assembled out of a pending
        // list whose blobs are still in flight is a document that must not
        // exist, not one that must not be sent. Fallible, like every other step
        // here, and — like every other step here — it runs before the layers
        // are spent, so a transport whose upload the network broke can be
        // sealed again once the caller has decided what to do about it.
        self.shared.join_layers()?;

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

        self.retrying(|| {
            self.runtime.block_on(self.client.push_manifest_raw(
                &reference,
                body.clone(),
                http::HeaderValue::from_static(OCI_IMAGE_MEDIA_TYPE),
            ))?;

            Ok(())
        })
    }
}

/// A layer being written into a registry, one record at a time.
///
/// Staged into a temporary file for a reason that is not a limitation of this
/// implementation: both push paths take the digest up front, and a layer's
/// digest is the digest of its own compressed bytes. There is no ordering of
/// the operations in which an upload learns the name first.
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

    /// Close the layer and hand it to the upload pool.
    ///
    /// The descriptor comes back as soon as the last record is framed: it is a
    /// fact about bytes this sink already has, and nothing the registry says
    /// can change it. The upload — and the `HEAD` that may make it
    /// unnecessary — runs concurrently with whatever the caller does next, and
    /// is joined by [`SteleWriter::seal`] before the manifest can name it.
    ///
    /// So an error here is a failure to *close the layer*; a failure to move it
    /// surfaces at the seal. That is a change in when a registry's refusal is
    /// reported and not in what it costs: a publish that cannot upload is a
    /// publish that does not seal, in either arrangement, having tagged
    /// nothing.
    fn finish(self) -> Result<WrittenLayer, Error> {
        let Self {
            sequence,
            shared,
            kind,
            media_type,
            scope,
        } = self;

        // Before the layer is closed, so the bound on outstanding round trips
        // is also the bound on staged layers: a caller with sixteen sinks open
        // waits here rather than filling the scratch directory. Taken before
        // the fallible steps below for the same reason it is released by the
        // task — a permit's lifetime is the layer's, and a layer that never
        // closes has none.
        let permit = shared.permit();

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

        shared.put_layer(&written, staged, permit);

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
/// The registry's answer to the question [`crate::dir::BLOB_INDEX_FILE`]
/// answers for a directory, and the reason a registry restore has always read
/// every blob once instead of twice.
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
/// The path for the layers [`Options::monolithic_max`] excludes — on mainnet,
/// `blocks` and one `state-accounts` shard. Everything smaller goes up whole
/// through [`staged_bytes`], in one request rather than a `PATCH` chain.
///
/// Reads from the staging file synchronously. The runtime under it is this
/// transport's own — the read is not waiting on anything it is responsible for
/// driving — and it is sized so that a read blocking a worker cannot starve the
/// other uploads: one worker per permit, decided in [`Registry::open`].
///
/// One chunk is allocated at a time and handed over, so what a *streamed*
/// upload holds is [`UPLOAD_CHUNK`] and not the layer.
/// Each chunk is reported as it is handed over, which is the only resolution
/// this loop has: a `PATCH` either went out or it did not, and the client does
/// not say how much of one has reached the wire. A single-request layer has no
/// such resolution to report and announces itself once, at the end.
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
                match read_uninterrupted(&mut file, &mut chunk[filled..]) {
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

    /// A `reqwest` error carrying a status, which is how
    /// [`oci_client::Client::blob_exists`] reports one.
    ///
    /// There is no constructor for one, so it is provoked: a response with the
    /// status, asked to fail on it. Built on the transport's own runtime kind
    /// rather than in an async test, because this module's rule is that its
    /// types are used from synchronous code.
    fn request_error(code: u16) -> OciDistributionError {
        let response = http::Response::builder()
            .status(code)
            .body(Vec::new())
            .unwrap();

        let refused = reqwest::Response::from(response)
            .error_for_status()
            .expect_err("a status the builder was given was not an error");

        OciDistributionError::RequestError(refused)
    }

    /// The two shapes worth asking again about, and the several that are not.
    #[test]
    fn a_transient_failure_is_the_registry_talking_about_itself() {
        // The measured class: the create-session `POST` that answers 500 and
        // works on the next ask.
        assert!(is_transient(&Error::Registry(server_error(500))));
        assert!(is_transient(&Error::Registry(server_error(502))));
        assert!(is_transient(&Error::Registry(server_error(503))));

        // And the same thing seen through `blob_exists`, which asks `reqwest`
        // for the status instead of mapping it.
        assert!(is_transient(&Error::Registry(request_error(500))));

        // A refusal of *this* request is not. Repeating it only delays the
        // report, and `429` is deliberately among them: a registry rationing
        // this publisher is something its operator has to be told.
        assert!(!is_transient(&Error::Registry(server_error(400))));
        assert!(!is_transient(&Error::Registry(server_error(404))));
        assert!(!is_transient(&Error::Registry(server_error(429))));
        assert!(!is_transient(&Error::Registry(request_error(429))));
        assert!(!is_transient(&Error::Registry(envelope(
            OciErrorCode::DigestInvalid
        ))));
        assert!(!is_transient(&Error::Registry(
            OciDistributionError::UnauthorizedError {
                url: "https://registry.invalid/v2/".to_owned(),
            }
        )));

        // Nor is anything that never involved the far side. A staging file that
        // would not read reads the same the second time.
        assert!(!is_transient(&Error::Io(std::io::Error::other("staging"))));
        assert!(!is_transient(&Error::LayerNotWritten("earlier".to_owned())));
    }

    /// A recorder for what the retry loop announced.
    #[derive(Default)]
    struct Retries(Mutex<Vec<(u32, u32)>>);

    impl crate::progress::Progress for Retries {
        fn on(&self, event: Event<'_>) {
            if let Event::Retry {
                attempt, remaining, ..
            } = event
            {
                self.0.lock().unwrap().push((attempt, remaining));
            }
        }
    }

    /// The loop with the waits taken out, so the bound can be exercised without
    /// waiting out a real backoff.
    fn retried<T>(
        attempts: u32,
        observer: &Observer,
        mut op: impl FnMut() -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut backoff = Backoff::new(attempts, Duration::ZERO);

        loop {
            match op() {
                Ok(value) => return Ok(value),
                Err(error) => match backoff.wait_after(&error, observer) {
                    Some(_) => continue,
                    None => return Err(error),
                },
            }
        }
    }

    #[test]
    fn a_call_that_succeeds_is_made_once() {
        let mut calls = 0;

        let value = retried(4, &Observer::silent(), || {
            calls += 1;
            Ok(7u8)
        })
        .unwrap();

        assert_eq!(value, 7);
        assert_eq!(calls, 1, "a success must not be retried");
    }

    /// The whole point: the registry's bad half-second costs a half-second and
    /// not an epoch.
    #[test]
    fn a_transient_failure_is_absorbed() {
        let watcher = Arc::new(Retries::default());
        let observer = Observer::new(watcher.clone());

        let mut calls = 0;

        let value = retried(4, &observer, || {
            calls += 1;

            match calls < 3 {
                true => Err(Error::Registry(server_error(500))),
                false => Ok(calls),
            }
        })
        .unwrap();

        assert_eq!(value, 3);
        assert_eq!(calls, 3);

        // And it said so both times, counting the failures up and the patience
        // down, so a watcher can tell a hiccup from a transport about to give
        // up.
        assert_eq!(*watcher.0.lock().unwrap(), vec![(1, 3), (2, 2)]);
    }

    /// Bounded, so a registry that is wrong rather than flaky still fails —
    /// and fails as itself, with what it actually said.
    #[test]
    fn patience_runs_out_and_the_last_failure_is_the_one_reported() {
        let watcher = Arc::new(Retries::default());
        let observer = Observer::new(watcher.clone());

        let mut calls = 0;

        let refused = retried(4, &observer, || {
            calls += 1;
            Err::<(), _>(Error::Registry(server_error(500)))
        })
        .expect_err("a registry that never answered was treated as having answered");

        assert_eq!(calls, 4, "the bound is attempts, not retries");
        assert!(matches!(refused, Error::Registry(_)));

        // Three retries for four attempts, and nothing announced for the last
        // failure — there was no next attempt to announce.
        assert_eq!(*watcher.0.lock().unwrap(), vec![(1, 3), (2, 2), (3, 1)]);
    }

    /// A refusal of this request is reported the first time it is made.
    #[test]
    fn a_refusal_of_this_request_is_not_retried() {
        let watcher = Arc::new(Retries::default());
        let observer = Observer::new(watcher.clone());

        let mut calls = 0;

        let refused = retried(4, &observer, || {
            calls += 1;
            Err::<(), _>(Error::Registry(envelope(OciErrorCode::DigestInvalid)))
        })
        .expect_err("a digest the registry rejected was retried");

        assert_eq!(calls, 1, "a refusal must cost one round trip");
        assert!(matches!(refused, Error::Registry(_)));
        assert!(watcher.0.lock().unwrap().is_empty());
    }

    /// `0` attempts is one attempt, for the reason `0` concurrency is one
    /// permit: a transport that would make no attempt at all is not a
    /// configuration anybody means.
    #[test]
    fn no_attempts_at_all_is_still_one_attempt() {
        let mut calls = 0;

        let refused = retried(0, &Observer::silent(), || {
            calls += 1;
            Err::<(), _>(Error::Registry(server_error(500)))
        });

        assert_eq!(calls, 1);
        assert!(refused.is_err());
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
