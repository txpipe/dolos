use axum::http::StatusCode;
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code as MetadataError, DrepsInnerMetadata, DrepsInnerMetadataError,
};
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::primitives::conway::Anchor,
};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, OnceLock,
    },
    time::{Duration, Instant, SystemTime},
};

const MAX_METADATA_BYTES: usize = 1024 * 1024;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REDIRECTS: usize = 3;

/// An anchor pins its content by hash, so a verified fetch never has to be
/// repeated: its body goes to the [`OffchainStore`] on disk and the rendered
/// metadata stays in memory within these caps. A failed fetch is kept in
/// memory for `FAILURE_TTL`, so a dead host costs one timeout per window
/// instead of one per page render.
const FAILURE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_CACHE_ENTRIES: usize = 4096;
const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

// Blockfrost hands back db-sync's own fetch-error text and backend-ryo picks
// the `code` out of that text by keyword (`transformOffChainFetchError`), so
// the messages below keep db-sync's wording, prefix included.
const ERROR_PREFIX: &str = "Error Offchain Voting Anchor";

fn hash_mismatch_error(
    url: &str,
    expected_hash: &[u8],
    actual_hash: &[u8],
) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::HashMismatch,
        format!(
            "Hash mismatch when fetching metadata from {url}. Expected \"{}\" but got \"{}\".",
            hex::encode(expected_hash),
            hex::encode(actual_hash),
        ),
    )
}

fn http_response_error(url: &str, status: StatusCode) -> DrepsInnerMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepsInnerMetadataError::new(
        MetadataError::HttpResponseError,
        format!(
            "{ERROR_PREFIX}: HTTP Response error from {url} resulted in HTTP status code : {} \"{reason}\"",
            status.as_u16(),
        ),
    )
}

fn connection_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::ConnectionError,
        format!("{ERROR_PREFIX}: Connection failure error when fetching metadata from {url}."),
    )
}

fn size_exceeded_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::SizeExceeded,
        format!(
            "{ERROR_PREFIX}: Size error when fetching metadata from {url}, the payload exceeds {MAX_METADATA_BYTES} bytes."
        ),
    )
}

/// db-sync refuses localhost anchors with a URL parse error, which ryo files
/// under `CONNECTION_ERROR`.
fn blocked_url_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::ConnectionError,
        format!(
            "{ERROR_PREFIX}: URL parse error for {url} resulted in : \"Access to non-public addresses is not allowed\""
        ),
    )
}

/// db-sync decides whether a payload is JSON from the response's content
/// type and files a mismatch as an HTTP response error quoting the type it
/// got, before it ever looks at the body or its hash.
fn content_type_error(url: &str, content_type: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::HttpResponseError,
        format!("{ERROR_PREFIX}: HTTP Response error from {url}: expected JSON, but got : \"{content_type}\""),
    )
}

fn decode_error(url: &str, reason: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::DecodeError,
        format!(
            "{ERROR_PREFIX}: JSON decode error when fetching metadata from {url} resulted in : \"{reason}\""
        ),
    )
}

/// Whether a response *claims* to carry JSON. This only picks which error a
/// body that would not parse is reported under, never whether the body is
/// read: anchors are served by whatever the DRep pointed at, and valid
/// metadata arrives under `text/plain` from a raw GitHub file or
/// `binary/octet-stream` from an S3 bucket often enough that the type alone
/// cannot turn a payload away.
fn content_type_claims_json(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .contains("json")
}

/// Address blocks a public host never sits in, written the way IANA's
/// special-purpose registries list them: the private ranges and
/// carrier-grade NAT, loopback, link-local (where the cloud metadata
/// services answer), multicast, documentation and the reserved space.
///
/// Spelled out rather than assembled from the `std` predicates because
/// those miss carrier-grade NAT and leave every IPv6 classifier unstable.
const RESERVED_V4: &[(Ipv4Addr, u32)] = &[
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(100, 64, 0, 0), 10),
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 0, 0, 0), 24),
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(224, 0, 0, 0), 4),
    (Ipv4Addr::new(240, 0, 0, 0), 4),
];

/// The same for IPv6. `::/96` covers the unspecified address, the loopback
/// and the deprecated IPv4-compatible form in one entry; the IPv4-mapped
/// block needs no entry because those addresses are unwrapped and judged as
/// the IPv4 addresses they dial.
const RESERVED_V6: &[(Ipv6Addr, u32)] = &[
    (Ipv6Addr::UNSPECIFIED, 96),
    (Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0), 64),
    (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
    (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
    (Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8),
];

/// Whether `addr` sits inside the `bits`-long prefix of `block`.
fn in_prefix(addr: &[u8], block: &[u8], bits: u32) -> bool {
    let whole = bits as usize / 8;
    let spare = bits % 8;

    addr[..whole] == block[..whole]
        && (spare == 0 || (addr[whole] ^ block[whole]) >> (8 - spare) == 0)
}

/// The anchor URL is attacker-controlled on-chain data, so an address in one
/// of the reserved blocks would let a DRep aim the node at its own network
/// position rather than at metadata.
fn ip_is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => !RESERVED_V4
            .iter()
            .any(|(block, bits)| in_prefix(&ip.octets(), &block.octets(), *bits)),

        // an IPv4 address wearing an IPv6 coat still dials the IPv4 host, so
        // it faces the IPv4 rules
        IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
            Some(ip) => ip_is_public(IpAddr::V4(ip)),
            None => !RESERVED_V6
                .iter()
                .any(|(block, bits)| in_prefix(&ip.octets(), &block.octets(), *bits)),
        },
    }
}

/// The gate on the URL itself: scheme, and a host that is a literal address
/// or `localhost`. A hostname passes here and gets vetted by
/// [`PublicOnlyResolver`] once it resolves.
fn is_fetchable(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };

    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }

    let Some(host) = parsed.host_str() else {
        return false;
    };

    // IPv6 hosts keep their brackets in `host_str`
    let host = host.trim_start_matches('[').trim_end_matches(']');

    match host.parse::<IpAddr>() {
        Ok(ip) => ip_is_public(ip),
        Err(_) => !host.eq_ignore_ascii_case("localhost"),
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The addresses of `host` the client may dial: whatever it resolves to,
/// minus the non-public ranges.
fn public_addresses(host: &str) -> std::io::Result<Vec<SocketAddr>> {
    // the port is a placeholder; the connector replaces it with the URL's
    let public: Vec<_> = (host, 0u16)
        .to_socket_addrs()?
        .filter(|addr| ip_is_public(addr.ip()))
        .collect();

    if public.is_empty() {
        return Err(std::io::Error::other(format!(
            "{host} resolves to no public address"
        )));
    }

    Ok(public)
}

/// A DRep controls its anchor's DNS records as much as its URL, so a name
/// resolving into the node's own network would slip past the literal-host
/// check in `is_fetchable`. This resolver runs for every connection the
/// client opens, redirect hops included, and hands the connector only the
/// vetted addresses: nothing can change between the check and the dial.
struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();

        Box::pin(async move {
            let addrs = tokio::task::spawn_blocking(move || public_addresses(&host)).await??;

            Ok::<Addrs, BoxError>(Box::new(addrs.into_iter()))
        })
    }
}

fn http_client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    if let Some(client) = CLIENT.get() {
        return Some(client);
    }

    // built outside `get_or_init` so a failed build is retried on the next
    // call instead of pinning every future fetch to a connection error
    let built = reqwest::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .dns_resolver(Arc::new(PublicOnlyResolver))
        // every redirect hop gets the same URL gate as the anchor, and its
        // hostname goes through the resolver like the anchor's did
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > MAX_REDIRECTS {
                attempt.error("too many redirects")
            } else if !is_fetchable(attempt.url().as_str()) {
                attempt.error("redirect to a non-public URL")
            } else {
                attempt.follow()
            }
        }))
        .user_agent("Dolos MiniBF")
        .build()
        .ok()?;

    Some(CLIENT.get_or_init(|| built))
}

type CacheKey = (String, Hash<32>);

struct CacheEntry {
    metadata: DrepsInnerMetadata,
    stored_at: Instant,
    size: usize,
}

impl CacheEntry {
    fn is_stale(&self, now: Instant) -> bool {
        // a verified fetch never goes stale: the hash pins the content
        self.metadata.error.is_some() && now.duration_since(self.stored_at) > FAILURE_TTL
    }
}

struct MetadataCache {
    entries: HashMap<CacheKey, CacheEntry>,
    // insertion order, oldest first, for eviction
    order: VecDeque<CacheKey>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl MetadataCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&self, key: &CacheKey, now: Instant) -> Option<DrepsInnerMetadata> {
        let entry = self.entries.get(key)?;

        (!entry.is_stale(now)).then(|| entry.metadata.clone())
    }

    fn insert(&mut self, key: CacheKey, metadata: DrepsInnerMetadata, now: Instant) {
        // the hex `bytes` dominate an entry, so the budget is a bound on
        // the order of the real footprint rather than an exact figure
        let size = key.0.len() + metadata.bytes.as_ref().map_or(0, |x| x.len());

        let entry = CacheEntry {
            metadata,
            stored_at: now,
            size,
        };

        match self.entries.insert(key.clone(), entry) {
            Some(old) => self.bytes -= old.size,
            None => self.order.push_back(key),
        }

        self.bytes += size;

        while self.entries.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };

            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes -= old.size;
            }
        }
    }
}

fn cache() -> MutexGuard<'static, MetadataCache> {
    static CACHE: OnceLock<Mutex<MetadataCache>> = OnceLock::new();

    CACHE
        .get_or_init(|| Mutex::new(MetadataCache::new(MAX_CACHE_ENTRIES, MAX_CACHE_BYTES)))
        .lock()
        // plain data under the lock: a panic elsewhere leaves nothing half-done
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn errored(mut out: DrepsInnerMetadata, error: DrepsInnerMetadataError) -> DrepsInnerMetadata {
    out.error = Some(Box::new(error));
    out
}

/// Verified bodies, content-addressed by their hash: one file per hash under
/// `<storage.path>/offchain`, so a fetch outlives the process. The hash check
/// on the way back in guards the file the way it guarded the download, and a
/// failure never reaches the disk.
pub struct OffchainStore {
    dir: PathBuf,
    budget: u64,
}

impl OffchainStore {
    pub const DIR: &'static str = "offchain";

    /// `budget` is the disk the store may occupy, in bytes; zero turns it
    /// off and leaves only the in-process cache.
    pub fn new(storage_path: &Path, budget: u64) -> Self {
        Self {
            dir: storage_path.join(Self::DIR),
            budget,
        }
    }

    fn path(&self, hash: &Hash<32>) -> PathBuf {
        self.dir.join(hex::encode(hash))
    }

    async fn read(&self, hash: Hash<32>) -> Option<Vec<u8>> {
        let path = self.path(&hash);

        let body = tokio::task::spawn_blocking(move || std::fs::read(path))
            .await
            .ok()?
            .ok()?;

        (Hasher::<256>::hash(&body) == hash).then_some(body)
    }

    /// Weigh the directory and drop the oldest files until it fits the
    /// budget again, taking it a fifth below so the next walk is not
    /// immediate. Age is the order the files were written, not the order
    /// they were last read: the anchor hash pins each body for good, so no
    /// entry is worth more than another and only the bound matters.
    async fn enforce_budget(&self) {
        let dir = self.dir.clone();
        let budget = self.budget;

        let _ = tokio::task::spawn_blocking(move || {
            let mut files: Vec<(SystemTime, u64, PathBuf)> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    let meta = entry.metadata().ok()?;

                    if !meta.is_file() {
                        return None;
                    }

                    Some((meta.modified().ok()?, meta.len(), entry.path()))
                })
                .collect();

            let total: u64 = files.iter().map(|(_, size, _)| size).sum();

            let Some(mut excess) = total.checked_sub(budget - budget / 5) else {
                return Ok(());
            };

            files.sort_by_key(|(written, _, _)| *written);

            for (_, size, path) in files {
                if excess == 0 {
                    break;
                }

                if std::fs::remove_file(&path).is_ok() {
                    excess = excess.saturating_sub(size);
                }
            }

            std::io::Result::Ok(())
        })
        .await;
    }

    async fn write(&self, hash: Hash<32>, body: Vec<u8>) {
        static SEQ: AtomicU64 = AtomicU64::new(0);

        // Weighing the directory costs a walk, so it happens once per
        // fifth-of-a-budget written rather than per file, which caps the
        // overshoot at that same fifth.
        static UNWEIGHED: AtomicU64 = AtomicU64::new(0);

        if self.budget == 0 {
            return;
        }

        let dir = self.dir.clone();
        let path = self.path(&hash);
        let size = body.len() as u64;

        // written beside its final name and renamed into place, so a
        // concurrent reader never sees a partial body
        let written = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;

            let tmp = dir.join(format!(
                "{}.{}.{}.tmp",
                hex::encode(hash),
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));

            std::fs::write(&tmp, &body)?;
            std::fs::rename(tmp, path)
        })
        .await;

        // the store is a cache: a body that could not be kept is fetched
        // again next time
        if let Err(err) = written.map_err(std::io::Error::other).and_then(|x| x) {
            tracing::warn!(%err, "failed to store off-chain metadata");
            return;
        }

        let unweighed = UNWEIGHED.fetch_add(size, Ordering::Relaxed) + size;

        if unweighed >= self.budget / 5 {
            UNWEIGHED.store(0, Ordering::Relaxed);
            self.enforce_budget().await;
        }
    }
}

/// The verified body of `anchor`, or the error Blockfrost would report.
async fn fetch_anchor(anchor: &Anchor) -> Result<Vec<u8>, DrepsInnerMetadataError> {
    let url = &anchor.url;

    let client = http_client().ok_or_else(|| connection_error(url))?;

    if !is_fetchable(url) {
        return Err(blocked_url_error(url));
    }

    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| connection_error(url))?;

    if !response.status().is_success() {
        return Err(http_response_error(url, response.status()));
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    if response
        .content_length()
        .is_some_and(|len| len > MAX_METADATA_BYTES as u64)
    {
        return Err(size_exceeded_error(url));
    }

    let mut body = Vec::new();

    while let Some(chunk) = response.chunk().await.map_err(|_| connection_error(url))? {
        if body.len() + chunk.len() > MAX_METADATA_BYTES {
            return Err(size_exceeded_error(url));
        }

        body.extend_from_slice(&chunk);
    }

    // db-sync decides a payload is not metadata at all before it weighs the
    // body against the anchor's hash, which is why Blockfrost answers a
    // shortener's HTML page with this error rather than a hash mismatch. It
    // reads the content type to say so, but the type alone is too unreliable
    // to reject on, so it only names the failure of a body that would not
    // have parsed anyway.
    if serde_json::from_slice::<serde_json::Value>(&body).is_err()
        && !content_type_claims_json(&content_type)
    {
        return Err(content_type_error(url, &content_type));
    }

    let actual_hash = Hasher::<256>::hash(&body);

    if actual_hash != anchor.content_hash {
        return Err(hash_mismatch_error(
            url,
            anchor.content_hash.as_ref(),
            actual_hash.as_ref(),
        ));
    }

    Ok(body)
}

/// The metadata of a verified body.
fn verified(out: DrepsInnerMetadata, body: &[u8]) -> DrepsInnerMetadata {
    match serde_json::from_slice(body) {
        Ok(json) => DrepsInnerMetadata {
            json_metadata: Some(json),
            bytes: Some(format!("\\x{}", hex::encode(body))),
            ..out
        },
        // the spec keeps `json_metadata` and `bytes` null on failed
        // validation and reports the failure through `error`
        Err(err) => {
            let error = decode_error(&out.url, &err.to_string());
            errored(out, error)
        }
    }
}

pub async fn fetch_drep_metadata(
    store: &OffchainStore,
    anchor: Option<Anchor>,
) -> Option<DrepsInnerMetadata> {
    let anchor = anchor?;
    let key = (anchor.url.clone(), anchor.content_hash);

    let cached = cache().get(&key, Instant::now());

    if cached.is_some() {
        return cached;
    }

    let out = DrepsInnerMetadata {
        url: anchor.url.clone(),
        hash: hex::encode(anchor.content_hash),
        json_metadata: None,
        bytes: None,
        error: None,
    };

    let metadata = match store.read(anchor.content_hash).await {
        Some(body) => verified(out, &body),
        None => match fetch_anchor(&anchor).await {
            Ok(body) => {
                store.write(anchor.content_hash, body.clone()).await;
                verified(out, &body)
            }
            Err(error) => errored(out, error),
        },
    };

    cache().insert(key, metadata.clone(), Instant::now());

    Some(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_http_schemes() {
        assert!(!is_fetchable("file:///etc/passwd"));
        assert!(!is_fetchable("ftp://example.com/x"));
        assert!(!is_fetchable("not a url"));
    }

    #[test]
    fn rejects_non_public_hosts() {
        assert!(!is_fetchable("http://127.0.0.1:8080/meta.json"));
        assert!(!is_fetchable("http://localhost:3000/meta.json"));
        assert!(!is_fetchable("http://169.254.169.254/latest/meta-data"));
        assert!(!is_fetchable("http://10.1.2.3/meta.json"));
        assert!(!is_fetchable("http://172.16.0.1/meta.json"));
        assert!(!is_fetchable("http://192.168.1.1/meta.json"));
        assert!(!is_fetchable("http://100.64.0.1/meta.json"));
        assert!(!is_fetchable("http://0.0.0.0/meta.json"));
        assert!(!is_fetchable("https://[::1]/meta.json"));
        assert!(!is_fetchable("https://[fe80::1]/meta.json"));
        assert!(!is_fetchable("https://[fd00::1]/meta.json"));
    }

    /// An IPv4 address written as IPv6 dials the IPv4 host, so the IPv4
    /// rules have to reach it: `::ffff:127.0.0.1` is not
    /// `Ipv6Addr::is_loopback`, yet it lands on the loopback.
    #[test]
    fn rejects_ipv4_addresses_written_as_ipv6() {
        for host in [
            "[::ffff:127.0.0.1]",
            "[::ffff:10.1.2.3]",
            "[::ffff:169.254.169.254]",
            "[::ffff:192.168.1.1]",
            "[::ffff:100.64.0.1]",
            // the deprecated IPv4-compatible spelling sits in `::/96`
            "[::127.0.0.1]",
        ] {
            let url = format!("http://{host}/meta.json");
            assert!(!is_fetchable(&url), "{url}");
        }

        // the coat itself is not the problem: a public IPv4 stays fetchable
        assert!(is_fetchable("http://[::ffff:93.184.216.34]/meta.json"));
    }

    #[test]
    fn prefixes_match_on_the_bits_that_matter() {
        let inside = |a: &str| !ip_is_public(a.parse().unwrap());

        // 100.64.0.0/10 ends mid-octet: .64 through .127 are carrier-grade
        // NAT, .63 and .128 are not
        assert!(inside("100.64.0.1"));
        assert!(inside("100.127.255.254"));
        assert!(!inside("100.63.255.255"));
        assert!(!inside("100.128.0.1"));

        // fc00::/7 covers fc.. and fd..
        assert!(inside("fc00::1"));
        assert!(inside("fdff::1"));
        assert!(!inside("fb00::1"));
        assert!(!inside("fe00::1"));
    }

    #[test]
    fn accepts_http_urls() {
        assert!(is_fetchable("https://example.com/meta.json"));
        assert!(is_fetchable("http://example.com/meta.json"));
        assert!(is_fetchable("https://93.184.216.34/meta.json"));
        assert!(is_fetchable("http://100.128.0.1/meta.json"));
    }

    /// `localhost` passes the URL gate as a name only to resolve into the
    /// loopback range, which is where the resolver has to refuse it.
    #[tokio::test]
    async fn resolver_refuses_names_on_non_public_addresses() {
        assert!(public_addresses("localhost").is_err());

        let name: Name = "localhost".parse().expect("failed to parse name");
        assert!(PublicOnlyResolver.resolve(name).await.is_err());
    }

    /// backend-ryo derives the error code from db-sync's message text; the
    /// keyword each message carries has to land on the code it is filed
    /// under.
    #[test]
    fn messages_carry_the_keywords_blockfrost_files_them_under() {
        let cases = [
            (hash_mismatch_error("u", &[1], &[2]), "hash mismatch"),
            (size_exceeded_error("u"), "size error"),
            (decode_error("u", "why"), "decode error"),
            (
                http_response_error("u", StatusCode::NOT_FOUND),
                "http response error",
            ),
            (content_type_error("u", "text/html"), "http response error"),
            (connection_error("u"), "connection failure error"),
            (blocked_url_error("u"), "url parse error"),
        ];

        for (error, keyword) in cases {
            assert!(
                error.message.to_lowercase().contains(keyword),
                "{:?} lacks {keyword:?}: {}",
                error.code,
                error.message
            );
        }

        assert_eq!(
            http_response_error("https://x.io/d.json", StatusCode::NOT_FOUND).message,
            "Error Offchain Voting Anchor: HTTP Response error from https://x.io/d.json resulted in HTTP status code : 404 \"Not Found\""
        );

        assert_eq!(
            content_type_error("https://x.io/d.json", "text/html; charset=utf-8").message,
            "Error Offchain Voting Anchor: HTTP Response error from https://x.io/d.json: expected JSON, but got : \"text/html; charset=utf-8\""
        );
    }

    #[test]
    fn content_types_that_claim_json() {
        for claimed in [
            "application/json",
            "application/json; charset=utf-8",
            "application/ld+json",
        ] {
            assert!(content_type_claims_json(claimed), "{claimed}");
        }

        // metadata does arrive under all of these, so none of them may be
        // the reason a body is turned away
        for unclaimed in [
            "text/plain; charset=utf-8",
            "binary/octet-stream",
            "application/octet-stream",
            "text/html; charset=utf-8",
            "",
        ] {
            assert!(!content_type_claims_json(unclaimed), "{unclaimed}");
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);

        std::env::temp_dir().join(format!(
            "dolos-offchain-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn store_keeps_verified_bodies_only() {
        let root = temp_root("verified");
        let store = OffchainStore::new(&root, 1024 * 1024);

        let body = br#"{"body":"hello"}"#.to_vec();
        let hash = Hasher::<256>::hash(&body);

        assert_eq!(store.read(hash).await, None);

        store.write(hash, body.clone()).await;
        assert_eq!(store.read(hash).await, Some(body.clone()));

        // a body that no longer matches its name is not served
        std::fs::write(store.path(&hash), b"tampered").unwrap();
        assert_eq!(store.read(hash).await, None);

        std::fs::remove_dir_all(root).unwrap();
    }

    /// The store is a cache beside the state and archive data, so it has to
    /// stay inside its budget however many anchors the chain carries.
    #[tokio::test]
    async fn store_evicts_the_oldest_bodies_to_stay_inside_its_budget() {
        let root = temp_root("budget");
        // room for two bodies, and the walk takes it a fifth under
        let store = OffchainStore::new(&root, 2048);

        let mut written = vec![];

        for (age, byte) in [(4u64, b'a'), (3, b'b'), (2, b'c'), (1, b'd')] {
            let body = vec![byte; 700];
            let hash = Hasher::<256>::hash(&body);
            store.write(hash, body).await;

            // one file per second apart, oldest first, since a filesystem
            // may not separate four writes in the same instant
            let file = std::fs::File::options()
                .write(true)
                .open(store.path(&hash))
                .unwrap();
            file.set_modified(SystemTime::now() - Duration::from_secs(age))
                .unwrap();

            written.push(hash);
        }

        store.enforce_budget().await;

        let kept: Vec<bool> =
            futures::future::join_all(written.iter().map(|hash| store.read(*hash)))
                .await
                .into_iter()
                .map(|body| body.is_some())
                .collect();

        assert_eq!(kept, vec![false, false, true, true], "oldest go first");

        let total: u64 = std::fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.metadata().unwrap().len())
            .sum();

        assert!(total <= 2048, "{total} bytes left behind");

        std::fs::remove_dir_all(root).unwrap();
    }

    /// A zero budget is the way to keep metadata out of the data directory
    /// entirely; the in-process cache still answers.
    #[tokio::test]
    async fn a_zero_budget_writes_nothing_to_disk() {
        let root = temp_root("nodisk");
        let store = OffchainStore::new(&root, 0);

        let body = b"{}".to_vec();
        let hash = Hasher::<256>::hash(&body);

        store.write(hash, body).await;

        assert!(store.read(hash).await.is_none());
        assert!(!root.join(OffchainStore::DIR).exists());
    }

    #[test]
    fn verified_bodies_render_json_or_a_decode_error() {
        let out = DrepsInnerMetadata {
            url: "https://x.io/d.json".to_string(),
            hash: String::new(),
            json_metadata: None,
            bytes: None,
            error: None,
        };

        let json = verified(out.clone(), br#"{"a":1}"#);
        assert_eq!(json.json_metadata, Some(serde_json::json!({"a": 1})));
        assert_eq!(json.bytes.as_deref(), Some("\\x7b2261223a317d"));
        assert!(json.error.is_none());

        let broken = verified(out, b"not json");
        assert_eq!(broken.json_metadata, None);
        assert_eq!(broken.bytes, None);
        assert_eq!(broken.error.unwrap().code, MetadataError::DecodeError);
    }

    fn metadata(url: &str, error: Option<DrepsInnerMetadataError>) -> DrepsInnerMetadata {
        DrepsInnerMetadata {
            url: url.to_string(),
            hash: String::new(),
            json_metadata: None,
            bytes: Some("\\x00".repeat(4)),
            error: error.map(Box::new),
        }
    }

    fn key(url: &str) -> CacheKey {
        (url.to_string(), Hash::<32>::from([0u8; 32]))
    }

    #[test]
    fn cache_keeps_verified_fetches_and_expires_failed_ones() {
        let mut cache = MetadataCache::new(16, usize::MAX);
        let now = Instant::now();
        let later = now + FAILURE_TTL + Duration::from_secs(1);

        cache.insert(key("ok"), metadata("ok", None), now);
        cache.insert(
            key("bad"),
            metadata("bad", Some(connection_error("bad"))),
            now,
        );

        assert!(cache.get(&key("ok"), now).is_some());
        assert!(cache.get(&key("bad"), now).is_some());
        assert!(cache.get(&key("ok"), later).is_some());
        assert!(cache.get(&key("bad"), later).is_none());
        assert!(cache.get(&key("missing"), now).is_none());
    }

    #[test]
    fn cache_evicts_oldest_past_its_caps() {
        let now = Instant::now();

        let mut by_count = MetadataCache::new(2, usize::MAX);
        for url in ["a", "b", "c"] {
            by_count.insert(key(url), metadata(url, None), now);
        }
        assert!(by_count.get(&key("a"), now).is_none());
        assert!(by_count.get(&key("b"), now).is_some());
        assert!(by_count.get(&key("c"), now).is_some());

        // each entry weighs its url plus 16 bytes of hex
        let mut by_bytes = MetadataCache::new(usize::MAX, 40);
        for url in ["a", "b", "c"] {
            by_bytes.insert(key(url), metadata(url, None), now);
        }
        assert!(by_bytes.get(&key("a"), now).is_none());
        assert!(by_bytes.get(&key("b"), now).is_some());
        assert!(by_bytes.get(&key("c"), now).is_some());
        assert_eq!(by_bytes.bytes, 34);

        // replacing an entry swaps its weight instead of adding to it
        by_bytes.insert(key("c"), metadata("c", None), now);
        assert_eq!(by_bytes.bytes, 34);
        assert_eq!(by_bytes.entries.len(), by_bytes.order.len());
    }
}
