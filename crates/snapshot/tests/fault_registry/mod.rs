//! An in-process OCI registry that can cut off the blobs it serves.
//!
//! Enough of the distribution API for a publish and a restore over plaintext
//! HTTP: monolithic blob uploads, blob `HEAD`/`GET`, manifest `PUT`/`GET`. A
//! scripted fault cuts a blob `GET` off partway, or answers a manifest `GET`
//! with a `503`, and a layer blob can wait before its first byte, for a time
//! or for another layer's `GET`. No container, so the suites using it run
//! under plain `cargo test`.
//!
//! **A trimmed copy of `stelae/tests/fault_registry`**, for the reason
//! `registry_fixture` gives: `stelae` must never depend on a `dolos-*` package,
//! so a fixture both suites import would live somewhere neither crate owns.
//!
//! Every response closes its connection, so each attempt the client makes is a
//! fresh request that this server sees and counts.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use stelae::Digest;

#[derive(Default)]
struct Script {
    once: usize,
    always: bool,
}

impl Script {
    /// Whether the next `GET` is cut off.
    fn next(&mut self) -> bool {
        if self.once > 0 {
            self.once -= 1;
            return true;
        }

        self.always
    }
}

#[derive(Default)]
struct State {
    blobs: HashMap<String, Vec<u8>>,
    manifests: HashMap<String, (String, Vec<u8>)>,
    /// Layer digests of the last manifest stored, in manifest order.
    layers: Vec<String>,
    /// Config blob digest of the last manifest stored.
    config: Option<String>,
    /// Manifest `GET`s still to be answered with a `503`.
    manifest_faults: usize,
    /// Manifest `GET`s still to be refused with a `403`, ahead of the `503`s.
    manifest_denials: usize,
    /// Manifest `GET`s received.
    manifest_gets: usize,
    /// Cut-offs keyed by blob digest.
    faults: HashMap<String, Script>,
    /// `GET`s per blob digest.
    gets: HashMap<String, usize>,
    uploads: usize,
    /// The wait before a layer blob's first byte, by digest.
    delays: HashMap<String, Duration>,
    /// Layer blobs whose first byte waits for another layer's `GET` to arrive.
    holds: HashMap<String, String>,
    /// Layer blob `GET`s in the order they arrived, and in the order their
    /// answers were done.
    arrived: Vec<String>,
    served: Vec<String>,
    /// Layer blob `GET`s being answered now, and the most there were at once.
    active: usize,
    peak: usize,
}

pub struct FaultRegistry {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl FaultRegistry {
    pub fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));

        let serving = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let state = Arc::clone(&serving);
                std::thread::spawn(move || {
                    let _ = handle(stream, &state);
                });
            }
        });

        Self { address, state }
    }

    /// The repository `name` on this registry, as a node names one.
    pub fn repository(&self, name: &str) -> stelae::oci::Repository {
        format!("oci://{}/{name}", self.address)
            .parse()
            .expect("a usable repository")
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The layer blob digests of the manifest last pushed.
    pub fn layers(&self) -> Vec<String> {
        self.state().layers.clone()
    }

    /// The config blob digest of the manifest last pushed.
    pub fn config(&self) -> String {
        self.state().config.clone().expect("nothing was published")
    }

    /// Answer the next `times` manifest `GET`s with a `503`.
    pub fn fail_next_manifest(&self, times: usize) {
        self.state().manifest_faults += times;
    }

    /// Refuse the next `times` manifest `GET`s with a `403` and a `DENIED`
    /// error, as a registry turning a token away answers.
    pub fn deny_next_manifest(&self, times: usize) {
        self.state().manifest_denials += times;
    }

    /// How many manifest `GET`s the registry received.
    pub fn manifest_gets(&self) -> usize {
        self.state().manifest_gets
    }

    /// The largest layer blob of the manifest last pushed: the one a cut-off
    /// can split furthest from either end.
    pub fn largest_layer(&self) -> String {
        let state = self.state();

        state
            .layers
            .iter()
            .max_by_key(|digest| state.blobs.get(*digest).map_or(0, Vec::len))
            .cloned()
            .expect("nothing was published")
    }

    /// Cut off the next `times` `GET`s of the blob `digest`.
    pub fn cut_next(&self, digest: &str, times: usize) {
        self.state()
            .faults
            .entry(digest.to_owned())
            .or_default()
            .once += times;
    }

    /// Cut off every `GET` of the blob `digest`.
    pub fn cut_always(&self, digest: &str) {
        self.state()
            .faults
            .entry(digest.to_owned())
            .or_default()
            .always = true;
    }

    /// How many `GET`s the blob `digest` received.
    pub fn gets(&self, digest: &str) -> usize {
        self.state().gets.get(digest).copied().unwrap_or(0)
    }

    /// Wait `delay` before the first byte of every `GET` of these blobs.
    pub fn delay(&self, digests: &[String], delay: Duration) {
        let mut state = self.state();
        for digest in digests {
            state.delays.insert(digest.clone(), delay);
        }
    }

    /// Hold the first byte of every `GET` of the blob `digest` until a `GET`
    /// of the layer blob `until` has arrived: an order between two requests a
    /// test needs, rather than a wait it hopes is long enough.
    pub fn hold(&self, digest: &str, until: &str) {
        self.state()
            .holds
            .insert(digest.to_owned(), until.to_owned());
    }

    /// Layer blob `GET`s in the order they arrived.
    pub fn arrived(&self) -> Vec<String> {
        self.state().arrived.clone()
    }

    /// Layer blob `GET`s in the order their answers were done.
    pub fn served(&self) -> Vec<String> {
        self.state().served.clone()
    }

    /// The most layer blob `GET`s answered at once.
    pub fn peak(&self) -> usize {
        self.state().peak
    }

    /// Forget the layer `GET`s logged so far, and the peak.
    pub fn forget(&self) {
        let mut state = self.state();
        state.arrived.clear();
        state.served.clear();
        state.peak = state.active;
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &TcpStream) -> std::io::Result<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));

    let mut headers = HashMap::new();
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let mut body = Vec::new();
    if let Some(length) = headers.get("content-length") {
        body.resize(length.parse().unwrap_or(0), 0);
        reader.read_exact(&mut body)?;
    } else if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size)?;
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk)?;
            if size == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..size]);
        }
    }

    Ok(Request {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        headers,
        body,
    })
}

/// Answer `status`, announcing `length` bytes and sending `body`, then close.
fn respond(
    mut stream: &TcpStream,
    status: u16,
    headers: &[(&str, String)],
    length: usize,
    body: &[u8],
) -> std::io::Result<()> {
    let mut head =
        format!("HTTP/1.1 {status} X\r\nConnection: close\r\nContent-Length: {length}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");

    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Both)
}

fn sha256(bytes: &[u8]) -> String {
    Digest::compute(bytes).to_string()
}

fn decode(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| v.replace("%3A", ":").replace("%3a", ":"))
    })
}

fn handle(stream: TcpStream, state: &Mutex<State>) -> std::io::Result<()> {
    let request = read_request(&stream)?;
    let lock = || state.lock().unwrap_or_else(|p| p.into_inner());
    let path = request.path.as_str();

    if path == "/v2/" || path == "/v2" {
        return respond(&stream, 200, &[], 2, b"{}");
    }

    let Some(rest) = path.strip_prefix("/v2/") else {
        return respond(&stream, 404, &[], 0, b"");
    };

    if let Some(index) = rest.find("/blobs/uploads/") {
        let repository = &rest[..index];
        return match request.method.as_str() {
            "POST" => {
                let id = {
                    let mut state = lock();
                    state.uploads += 1;
                    state.uploads
                };
                let location = format!("/v2/{repository}/blobs/uploads/{id}");
                respond(&stream, 202, &[("Location", location)], 0, b"")
            }
            "PUT" => {
                let digest = decode(&request.query, "digest").unwrap_or_default();
                lock().blobs.insert(digest.clone(), request.body);
                let location = format!("/v2/{repository}/blobs/{digest}");
                respond(&stream, 201, &[("Location", location)], 0, b"")
            }
            _ => respond(&stream, 405, &[], 0, b""),
        };
    }

    if let Some(index) = rest.find("/blobs/") {
        let digest = rest[index + "/blobs/".len()..].to_owned();
        let head = request.method == "HEAD";

        let (blob, cut) = {
            let mut state = lock();
            let blob = state.blobs.get(&digest).cloned();

            let cut = !head && {
                *state.gets.entry(digest.clone()).or_default() += 1;
                state.faults.get_mut(&digest).is_some_and(Script::next)
            };

            (blob, cut)
        };

        // Only layer `GET`s are logged and delayed, so a test reads the order a
        // restore fetched its layers in without the config blob in it.
        let layer = !head && lock().layers.contains(&digest);
        if layer {
            let (delay, hold) = {
                let mut state = lock();
                state.arrived.push(digest.clone());
                state.active += 1;
                state.peak = state.peak.max(state.active);
                (
                    state.delays.get(&digest).copied(),
                    state.holds.get(&digest).cloned(),
                )
            };

            // Bounded, so a test whose other request never comes fails on its
            // own assertion rather than hanging.
            if let Some(until) = hold {
                let deadline = Instant::now() + Duration::from_secs(30);
                while !lock().arrived.contains(&until) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }

            if let Some(delay) = delay {
                std::thread::sleep(delay);
            }
        }

        let answered = match blob {
            None => respond(&stream, 404, &[], 0, b""),
            Some(blob) if head => {
                let mut stream = &stream;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                    blob.len()
                );
                stream.write_all(head.as_bytes())?;
                stream.shutdown(std::net::Shutdown::Both)
            }
            // The whole length announced and half of it sent: a transfer that
            // dropped partway.
            Some(blob) if cut => respond(&stream, 200, &[], blob.len(), &blob[..blob.len() / 2]),
            Some(blob) => respond(&stream, 200, &[], blob.len(), &blob),
        };

        if layer {
            let mut state = lock();
            state.active -= 1;
            state.served.push(digest);
        }

        return answered;
    }

    if let Some(index) = rest.find("/manifests/") {
        let reference = rest[index + "/manifests/".len()..].to_owned();

        if request.method == "PUT" {
            let media_type = request
                .headers
                .get("content-type")
                .cloned()
                .unwrap_or_default();
            let digest = sha256(&request.body);
            let parsed: serde_json::Value = serde_json::from_slice(&request.body).unwrap();

            let mut state = lock();
            state.layers = parsed["layers"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|layer| layer["digest"].as_str().unwrap().to_owned())
                .collect();
            state.config = parsed["config"]["digest"].as_str().map(str::to_owned);
            let entry = (media_type, request.body);
            state.manifests.insert(digest.clone(), entry.clone());
            state.manifests.insert(reference, entry);
            drop(state);

            let location = format!("/v2/{}/manifests/{digest}", &rest[..index]);
            return respond(
                &stream,
                201,
                &[("Location", location), ("Docker-Content-Digest", digest)],
                0,
                b"",
            );
        }

        let (manifest, denied, failed) = {
            let mut state = lock();
            let get = request.method == "GET";

            if get {
                state.manifest_gets += 1;
            }

            let denied = get && state.manifest_denials > 0;
            let failed = get && !denied && state.manifest_faults > 0;

            if denied {
                state.manifest_denials -= 1;
            }

            if failed {
                state.manifest_faults -= 1;
            }

            (state.manifests.get(&reference).cloned(), denied, failed)
        };

        if denied {
            let body = br#"{"errors":[{"code":"DENIED","message":"denied"}]}"#;
            return respond(&stream, 403, &[], body.len(), body);
        }

        if failed {
            return respond(&stream, 503, &[], 0, b"");
        }

        return match manifest {
            None => {
                let body = br#"{"errors":[{"code":"MANIFEST_UNKNOWN","message":"unknown"}]}"#;
                respond(&stream, 404, &[], body.len(), body)
            }
            Some((media_type, body)) if request.method == "HEAD" => {
                let headers = [
                    ("Content-Type", media_type),
                    ("Docker-Content-Digest", sha256(&body)),
                ];
                respond(&stream, 200, &headers, body.len(), b"")
            }
            Some((media_type, body)) => {
                let headers = [
                    ("Content-Type", media_type),
                    ("Docker-Content-Digest", sha256(&body)),
                ];
                respond(&stream, 200, &headers, body.len(), &body)
            }
        };
    }

    respond(&stream, 404, &[], 0, b"")
}
