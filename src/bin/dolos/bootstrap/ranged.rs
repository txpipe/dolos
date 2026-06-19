//! Ranged ring-buffer downloader for bootstrap snapshots.
//!
//! Instead of streaming a single long-lived HTTP response directly into the tar
//! extractor (which couples the connection lifetime to disk-write backpressure),
//! this downloads the snapshot in bounded byte ranges. A background thread keeps
//! a small, fixed-size window of chunks staged on disk ahead of the extractor;
//! when extraction falls behind, the downloader simply stops issuing range
//! requests rather than holding a connection open and idle.
//!
//! This matters for Cloudflare R2, which tears down long-lived / slow-drained
//! streamed responses where S3 tolerated them. Each range request here is
//! short-lived (one chunk drained at line rate), so the server never sees a
//! stalled connection. As a bonus, individual chunks are retried on failure,
//! making the download resilient to transient errors.

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use miette::{Context, IntoDiagnostic};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, RANGE};

use crate::feedback::ProgressBar;

/// Size of each ranged request. The connection for a single chunk stays open
/// only as long as it takes to drain this many bytes at line rate.
const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Number of downloaded-but-not-yet-extracted chunks allowed to exist on disk
/// at once. Bounds the staging footprint to `CHUNK_SIZE * WINDOW_PARTS`.
const WINDOW_PARTS: usize = 4;

/// Overall per-chunk request timeout. Because each request is bounded to a
/// single `CHUNK_SIZE` chunk, an overall timeout is safe here (unlike a single
/// full-body stream, where it would cap the whole multi-GB transfer). A chunk
/// that stalls — the failure mode that broke the old R2 download — trips this
/// and is retried, rather than hanging the whole bootstrap. Sized so a 64 MiB
/// chunk completes even on a slow (~0.5 MB/s) link.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(150);

/// Connection establishment timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of times to retry a single failing chunk before giving up.
const MAX_RETRIES: u32 = 5;

/// Result of probing the remote URL for range support.
pub struct RangeProbe {
    pub total_size: u64,
    pub supports_ranges: bool,
}

/// Build an HTTP client suitable for ranged downloads. Because every request is
/// bounded to a single chunk, an overall request timeout is safe here (unlike a
/// single full-body stream, where it would cap the entire multi-GB transfer).
pub fn build_client() -> miette::Result<Client> {
    Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(CHUNK_TIMEOUT)
        .build()
        .into_diagnostic()
        .context("Failed to build HTTP client")
}

/// Probe the URL with a HEAD request to learn its size and whether it advertises
/// `Accept-Ranges: bytes`.
pub fn probe(client: &Client, url: &str) -> miette::Result<RangeProbe> {
    let response = client
        .head(url)
        .send()
        .into_diagnostic()
        .context("Failed to probe snapshot URL")?
        .error_for_status()
        .into_diagnostic()
        .context("Failed to probe snapshot URL")?;

    let supports_ranges = response
        .headers()
        .get(ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("bytes"))
        .unwrap_or(false);

    let total_size = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(RangeProbe {
        total_size,
        supports_ranges,
    })
}

/// Message handed from the downloader thread to the reader. A path points at a
/// staged chunk file ready to be consumed; an error aborts the read.
type ChunkMsg = io::Result<PathBuf>;

/// A `Read` implementation backed by a bounded, disk-staged ring of ranged
/// chunks. Reading consumes staged chunk files in order, deleting each one and
/// returning a permit (which lets the downloader fetch one more chunk) as it is
/// exhausted.
pub struct RangedReader {
    data_rx: Receiver<ChunkMsg>,
    // `Option` so `Drop` can release the permit sender *before* draining the data
    // channel, which is what lets a downloader blocked on `permits_rx.recv()`
    // unwind instead of deadlocking.
    permits_tx: Option<SyncSender<()>>,
    current: Option<(BufReader<File>, PathBuf)>,
    handle: Option<JoinHandle<()>>,
}

impl RangedReader {
    fn finish_current(&mut self) {
        if let Some((_, path)) = self.current.take() {
            let _ = std::fs::remove_file(&path);
            // Returning a permit lets the downloader stage one more chunk. If the
            // downloader is already gone, the send simply fails and we ignore it.
            if let Some(tx) = self.permits_tx.as_ref() {
                let _ = tx.send(());
            }
        }
    }
}

impl Read for RangedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some((reader, _)) = self.current.as_mut() {
                let n = reader.read(buf)?;
                if n > 0 {
                    return Ok(n);
                }
                // Current chunk exhausted: delete it, free a permit, fetch next.
                self.finish_current();
                continue;
            }

            match self.data_rx.recv() {
                Ok(Ok(path)) => {
                    let file = File::open(&path)?;
                    self.current = Some((BufReader::new(file), path));
                    continue;
                }
                // Downloader surfaced an error for this chunk.
                Ok(Err(e)) => return Err(e),
                // Sender dropped with no error: clean end of stream.
                Err(_) => return Ok(0),
            }
        }
    }
}

impl Drop for RangedReader {
    fn drop(&mut self) {
        // Remove any chunk we were mid-read on.
        self.finish_current();

        // Release the permit sender first. A downloader blocked waiting for a
        // free slot will see `permits_rx.recv()` fail and wind down; one blocked
        // on `data_tx.send()` unblocks as we drain below. Either way it stops
        // issuing requests and eventually drops `data_tx`, ending the drain.
        self.permits_tx = None;

        // Drain and delete any chunks already staged so nothing lingers, then
        // join the now-terminating downloader thread.
        while let Ok(Ok(path)) = self.data_rx.recv() {
            let _ = std::fs::remove_file(&path);
        }

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Download a single byte range `[start, end]` into `path`, retrying transient
/// failures. Progress is advanced by the chunk length only on success, so a
/// retried chunk is never double-counted.
fn download_chunk(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
    path: &Path,
    progress: &ProgressBar,
) -> io::Result<()> {
    let range = format!("bytes={start}-{end}");
    let mut attempt = 0;

    loop {
        match try_download_chunk(client, url, &range, path) {
            Ok(()) => {
                progress.inc(end - start + 1);
                return Ok(());
            }
            Err(e) => {
                // Clean up any partial file before retrying.
                let _ = std::fs::remove_file(path);

                if attempt >= MAX_RETRIES {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("range {range} failed after {MAX_RETRIES} retries: {e}"),
                    ));
                }

                // Exponential backoff, capped at 16s.
                let backoff = Duration::from_millis(500 * (1u64 << attempt.min(5)));
                std::thread::sleep(backoff);
                attempt += 1;
            }
        }
    }
}

fn try_download_chunk(client: &Client, url: &str, range: &str, path: &Path) -> io::Result<()> {
    let mut response = client
        .get(url)
        .header(RANGE, range)
        .send()
        .and_then(|r| r.error_for_status())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let mut file = File::create(path)?;
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = response
            .read(&mut buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
    }

    file.flush()?;
    Ok(())
}

/// Spawn the background downloader and return a reader over the staged chunks.
///
/// `staging` must be an existing, dedicated directory on the same filesystem as
/// the extraction target; chunk files are created and removed inside it.
pub fn ranged_reader(
    client: Client,
    url: String,
    total_size: u64,
    staging: PathBuf,
    progress: ProgressBar,
) -> RangedReader {
    ranged_reader_with_chunk(client, url, total_size, CHUNK_SIZE, staging, progress)
}

/// Like [`ranged_reader`] but with an explicit chunk size. Exposed mainly so
/// tests can exercise multi-chunk flow and the staging window without moving
/// hundreds of megabytes.
pub fn ranged_reader_with_chunk(
    client: Client,
    url: String,
    total_size: u64,
    chunk_size: u64,
    staging: PathBuf,
    progress: ProgressBar,
) -> RangedReader {
    let (data_tx, data_rx) = sync_channel::<ChunkMsg>(WINDOW_PARTS);
    let (permits_tx, permits_rx) = sync_channel::<()>(WINDOW_PARTS);

    // Pre-fill the permit pool: at most WINDOW_PARTS chunks may be staged on
    // disk ahead of the extractor at any time.
    for _ in 0..WINDOW_PARTS {
        permits_tx.send(()).expect("permit channel just created");
    }

    let handle = std::thread::spawn(move || {
        let mut offset = 0u64;
        let mut idx = 0u64;

        while offset < total_size {
            // Backpressure point: block here, with NO connection open, until the
            // extractor frees a slot. This is what keeps R2 from ever seeing an
            // idle/slow-drained connection.
            if permits_rx.recv().is_err() {
                // Reader dropped; stop downloading.
                return;
            }

            let len = chunk_size.min(total_size - offset);
            let end = offset + len - 1;
            let path = staging.join(format!("part-{idx:08}.chunk"));

            match download_chunk(&client, &url, offset, end, &path, &progress) {
                Ok(()) => {
                    if data_tx.send(Ok(path)).is_err() {
                        // Reader dropped; the staged file will be cleaned up by
                        // the caller's staging-dir teardown.
                        return;
                    }
                }
                Err(e) => {
                    let _ = data_tx.send(Err(e));
                    return;
                }
            }

            offset += len;
            idx += 1;
        }
        // Loop exhausted: dropping `data_tx` signals clean EOF to the reader.
    });

    RangedReader {
        data_rx,
        permits_tx: Some(permits_tx),
        current: None,
        handle: Some(handle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_URL: &str =
        "https://dolos-snapshots.txpipe.cloud/v3/764824073/full/latest.tar.gz";

    /// End-to-end check against the real R2 endpoint: download a small prefix in
    /// many small chunks through the ring buffer and confirm the bytes match a
    /// single plain ranged GET of the same prefix. Ignored by default (network +
    /// ~5 MB transfer); run with:
    ///   cargo test --bin dolos ranged_matches_direct_fetch -- --ignored --nocapture
    #[test]
    #[ignore]
    fn ranged_matches_direct_fetch() {
        // 5 MiB total in 1 MiB chunks => 5 chunks, exercising the 4-part window
        // (so the downloader must block on a permit at least once) and a final
        // short chunk.
        const TOTAL: u64 = 5 * 1024 * 1024;
        const CHUNK: u64 = 1024 * 1024;

        let client = build_client().expect("client");

        let probe = probe(&client, TEST_URL).expect("probe");
        assert!(probe.supports_ranges, "endpoint must advertise ranges");
        assert!(probe.total_size >= TOTAL, "remote smaller than test window");

        // Reference: one plain ranged GET of the same prefix.
        let expected = client
            .get(TEST_URL)
            .header(RANGE, format!("bytes=0-{}", TOTAL - 1))
            .send()
            .and_then(|r| r.error_for_status())
            .expect("reference fetch")
            .bytes()
            .expect("reference body");
        assert_eq!(expected.len() as u64, TOTAL);

        // Under test: pull the same prefix through the ring buffer.
        let staging = std::env::temp_dir().join("dolos-ranged-test");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging).expect("staging");

        let progress = ProgressBar::hidden();
        let mut reader = ranged_reader_with_chunk(
            client,
            TEST_URL.to_string(),
            TOTAL,
            CHUNK,
            staging.clone(),
            progress.clone(),
        );

        let mut actual = Vec::new();
        let n = reader.read_to_end(&mut actual).expect("read ring buffer");

        assert_eq!(n as u64, TOTAL, "total byte count mismatch");
        assert_eq!(actual.len(), expected.len(), "length mismatch");
        assert!(actual == expected.as_ref(), "byte content mismatch");
        assert_eq!(progress.position(), TOTAL, "progress should equal total");

        drop(reader);

        // Staging dir should be empty of chunk files after a full, clean read.
        let leftover: Vec<_> = std::fs::read_dir(&staging)
            .expect("read staging")
            .filter_map(Result::ok)
            .collect();
        assert!(leftover.is_empty(), "staging not cleaned: {leftover:?}");

        let _ = std::fs::remove_dir_all(&staging);
    }
}
