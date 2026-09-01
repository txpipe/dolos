//! Acquiring immutable chain data from a mithril aggregator.
//!
//! One aggregator client, one download plan, one verification, for both
//! callers that fetch: the daemon in [`backfill`](crate::backfill), and the
//! `dolos bootstrap mithril` command that wraps [`fetch_snapshot`] in a
//! runtime of its own.
//!
//! Rendering is the binary's. The client reports through `mithril_client`'s
//! own [`FeedbackReceiver`], which arrives here as an argument and is `None`
//! for a caller with nothing to draw.

use std::path::Path;
use std::sync::Arc;

use dolos_core::config::MithrilConfig;
use mithril_client::cardano_database_client::{DownloadUnpackOptions, ImmutableFileRange};
use mithril_client::feedback::FeedbackReceiver;
use mithril_client::{
    AggregatorDiscoveryType, ClientBuilder, MessageBuilder, MithrilError, MithrilResult,
};
use tracing::{info, warn};

/// One download round, freed of the CLI that spelled it.
pub struct Fetch<'a> {
    /// Directory the snapshot is unpacked into; the immutable files land in
    /// its `immutable` subdirectory.
    pub download_dir: &'a Path,

    /// Skip the digest and merkle validation. The certificate chain is still
    /// verified.
    pub skip_validation: bool,

    /// Download from this immutable file number, inclusive.
    pub download_start: Option<u64>,

    /// Download up to this immutable file number, inclusive.
    pub download_end: Option<u64>,
}

/// Scan the immutable directory for the highest immutable file number present.
pub fn highest_existing_immutable(immutable_dir: &Path) -> Option<u64> {
    let entries = std::fs::read_dir(immutable_dir).ok()?;
    let mut max: Option<u64> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(num_str) = name.split('.').next().and_then(|s| s.parse::<u64>().ok()) {
            max = Some(max.map_or(num_str, |m| m.max(num_str)));
        }
    }
    max
}

/// Ranges of immutable files to download and to verify.
struct DownloadPlan {
    /// Range to download, if any. `None` when files on disk already cover the
    /// snapshot.
    download: Option<ImmutableFileRange>,
    /// Range to verify against the certificate.
    verify: ImmutableFileRange,
}

/// Build the explicit range the caller asked for, if any.
fn explicit_range(fetch: &Fetch<'_>) -> Option<ImmutableFileRange> {
    match (fetch.download_start, fetch.download_end) {
        (Some(start), Some(end)) => Some(ImmutableFileRange::Range(start, end)),
        (Some(start), None) => Some(ImmutableFileRange::From(start)),
        (None, Some(end)) => Some(ImmutableFileRange::UpTo(end)),
        (None, None) => None,
    }
}

/// Compute the download & verification plan based on the caller's range,
/// existing files on disk and the snapshot's last immutable file number.
///
/// When an explicit range is given, both download and verification are scoped
/// to it. Otherwise the full range is verified; if immutables already exist
/// locally, the download resumes from the highest file present. The highest
/// file is re-fetched (not skipped) because an interrupted run may have left
/// it truncated, and it's never verified until this run completes.
fn plan_download(fetch: &Fetch<'_>, immutable_dir: &Path, last_immutable: u64) -> DownloadPlan {
    if let Some(verify) = explicit_range(fetch) {
        return DownloadPlan {
            download: explicit_range(fetch),
            verify,
        };
    }

    let download = match highest_existing_immutable(immutable_dir) {
        Some(highest) if highest > last_immutable => {
            info!(
                highest,
                last_immutable, "local immutable files already cover the snapshot"
            );
            None
        }
        Some(highest) => {
            info!(highest, "resuming download from immutable file {highest}");
            Some(ImmutableFileRange::From(highest))
        }
        None => Some(ImmutableFileRange::Full),
    };

    DownloadPlan {
        download,
        verify: ImmutableFileRange::Full,
    }
}

/// One aggregator client configuration, shared by the fetch and the beacon
/// query so the two cannot drift apart on discovery or key handling.
fn client_builder(config: &MithrilConfig) -> ClientBuilder {
    ClientBuilder::new(AggregatorDiscoveryType::Url(config.aggregator.clone()))
        .set_genesis_verification_key(mithril_client::GenesisVerificationKey::JsonHex(
            config.genesis_key.clone(),
        ))
}

/// The highest immutable file number any aggregator snapshot covers.
///
/// What `snapshot backfill` sizes its next download window against, and how it
/// knows the aggregator has nothing past the files already on disk.
pub async fn latest_immutable_file(config: &MithrilConfig) -> MithrilResult<u64> {
    let client = client_builder(config).build()?;

    let snapshots = client.cardano_database_v2().list().await?;

    snapshots
        .iter()
        .map(|snapshot| snapshot.beacon.immutable_file_number)
        .max()
        .ok_or(MithrilError::msg("no snapshot available"))
}

/// Download and verify the aggregator's latest snapshot, over the range
/// `fetch` names.
pub async fn fetch_snapshot(
    fetch: &Fetch<'_>,
    config: &MithrilConfig,
    feedback: Option<Arc<dyn FeedbackReceiver>>,
) -> MithrilResult<()> {
    let mut builder = client_builder(config);

    if let Some(feedback) = feedback {
        builder = builder.add_feedback_receiver(feedback);
    }

    let client = builder.build()?;

    let db_client = client.cardano_database_v2();

    let snapshots = db_client.list().await?;

    let last_digest = snapshots
        .iter()
        .max_by_key(|s| s.beacon.immutable_file_number)
        .ok_or(MithrilError::msg("no snapshot available"))?
        .hash
        .as_str();

    let snapshot = db_client
        .get(last_digest)
        .await?
        .ok_or(MithrilError::msg("no snapshot available"))?;

    let certificate = client
        .certificate()
        .verify_chain(&snapshot.certificate_hash)
        .await?;

    let target_directory = fetch.download_dir;
    let immutable_dir = target_directory.join("immutable");

    let last_immutable = snapshot.beacon.immutable_file_number;
    let plan = plan_download(fetch, &immutable_dir, last_immutable);

    if let Some(immutable_range) = &plan.download {
        let download_opts = DownloadUnpackOptions {
            allow_override: true,
            include_ancillary: false,
            ..DownloadUnpackOptions::default()
        };

        db_client
            .download_unpack(&snapshot, immutable_range, target_directory, download_opts)
            .await?;

        let nb_files = immutable_range.length(last_immutable);

        if let Err(e) = db_client
            .add_statistics(
                *immutable_range == ImmutableFileRange::Full,
                false,
                nb_files,
            )
            .await
        {
            warn!("failed incrementing snapshot download statistics: {:?}", e);
        }
    }

    if !fetch.skip_validation {
        let verified_digests = db_client
            .download_and_verify_digests(&certificate, &snapshot)
            .await?;

        let merkle_proof = db_client
            .verify_cardano_database(
                &certificate,
                &snapshot,
                &plan.verify,
                false,
                target_directory,
                &verified_digests,
            )
            .await
            .map_err(|e| MithrilError::msg(format!("verification failed: {e:?}")))?;

        let message = MessageBuilder::new()
            .compute_cardano_database_message(&certificate, &merkle_proof)
            .await?;

        if !certificate.match_message(&message) {
            return Err(MithrilError::msg(
                "mithril certificate does not match the downloaded snapshot",
            ));
        }
    } else {
        warn!("skipping validation, assuming snapshot is already validated");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREVIEW_GENESIS_KEY: &str = "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d";

    fn fetch_with_range(dir: &Path, start: Option<u64>, end: Option<u64>) -> Fetch<'_> {
        Fetch {
            download_dir: dir,
            skip_validation: false,
            download_start: start,
            download_end: end,
        }
    }

    fn touch_immutables(dir: &Path, numbers: impl IntoIterator<Item = u64>) {
        for n in numbers {
            for ext in ["chunk", "primary", "secondary"] {
                std::fs::write(dir.join(format!("{n:05}.{ext}")), []).unwrap();
            }
        }
    }

    #[test]
    fn plan_uses_explicit_range_for_download_and_verify() {
        let dir = tempfile::tempdir().unwrap();

        let plan = plan_download(
            &fetch_with_range(dir.path(), Some(5), Some(8)),
            dir.path(),
            10,
        );
        assert_eq!(plan.download, Some(ImmutableFileRange::Range(5, 8)));
        assert_eq!(plan.verify, ImmutableFileRange::Range(5, 8));

        let plan = plan_download(&fetch_with_range(dir.path(), Some(5), None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(5)));
        assert_eq!(plan.verify, ImmutableFileRange::From(5));

        let plan = plan_download(&fetch_with_range(dir.path(), None, Some(8)), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::UpTo(8)));
        assert_eq!(plan.verify, ImmutableFileRange::UpTo(8));
    }

    #[test]
    fn plan_downloads_and_verifies_full_on_fresh_dir() {
        let dir = tempfile::tempdir().unwrap();

        let plan = plan_download(&fetch_with_range(dir.path(), None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::Full));
        assert_eq!(plan.verify, ImmutableFileRange::Full);

        // a missing dir behaves like a fresh one
        let plan = plan_download(
            &fetch_with_range(dir.path(), None, None),
            &dir.path().join("nope"),
            10,
        );
        assert_eq!(plan.download, Some(ImmutableFileRange::Full));
    }

    #[test]
    fn plan_resumes_from_highest_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=4);

        // the highest file is re-fetched (not skipped): an interrupted run may
        // have left it truncated
        let plan = plan_download(&fetch_with_range(dir.path(), None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(4)));
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn plan_refetches_boundary_file_when_download_complete() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=10);

        // highest == last must not produce an out-of-bounds range (From(11)
        // would make the mithril client fail with "invalid immutable file
        // range" when resuming after a crash during import)
        let plan = plan_download(&fetch_with_range(dir.path(), None, None), dir.path(), 10);
        assert_eq!(plan.download, Some(ImmutableFileRange::From(10)));
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn plan_skips_download_when_local_files_exceed_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), 0..=11);

        let plan = plan_download(&fetch_with_range(dir.path(), None, None), dir.path(), 10);
        assert_eq!(plan.download, None);
        assert_eq!(plan.verify, ImmutableFileRange::Full);
    }

    #[test]
    fn highest_existing_ignores_non_numeric_files() {
        let dir = tempfile::tempdir().unwrap();
        touch_immutables(dir.path(), [0, 3]);
        std::fs::write(dir.path().join("lock"), []).unwrap();
        std::fs::write(dir.path().join("clean"), []).unwrap();

        assert_eq!(highest_existing_immutable(dir.path()), Some(3));
    }

    #[test]
    fn mithril_client_builds_with_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let client = ClientBuilder::new(AggregatorDiscoveryType::Url(
            "https://aggregator.example.com".into(),
        ))
        .set_genesis_verification_key(mithril_client::GenesisVerificationKey::JsonHex(
            PREVIEW_GENESIS_KEY.into(),
        ))
        .build();

        assert!(client.is_ok());
    }
}
