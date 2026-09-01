pub use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

use dolos_snapshot::progress::{Event, Observer, Outcome, Progress};

#[cfg(feature = "mithril")]
use dolos_mithril::mithril_client;

pub struct ProgressReader<R> {
    inner: R,
    progress: ProgressBar,
}

impl<R: std::io::Read> ProgressReader<R> {
    pub fn new(inner: R, progress: ProgressBar) -> Self {
        Self { inner, progress }
    }
}

impl<R: std::io::Read> std::io::Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = self.inner.read(buf)?;
        self.progress.inc(bytes_read as u64);
        Ok(bytes_read)
    }
}

pub struct Feedback {
    multi: Arc<MultiProgress>,
}

impl Feedback {
    pub fn multi_progress(&self) -> Arc<MultiProgress> {
        self.multi.clone()
    }

    pub fn indeterminate_progress_bar(&self) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}").unwrap(),
        );

        self.multi.add(pb)
    }

    pub fn slot_progress_bar(&self) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {per_sec:>7} slots/s (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        self.multi.add(pb)
    }

    pub fn bytes_progress_bar(&self) -> ProgressBar {
        let pb = ProgressBar::new_spinner();

        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        self.multi.add(pb)
    }
}

impl Feedback {
    /// A feedback surface that draws nothing.
    ///
    /// For the tests over the renderers built on it, which assert on the state
    /// a bar holds rather than on a terminal — and which would otherwise
    /// scribble over the test harness's own output.
    #[cfg(test)]
    pub fn hidden() -> Self {
        Self {
            multi: Arc::new(MultiProgress::with_draw_target(
                indicatif::ProgressDrawTarget::hidden(),
            )),
        }
    }
}

impl Default for Feedback {
    fn default() -> Self {
        let multi = Arc::new(MultiProgress::new());

        Self { multi }
    }
}

/// A stele transfer, drawn while it happens.
///
/// The one renderer for both directions, because both report through one seam
/// and an operator watching a publish and an operator watching a restore want
/// the same three things: which layer of how many, how much of the blob in
/// flight has moved, and that records are still going past. What differs
/// between the two commands is the words, which is
/// [`SteleProgress::publishing`] and [`SteleProgress::restoring`].
///
/// Three bars rather than one because the three move on entirely different
/// clocks: a layer boundary can be a minute apart on mainnet, a blob's bytes
/// tick continuously, and the record counter is the only thing that moves at
/// all during the epoch scan that dominates a publish.
pub struct SteleProgress {
    verb: &'static str,
    layers: ProgressBar,
    blob: ProgressBar,
    records: ProgressBar,
}

impl SteleProgress {
    /// The renderer `dolos snapshot publish` reports through.
    pub fn publishing(feedback: &Feedback) -> Arc<Self> {
        Self::new(feedback, "publishing")
    }

    /// The renderer `dolos bootstrap stelae` reports through.
    pub fn restoring(feedback: &Feedback) -> Arc<Self> {
        Self::new(feedback, "restoring")
    }

    fn new(feedback: &Feedback, verb: &'static str) -> Arc<Self> {
        let multi = feedback.multi_progress();

        let layers = multi.add(ProgressBar::new(0));
        layers.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>4}/{len:4} layers {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        let blob = multi.add(ProgressBar::new(0));
        blob.set_style(
            ProgressStyle::with_template(
                "  {bar:40.cyan/blue} {bytes}/{total_bytes} {binary_bytes_per_sec} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        let records = multi.add(ProgressBar::new_spinner());
        records.set_style(
            ProgressStyle::with_template("  {spinner:.green} {human_pos} records").unwrap(),
        );

        Arc::new(Self {
            verb,
            layers,
            blob,
            records,
        })
    }

    /// The handle a driver takes.
    pub fn observer(self: &Arc<Self>) -> Observer {
        Observer::new(self.clone())
    }

    /// Leave the bars saying what the run ended as.
    ///
    /// Called by the command once the driver returns, so that the summary it
    /// prints is not interleaved with a bar still claiming to be drawing.
    pub fn finish(&self) {
        self.blob.finish_and_clear();
        self.records.finish_and_clear();
        self.layers.finish_and_clear();
    }

    /// Where the bars ended up.
    ///
    /// The renderer's whole output is terminal state, so a test over it has to
    /// read that state; these are the accessors it reads. Test-only, because
    /// nothing in the program has a reason to ask a bar what it says.
    #[cfg(test)]
    pub fn layers_position(&self) -> u64 {
        self.layers.position()
    }

    #[cfg(test)]
    pub fn layers_length(&self) -> Option<u64> {
        self.layers.length()
    }

    #[cfg(test)]
    pub fn blob_position(&self) -> u64 {
        self.blob.position()
    }

    #[cfg(test)]
    pub fn blob_length(&self) -> Option<u64> {
        self.blob.length()
    }

    #[cfg(test)]
    pub fn records_position(&self) -> u64 {
        self.records.position()
    }

    /// How a layer is named to an operator: its kind, and whatever of the
    /// profile's scope identifies it.
    ///
    /// The scope is opaque to everything below the binary, so this reads the
    /// two keys this profile actually uses and falls back to the compact JSON
    /// rather than inventing a vocabulary the inscription does not have.
    fn describe(kind: &str, scope: &serde_json::Value) -> String {
        let field = |name: &str| scope.get(name).and_then(|v| v.as_u64());

        match (field("epoch"), field("shard")) {
            (Some(epoch), Some(shard)) => format!("{kind} epoch {epoch} shard {shard}"),
            (Some(epoch), None) => format!("{kind} epoch {epoch}"),
            (None, Some(shard)) => format!("{kind} shard {shard}"),
            (None, None) => format!("{kind} {scope}"),
        }
    }
}

impl Progress for SteleProgress {
    fn on(&self, event: Event<'_>) {
        match event {
            Event::LayerStarted {
                total, kind, scope, ..
            } => {
                self.layers.set_length(total as u64);
                self.layers
                    .set_message(format!("{} {}", self.verb, Self::describe(kind, scope)));
            }

            // Counted rather than positioned from the index: a driver may hold
            // several layers open at once and close them in whatever order its
            // sinks finish — the export's sixteen state shards do exactly that —
            // and a bar placed at `index + 1` would run backwards when it did.
            Event::LayerFinished {
                total,
                kind,
                outcome,
                ..
            } => {
                self.layers.set_length(total as u64);
                self.layers.inc(1);

                match outcome {
                    Outcome::Transferred => {}
                    Outcome::Skipped => self
                        .layers
                        .set_message(format!("skipped {kind}, already restored")),
                    Outcome::Inherited => self
                        .layers
                        .set_message(format!("carried forward {kind}, not rebuilt")),
                }
            }

            Event::Records(moved) => self.records.inc(moved),

            // A running total rather than a bar per blob. A publish moves
            // several layers at once, so "the blob in flight" is not a single
            // thing and a bar reset here would be eight uploads overwriting
            // each other; what an operator is watching is the link. The total
            // grows as the transfer takes blobs on, which is what makes the
            // rate and the estimate honest without the transport having to know
            // the whole stele's weight up front.
            Event::Blob { moved, bytes } => {
                self.blob.inc_length(bytes);

                // Counted as done the moment it is announced: nothing will
                // cross the wire for it, and a bar whose total grew by bytes
                // that are never coming would stall a little further from the
                // end with every blob the far side already held.
                if !moved {
                    self.blob.inc(bytes);
                }
            }

            // The total is held to at least the position rather than trusted
            // to bound it. A round trip the registry failed is made again from
            // the blob's first byte, and what the lost attempt moved is bytes
            // that crossed the wire — so the deltas can outrun the
            // announcements by however far the failure got. Taking the total up
            // to meet them is what the bar means: the link carried that much.
            Event::Bytes(moved) => {
                self.blob.inc(moved);

                let position = self.blob.position();

                if self.blob.length().is_some_and(|total| total < position) {
                    self.blob.set_length(position);
                }
            }

            // Said out loud rather than absorbed. The whole point of retrying
            // is that the run survives a registry's bad minute, and the whole
            // risk of retrying is that nobody finds out it had one — a `500`
            // per hour is a fact about the registry, and it reaches an operator
            // through the run log or not at all.
            Event::Retry {
                attempt,
                remaining,
                reason,
            } => tracing::warn!(
                attempt,
                remaining,
                reason,
                "the registry failed a round trip; making it again",
            ),
        }
    }
}

/// The mithril client's download and validation as progress bars.
///
/// Built fresh per download round by both callers that fetch — `bootstrap
/// mithril` and `snapshot backfill` — so a window's bars are its own.
#[cfg(feature = "mithril")]
pub struct MithrilFeedback {
    aggregate_pb: indicatif::ProgressBar,
    validate_pb: indicatif::ProgressBar,
}

#[cfg(feature = "mithril")]
impl MithrilFeedback {
    pub fn new(feedback: &Feedback) -> Self {
        let multi = feedback.multi_progress();

        let aggregate_pb = multi.add(indicatif::ProgressBar::hidden());
        aggregate_pb.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        aggregate_pb.set_message("downloading immutable files");

        let validate_pb = multi.add(indicatif::ProgressBar::new_spinner());
        validate_pb.set_style(
            indicatif::ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );

        Self {
            aggregate_pb,
            validate_pb,
        }
    }
}

#[cfg(feature = "mithril")]
#[async_trait::async_trait]
impl mithril_client::feedback::FeedbackReceiver for MithrilFeedback {
    async fn handle_event(&self, event: mithril_client::feedback::MithrilEvent) {
        match event {
            mithril_client::feedback::MithrilEvent::CardanoDatabase(db_event) => match db_event {
                mithril_client::feedback::MithrilEventCardanoDatabase::Started {
                    total_immutable_files,
                    ..
                } => {
                    self.aggregate_pb
                        .set_draw_target(indicatif::ProgressDrawTarget::stderr());
                    self.aggregate_pb.set_length(total_immutable_files);
                    self.aggregate_pb.set_position(0);
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::ImmutableDownloadCompleted {
                    ..
                } => {
                    self.aggregate_pb.inc(1);
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::Completed { .. } => {
                    self.aggregate_pb.finish_with_message("download completed");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadStarted {
                    size,
                    ..
                } => {
                    self.validate_pb.set_length(size);
                    self.validate_pb.set_position(0);
                    self.validate_pb.set_message("downloading digests");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadProgress {
                    downloaded_bytes,
                    size,
                    ..
                } => {
                    self.validate_pb.set_length(size);
                    self.validate_pb.set_position(downloaded_bytes);
                    self.validate_pb.set_message("downloading digests");
                }
                mithril_client::feedback::MithrilEventCardanoDatabase::DigestDownloadCompleted {
                    ..
                } => {
                    self.validate_pb
                        .finish_with_message("digests downloaded");
                }
                _ => {
                    tracing::debug!("unhandled mithril event: {db_event:?}");
                }
            },
            mithril_client::feedback::MithrilEvent::CertificateChainValidationStarted {
                ..
            } => {
                self.validate_pb
                    .set_message("certificate chain validation started");
            }
            mithril_client::feedback::MithrilEvent::CertificateValidated {
                certificate_hash: hash,
                ..
            } => {
                self.validate_pb
                    .set_message(format!("validating cert: {hash}"));
            }
            mithril_client::feedback::MithrilEvent::CertificateChainValidated { .. } => {
                self.validate_pb.set_message("certificate chain validated");
            }
            mithril_client::feedback::MithrilEvent::CertificateFetchedFromCache { .. } => {
                self.validate_pb
                    .set_message("certificate fetched from cache");
            }
            x => {
                tracing::debug!("unhandled mithril event: {x:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hidden(verb: &'static str) -> Arc<SteleProgress> {
        SteleProgress::new(&Feedback::hidden(), verb)
    }

    #[test]
    fn a_layer_names_itself_by_scope() {
        let epoch = serde_json::json!({"networkMagic": 764824073, "epoch": 512});
        let shard = serde_json::json!({"networkMagic": 764824073, "epoch": 512, "shard": 3});
        let neither = serde_json::json!({"lastImmutable": 9});

        assert_eq!(
            SteleProgress::describe("blocks", &epoch),
            "blocks epoch 512"
        );
        assert_eq!(
            SteleProgress::describe("state", &shard),
            "state epoch 512 shard 3"
        );
        assert_eq!(
            SteleProgress::describe("digests", &neither),
            "digests {\"lastImmutable\":9}"
        );
    }

    /// Layers held open together and closed in whatever order their sinks
    /// finish — the shape the export's state pass produces — leave the bar at
    /// the run's real position and never run it backwards.
    #[test]
    fn closing_out_of_order_does_not_overshoot() {
        let progress = hidden("publishing");
        let scope = serde_json::json!({"shard": 0});

        for index in 0..4 {
            progress.on(Event::LayerStarted {
                index,
                total: 4,
                kind: "state",
                scope: &scope,
            });
        }

        for index in (0..4).rev() {
            let before = progress.layers.position();

            progress.on(Event::LayerFinished {
                index,
                total: 4,
                kind: "state",
                outcome: Outcome::Transferred,
            });

            assert!(
                progress.layers.position() > before,
                "the bar went backwards"
            );
        }

        assert_eq!(progress.layers.position(), 4);
        assert_eq!(progress.layers.length(), Some(4));
    }

    /// The byte bar totals the run, and a blob nothing moves for is counted
    /// done rather than pending.
    ///
    /// The interleaving is the point of the first half: two blobs are announced
    /// before either finishes, exactly as a concurrent publish announces them,
    /// and the deltas that follow belong to whichever of the two produced them.
    /// A bar that reset per blob would answer 40 here.
    #[test]
    fn the_byte_bar_totals_the_run_across_blobs_in_flight() {
        let progress = hidden("publishing");

        progress.on(Event::Blob {
            moved: true,
            bytes: 300,
        });
        progress.on(Event::Blob {
            moved: true,
            bytes: 100,
        });

        progress.on(Event::Bytes(120));
        progress.on(Event::Bytes(180));
        progress.on(Event::Bytes(100));

        assert_eq!(progress.blob.position(), 400);
        assert_eq!(progress.blob.length(), Some(400));

        // And a blob the far side already holds lands on both sides at once, so
        // the bar stays where it was rather than falling behind by its size.
        progress.on(Event::Blob {
            moved: false,
            bytes: 50,
        });

        assert_eq!(progress.blob.position(), 450);
        assert_eq!(progress.blob.length(), Some(450));
    }

    /// A blob the registry failed part way through is sent again from its first
    /// byte, so the deltas outrun the announcement — and the total goes up to
    /// meet them rather than the bar sitting past its own end.
    ///
    /// The arithmetic is the point: 120 bytes of a 300-byte blob went out
    /// before the round trip died, the retry sent all 300, and 420 bytes really
    /// did cross the wire for a blob that was announced once.
    #[test]
    fn a_retried_blob_takes_the_total_up_to_what_the_link_carried() {
        let progress = hidden("publishing");

        progress.on(Event::Blob {
            moved: true,
            bytes: 300,
        });

        progress.on(Event::Bytes(120));

        progress.on(Event::Retry {
            attempt: 1,
            remaining: 3,
            reason: "the registry said 500",
        });

        progress.on(Event::Bytes(300));

        assert_eq!(progress.blob.position(), 420);
        assert_eq!(progress.blob.length(), Some(420));
    }
}
