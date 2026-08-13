pub use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

use dolos_snapshot::progress::{Event, Observer, Outcome, Progress};

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

            // Per blob, not per run: the total a run will move is not knowable
            // up front on the publish side — a layer's compressed size exists
            // only once it has been compressed — so a cumulative bar would show
            // a length that grew as it filled.
            Event::Blob { moved, bytes } => {
                self.blob.set_position(0);
                self.blob.set_length(bytes);

                match moved {
                    true => self.blob.set_message(""),
                    false => self.blob.set_message("already in the registry"),
                }
            }

            Event::Bytes(moved) => self.blob.inc(moved),
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

    #[test]
    fn a_blob_bar_is_per_blob_and_a_skip_moves_nothing() {
        let progress = hidden("publishing");

        progress.on(Event::Blob {
            moved: true,
            bytes: 300,
        });
        progress.on(Event::Bytes(120));
        progress.on(Event::Bytes(180));

        assert_eq!(progress.blob.position(), 300);
        assert_eq!(progress.blob.length(), Some(300));

        progress.on(Event::Blob {
            moved: false,
            bytes: 50,
        });

        assert_eq!(progress.blob.position(), 0);
        assert_eq!(progress.blob.length(), Some(50));
    }
}
