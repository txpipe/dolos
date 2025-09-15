pub use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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
    multi: MultiProgress,
}

impl Feedback {
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

impl Default for Feedback {
    fn default() -> Self {
        let multi = MultiProgress::new();
        Self { multi }
    }
}
