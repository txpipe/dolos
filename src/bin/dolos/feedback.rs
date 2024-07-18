use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} slots (eta: {eta}) {msg}",
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
