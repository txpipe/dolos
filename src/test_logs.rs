//! What the code under test logs at WARN, for tests that assert on it.

use std::sync::{Arc, Mutex};

use tracing::{dispatcher::DefaultGuard, Dispatch};
use tracing_subscriber::{filter::Targets, layer::SubscriberExt as _, Layer as _};

#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Collects the WARN events of one module and its children on the current
/// thread until dropped. Under a current-thread runtime that covers every task
/// the test spawns.
pub(crate) struct Warnings {
    logged: Logged,
    _guard: DefaultGuard,
    _second: Dispatch,
}

impl Warnings {
    pub(crate) fn capture(module: &str) -> Self {
        let logged = Logged::default();
        let writer = logged.clone();

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(Targets::new().with_target(module, tracing::Level::WARN)),
        );

        // A callsite caches its interest per process: with a second dispatcher
        // alive, every one asks the thread's subscriber instead.
        let second = Dispatch::new(tracing_subscriber::registry());
        let guard = tracing::subscriber::set_default(subscriber);

        Self {
            logged,
            _guard: guard,
            _second: second,
        }
    }

    /// The lines logged so far, one per event.
    pub(crate) fn lines(&self) -> Vec<String> {
        let logged = String::from_utf8(self.logged.0.lock().unwrap().clone()).unwrap();
        logged.lines().map(String::from).collect()
    }
}
