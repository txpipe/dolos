//! What the code under test logs at WARN, for tests that assert on it.

use std::sync::{Arc, Mutex};

use tracing::Level;

/// The WARN events `run` logs on this thread, each as its fields.
pub(crate) fn warned_while(run: impl FnOnce()) -> Vec<String> {
    #[derive(Clone, Default)]
    struct Warned(Arc<Mutex<Vec<String>>>);

    struct Fields(String);

    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={value:?} ", field.name()));
        }
    }

    impl tracing::Subscriber for Warned {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= Level::WARN
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == Level::WARN {
                let mut fields = Fields(String::new());
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    // A callsite caches its interest per process: with a second dispatcher
    // alive, every one asks the thread's subscriber instead.
    let _second = tracing::Dispatch::new(Warned::default());
    let warned = Warned::default();

    tracing::subscriber::with_default(warned.clone(), run);

    let events = warned.0.lock().unwrap().clone();

    events
}
