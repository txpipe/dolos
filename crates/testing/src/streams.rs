use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use futures_core::Stream;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

/// Build a no-op waker for synchronously driving streams in tests.
pub fn noop_waker() -> Waker {
    Arc::new(NoopWake).into()
}

/// A generic stream backed by a scripted sequence of poll results.
///
/// Each call to `poll_next` pops the front item from the internal queue.
/// When the queue is empty, returns `Ready(None)`.
pub struct ScriptedStream<T> {
    items: VecDeque<Poll<Option<T>>>,
}

impl<T> ScriptedStream<T> {
    pub fn new(items: Vec<Poll<Option<T>>>) -> Self {
        Self {
            items: VecDeque::from(items),
        }
    }

    pub fn empty() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }
}

// Safety: ScriptedStream only contains a VecDeque (heap-allocated), so it is
// always safe to move even after pinning.
impl<T> Unpin for ScriptedStream<T> {}

impl<T> Stream for ScriptedStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.items.pop_front().unwrap_or(Poll::Ready(None))
    }
}
