//! The next layers of a restore's plan, read while the writer drains the
//! current one.
//!
//! A registry transport downloads a whole blob before it yields a record, so a
//! serial restore waits on the network once per layer. [`ReadAhead`] moves that
//! wait onto worker threads, at most its depth ahead of the writer and in plan
//! order. The writer still takes each layer at its turn, so what it writes and
//! records keeps its order, and a layer whose read failed fails at its turn.

use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{Condvar, Mutex, MutexGuard, PoisonError},
};

use stelae::{
    frame::Limits,
    inscription::LayerDescriptor,
    transport::{BlobIndex, SteleReader},
    LayerReader,
};

use crate::DolosProfile;

/// A layer as a worker left it: ready to drain, or the error its read ended in.
type Staged<B> = Result<LayerReader<B>, stelae::Error>;

/// Layers read ahead of a restore's writer, in the order it drains them.
pub(super) struct ReadAhead<'a, R: SteleReader> {
    stele: &'a R,
    index: &'a BlobIndex,
    limits: Limits,
    /// The layers the writer will drain, in its order: every layer the
    /// checkpoint has not recorded.
    order: Vec<LayerDescriptor>,
    depth: usize,
    queue: Mutex<Queue<R::Blob>>,
    changed: Condvar,
}

struct Queue<B: std::io::Read> {
    /// The position of the next layer a worker reads.
    claimed: usize,
    /// The position of the next layer the writer takes.
    taken: usize,
    ready: HashMap<usize, Staged<B>>,
    stopped: bool,
}

impl<'a, R: SteleReader> ReadAhead<'a, R> {
    /// Read `order` ahead of a writer, holding at most `depth` layers read or
    /// in flight that it has not taken yet.
    pub(super) fn new(
        stele: &'a R,
        index: &'a BlobIndex,
        limits: Limits,
        order: Vec<LayerDescriptor>,
        depth: usize,
    ) -> Self {
        Self {
            stele,
            index,
            limits,
            order,
            depth: depth.max(1),
            queue: Mutex::new(Queue {
                claimed: 0,
                taken: 0,
                ready: HashMap::new(),
                stopped: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Run `write` with `depth` workers reading ahead of it, and join them
    /// before returning, however `write` ends.
    pub(super) fn run<T>(&self, write: impl FnOnce() -> T) -> T
    where
        R: Sync,
        R::Blob: Send,
    {
        let span = tracing::Span::current();

        std::thread::scope(|scope| {
            // Before the first spawn, so no exit leaves a worker waiting.
            let _stop = Stop(self);

            for _ in 0..self.depth.min(self.order.len()) {
                let span = span.clone();

                std::thread::Builder::new()
                    .name("restore-read-ahead".into())
                    .spawn_scoped(scope, move || span.in_scope(|| self.work()))
                    .expect("spawning a read-ahead worker");
            }

            write()
        })
    }

    /// The layer the writer drains next, once a worker has read it, or `None`
    /// when `descriptor` is not that layer and the writer reads it itself.
    pub(super) fn take(&self, descriptor: &LayerDescriptor) -> Option<Staged<R::Blob>> {
        let mut queue = self.lock();
        let at = queue.taken;

        if self.order.get(at).map(|next| next.diff_id) != Some(descriptor.diff_id) {
            return None;
        }

        loop {
            if let Some(staged) = queue.ready.remove(&at) {
                queue.taken += 1;
                self.changed.notify_all();

                return Some(staged);
            }

            if queue.stopped {
                return None;
            }

            queue = self.wait(queue);
        }
    }

    fn work(&self) {
        while let Some(at) = self.claim() {
            // The transport stages the blob and opens the layer in one call:
            // it has no read that stops at the staged file.
            let read = AssertUnwindSafe(|| {
                self.stele
                    .stream_layer(self.index, &DolosProfile, &self.order[at], self.limits)
            });

            // A panic becomes the layer's error, so the writer is not left
            // waiting for a layer nobody will deliver.
            let staged = std::panic::catch_unwind(read).unwrap_or_else(|_| {
                Err(stelae::Error::Io(std::io::Error::other(
                    "reading this layer ahead of the restore panicked",
                )))
            });

            self.deliver(at, staged);
        }
    }

    fn claim(&self) -> Option<usize> {
        let mut queue = self.lock();

        loop {
            if queue.stopped || queue.claimed == self.order.len() {
                return None;
            }

            if queue.claimed < queue.taken + self.depth {
                queue.claimed += 1;

                return Some(queue.claimed - 1);
            }

            queue = self.wait(queue);
        }
    }

    fn deliver(&self, at: usize, staged: Staged<R::Blob>) {
        let mut queue = self.lock();

        // Dropped rather than kept once stopped, which removes its staged file.
        if queue.stopped {
            return;
        }

        queue.ready.insert(at, staged);
        self.changed.notify_all();
    }

    /// Stop reading ahead and drop what was read, removing its staged files.
    fn stop(&self) {
        let ready = {
            let mut queue = self.lock();

            queue.stopped = true;
            self.changed.notify_all();

            std::mem::take(&mut queue.ready)
        };

        drop(ready);
    }

    fn lock(&self) -> MutexGuard<'_, Queue<R::Blob>> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'q>(&self, queue: MutexGuard<'q, Queue<R::Blob>>) -> MutexGuard<'q, Queue<R::Blob>> {
        self.changed
            .wait(queue)
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Stops a [`ReadAhead`] when dropped.
struct Stop<'r, 'a, R: SteleReader>(&'r ReadAhead<'a, R>);

impl<R: SteleReader> Drop for Stop<'_, '_, R> {
    fn drop(&mut self) {
        self.0.stop();
    }
}
