use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::Epoch,
};
use serde::{Deserialize, Serialize};

use dolos_core::ChainError;

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub(super) enum EpochPosition {
    #[n(0)]
    Genesis,

    #[n(1)]
    Epoch(#[n(0)] Epoch),
}

impl EpochPosition {
    pub fn mark(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 1 => Some(*epoch - 1),
            _ => None,
        }
    }
    pub fn set(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 2 => Some(*epoch - 2),
            _ => None,
        }
    }
    pub fn go(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 3 => Some(*epoch - 3),
            _ => None,
        }
    }
}

impl PartialEq<Epoch> for EpochPosition {
    fn eq(&self, other: &Epoch) -> bool {
        match self {
            EpochPosition::Genesis => false,
            EpochPosition::Epoch(epoch) => *epoch == *other,
        }
    }
}

impl std::ops::Add<Epoch> for EpochPosition {
    type Output = EpochPosition;

    fn add(self, other: Epoch) -> Self::Output {
        match self {
            EpochPosition::Genesis if other == 0 => EpochPosition::Genesis,
            EpochPosition::Genesis => EpochPosition::Epoch(other - 1),
            EpochPosition::Epoch(current) => EpochPosition::Epoch(current + other),
        }
    }
}

impl std::ops::AddAssign<Epoch> for EpochPosition {
    fn add_assign(&mut self, other: Epoch) {
        *self = *self + other;
    }
}

/// Allows implementing types to define what the default value is when
/// transitioning to the next epoch. Some scenarios require a reset of its value
/// and some others just a copy of the live one.
pub trait TransitionDefault: Sized {
    fn next_value(current: Option<&Self>) -> Option<Self>;
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochValue<T> {
    /// The epoch representing the live version of the value
    #[n(0)]
    epoch: EpochPosition,

    /// The next version of the value already scheduled for the next epoch
    #[n(1)]
    next: Option<T>,

    /// The current, mutating version of the value
    #[n(2)]
    live: Option<T>,

    /// Epoch - 1 version of the value
    #[n(4)]
    mark: Option<T>,

    /// Epoch - 2 version of the value
    #[n(5)]
    set: Option<T>,

    /// Epoch - 3 version of the value
    #[n(6)]
    go: Option<T>,
}

impl<T> EpochValue<T>
where
    T: Clone + std::fmt::Debug,
{
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: None,
            next: None,
        }
    }

    pub fn with_live(epoch: Epoch, live: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: Some(live),
            next: None,
        }
    }

    pub fn with_scheduled(epoch: Epoch, next: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: None,
            next: Some(next),
        }
    }

    /// Test-only raw constructor used by proptest strategies to populate every slot
    /// independently. Must stay behind `cfg(test)` so production code keeps going through
    /// the regular `new`/`with_live`/`schedule`/`transition` API.
    #[cfg(test)]
    pub(crate) fn from_parts(
        epoch: Epoch,
        live: Option<T>,
        next: Option<T>,
        mark: Option<T>,
        set: Option<T>,
        go: Option<T>,
    ) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            live,
            next,
            mark,
            set,
            go,
        }
    }

    pub fn with_genesis(live: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(0),
            go: None,
            set: None,
            mark: Some(live.clone()),
            live: Some(live),
            next: None,
        }
    }

    /// Returns the epoch of the live value
    pub fn epoch(&self) -> Option<Epoch> {
        match self.epoch {
            EpochPosition::Genesis => None,
            EpochPosition::Epoch(epoch) => Some(epoch),
        }
    }

    /// Returns a reference to the live value that matches the ongoing epoch.
    pub fn live(&self) -> Option<&T> {
        self.live.as_ref()
    }

    pub fn unwrap_live(&self) -> &T {
        self.live.as_ref().expect("live value not initialized")
    }

    pub fn unwrap_live_mut(&mut self) -> &mut T {
        self.live.as_mut().expect("live value not initialized")
    }

    pub fn go(&self) -> Option<&T> {
        self.go.as_ref()
    }

    pub fn set(&self) -> Option<&T> {
        self.set.as_ref()
    }

    pub fn mark(&self) -> Option<&T> {
        self.mark.as_ref()
    }

    pub fn next(&self) -> Option<&T> {
        self.next.as_ref()
    }

    pub fn next_mut(&mut self) -> Option<&mut T> {
        self.next.as_mut()
    }

    /// Schedules the next value to be applied on the next epoch transition
    pub fn schedule(&mut self, current_epoch: Epoch, next: Option<T>) {
        #[cfg(feature = "strict")]
        assert_eq!(self.epoch, current_epoch);
        let _ = current_epoch;

        self.next = next;
    }

    /// Mutates the live value for the current epoch without rotating any of
    /// the previous values
    pub fn live_mut(&mut self, epoch: Epoch) -> &mut Option<T> {
        #[cfg(feature = "strict")]
        assert_eq!(self.epoch, epoch);
        let _ = epoch;

        assert!(
            self.next.is_none(),
            "can't change live value when next value is already scheduled"
        );

        &mut self.live
    }

    /// Resets the live value for the current epoch.
    pub fn reset(&mut self, live: Option<T>) {
        self.live = live;
    }

    /// Replaces the live value for the current epoch without rotating any of
    /// the previous values
    pub fn replace(&mut self, live: T, epoch: Epoch) {
        #[cfg(feature = "strict")]
        assert_eq!(self.epoch, epoch);
        let _ = epoch;

        self.live = Some(live);
    }

    /// Transitions into the next epoch by taking a snapshot of the live value
    /// and rotating the previous ones.
    pub fn transition(&mut self, next_epoch: Epoch) {
        #[cfg(feature = "strict")]
        assert_eq!(self.epoch + 1, next_epoch);
        let _ = next_epoch;

        self.go = self.set.clone();
        self.set = self.mark.clone();
        self.mark = self.live.clone();
        self.live = self.next.take();

        self.epoch += 1;
    }

    /// Returns the value for the snapshot taken at the end of the given epoch.
    pub fn snapshot_at(&self, ending_epoch: Epoch) -> Option<&T> {
        if self.epoch == ending_epoch {
            self.live.as_ref()
        } else if self.epoch.mark() == Some(ending_epoch) {
            self.mark.as_ref()
        } else if self.epoch.set() == Some(ending_epoch) {
            self.set.as_ref()
        } else if self.epoch.go() == Some(ending_epoch) {
            self.go.as_ref()
        } else {
            None
        }
    }

    pub fn try_snapshot_at(&self, epoch: Epoch) -> Result<&T, ChainError> {
        match self.snapshot_at(epoch) {
            Some(value) => Ok(value),
            None => Err(ChainError::EpochValueVersionNotFound(epoch)),
        }
    }
}

impl<T> EpochValue<T>
where
    T: TransitionDefault + std::fmt::Debug + Clone,
{
    pub fn scheduled_or_default(&mut self) -> &mut T {
        if self.next.is_none() {
            self.next = T::next_value(self.live.as_ref());
        }

        self.next.as_mut().unwrap()
    }

    /// Transitions into the next epoch using the scheduled value, falling back
    /// to the default value if the next is not scheduled.
    pub fn default_transition(&mut self, next_epoch: Epoch) {
        if self.next.is_none() {
            let next = T::next_value(self.live.as_ref());
            self.next = next;
        }

        self.transition(next_epoch);
    }
}

#[cfg(test)]
pub(crate) mod testing {
    #![allow(dead_code)]

    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    /// Generate an `EpochValue<T>` where `live` is always populated (most deltas require
    /// it) and the other slots are independently randomized.
    ///
    /// Takes a `BoxedStrategy` because many proptest combinator types are not `Clone`,
    /// which is required by the tuple-combinator composition used here.
    pub fn any_epoch_value<T>(inner: BoxedStrategy<T>) -> impl Strategy<Value = EpochValue<T>>
    where
        T: Clone + std::fmt::Debug + 'static,
    {
        (
            root::any_epoch(),
            inner.clone(),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner),
        )
            .prop_map(|(epoch, live, next, mark, set, go)| {
                EpochValue::from_parts(epoch, Some(live), next, mark, set, go)
            })
    }

    /// Variant of `any_epoch_value` that lets `live` be `None` too.
    pub fn any_epoch_value_opt_live<T>(
        inner: BoxedStrategy<T>,
    ) -> impl Strategy<Value = EpochValue<T>>
    where
        T: Clone + std::fmt::Debug + 'static,
    {
        (
            root::any_epoch(),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner),
        )
            .prop_map(|(epoch, live, next, mark, set, go)| {
                EpochValue::from_parts(epoch, live, next, mark, set, go)
            })
    }

    /// Variant of `any_epoch_value` that forces `next` to `None`. Required by deltas
    /// whose `apply` calls `live_mut`, which asserts there is no scheduled next.
    pub fn any_epoch_value_no_next<T>(
        inner: BoxedStrategy<T>,
    ) -> impl Strategy<Value = EpochValue<T>>
    where
        T: Clone + std::fmt::Debug + 'static,
    {
        (
            root::any_epoch(),
            inner.clone(),
            prop::option::of(inner.clone()),
            prop::option::of(inner.clone()),
            prop::option::of(inner),
        )
            .prop_map(|(epoch, live, mark, set, go)| {
                EpochValue::from_parts(epoch, Some(live), None, mark, set, go)
            })
    }
}
