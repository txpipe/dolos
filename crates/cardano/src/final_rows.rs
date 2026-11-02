//! The final-row log (decision 0064, item 2): an entity that has left the live
//! set leaves state, and the archive keeps its last row.
//!
//! At ESTART, the boundary visitors name the rows no ledger rule reads again
//! (`estart::final_rows`). Each is deleted from state, and its final row is
//! written to the log of the entity's own namespace, as `EpochState` writes its
//! closing row to `epochs`. The log row is keyed `epoch_start(e) ‖ entity key`,
//! where `e` is the epoch the boundary closes: the last epoch the row spent in
//! state. The archive commits before the state, so a crash between the two
//! re-runs the boundary over the same rows, which writes the same log rows
//! again.
//!
//! The key is the closing epoch, never an earlier one the entity names, for
//! two reasons:
//! - **Rollback.** A rollback cuts every log row at or after the slot it rolls
//!   back to. It can reach the closing epoch's last blocks, but not its start.
//! - **Steles.** A publisher carries a closed epoch's log layer forward
//!   unchanged. The first stele that holds epoch `e` closed is the one cut at
//!   the boundary that opens `e + 1`, after this write. A row written into an
//!   epoch that a stele already holds closed would be missing from every stele
//!   after it.
//!
//! Readers take the state row first and the log second. An entity can leave
//! state more than once, a pool that registers again and retires again, so
//! the newest final row wins ([`newest_final_row`]).

use dolos_core::{
    keys, ArchiveError, ArchiveStore, ArchiveWriter, BlockSlot, ChainError, EntityValue, KeyRange,
    LogEntity, LogKey, NsKey, StateEntity, StateWriter,
};

/// An entity kind whose rows ESTART moves from state to its log once they have
/// left the live set.
///
/// The kind is a state entity and a log entity under one namespace and one key
/// kind.
pub trait FinalRow: StateEntity + LogEntity<Key = <Self as StateEntity>::Key> {
    /// The row the log keeps: the state row without what no reader needs once
    /// the entity has left state.
    fn final_row(&self) -> Self;
}

/// One moved row: the state key it leaves and the log row that keeps it.
#[derive(Debug)]
struct Moved {
    state: NsKey,
    log: LogKey,
    value: EntityValue,
}

/// The rows one boundary moves, gathered by its visitors and written by its
/// commit.
#[derive(Debug, Default)]
pub struct FinalRows {
    rows: Vec<Moved>,
}

impl FinalRows {
    /// Move `row`, the state row under `key`, to its log at `slot`.
    pub fn push<E: FinalRow>(&mut self, slot: BlockSlot, key: &<E as StateEntity>::Key, row: &E) {
        let (_, value) = E::encode_entity(&row.final_row());

        self.rows.push(Moved {
            state: NsKey::of::<E>(key),
            log: LogKey::of(slot, key),
            value,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// The state keys of the moved rows.
    pub fn keys(&self) -> impl Iterator<Item = &NsKey> {
        self.rows.iter().map(|row| &row.state)
    }

    /// Write each final row to its log and delete its state row. The caller
    /// commits `archive` before `state`.
    pub fn write(
        &self,
        state: &impl StateWriter,
        archive: &impl ArchiveWriter,
    ) -> Result<(), ChainError> {
        for row in &self.rows {
            archive.write_log(row.state.ns(), &row.log, &row.value)?;
            state.delete_entity(row.state.ns(), row.state.key())?;
        }

        Ok(())
    }
}

/// The newest final row of `key`, with the slot it is keyed at.
///
/// A scan of the namespace's log: the log is keyed epoch first, and a reader
/// that knows only the entity has no epoch to seek to. Only the rows of `key`
/// are decoded.
pub fn newest_final_row<E: FinalRow>(
    archive: &impl ArchiveStore,
    key: &<E as StateEntity>::Key,
) -> Result<Option<(BlockSlot, E)>, ArchiveError> {
    let wanted = keys::encode(key);

    let mut newest = None;

    for row in archive.iter_logs(E::NS, KeyRange::all())? {
        let (log_key, value) = row?;

        if log_key.entity() == wanted.as_slice() {
            newest = Some((log_key.slot(), value));
        }
    }

    newest
        .map(|(slot, value)| {
            E::decode_entity(E::NS, &value)
                .map(|row| (slot, row))
                .map_err(|err| ArchiveError::EntityDecodingError(err.to_string()))
        })
        .transpose()
}
