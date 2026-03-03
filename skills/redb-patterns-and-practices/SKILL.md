# Redb Conventions for dolos-redb3

Rules and patterns extracted from `crates/redb3/` (wal, state, archive, mempool modules). These are **implementation-agnostic** — they apply whenever adding or modifying redb-backed storage in this crate.

---

## 1. Key/value newtypes (`redb::Value`, `redb::Key`)

### When to create one

- **Always** wrap domain types that will be used as table keys or values in a dedicated newtype.
- Use a newtype even for simple byte arrays (`[u8; 32]`, `[u8; 40]`) — it gives type safety in `TableDefinition` and a home for conversion methods.
- Foreign types (defined outside the crate, e.g. `EraCbor`) **must** be wrapped because orphan rules prevent implementing `redb::Value` on them directly.

### Fixed-width (raw byte keys)

Pattern: newtype over `[u8; N]`.

```rust
#[derive(Debug)]
struct DbFoo([u8; N]);

impl redb::Value for DbFoo {
    type SelfType<'a> = Self where Self: 'a;
    type AsBytes<'a> = &'a [u8; N] where Self: 'a;

    fn fixed_width() -> Option<usize> { Some(N) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        Self(<[u8; N]>::try_from(data).unwrap())
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        &value.0
    }
    fn type_name() -> redb::TypeName { redb::TypeName::new("qualified_name") }
}
```

- If the type will be a table **key**, also impl `redb::Key` with lexicographic compare:
  ```rust
  impl redb::Key for DbFoo {
      fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering { data1.cmp(data2) }
  }
  ```

### Variable-width (serialized records)

Pattern: struct with `#[derive(Encode, Decode)]` (minicbor), `AsBytes = Vec<u8>`.

```rust
impl redb::Value for MyRecord {
    type SelfType<'a> = Self where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;

    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        minicbor::decode(data).unwrap()
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        minicbor::to_vec(value).unwrap()
    }
    fn type_name() -> redb::TypeName { redb::TypeName::new("qualified_name") }
}
```

### Required derives

- `Debug` is **required** by redb (`Value: Debug`).
- Add `Clone` if the record will be passed by value to `table.insert()` or needs to be extracted from an `AccessGuard` and used after the table is dropped.

### Naming

- Prefix with `Db` for storage-specific newtypes that mirror a domain type (e.g. `DbTxHash`, `DbChainPoint`).
- Domain records stored directly (e.g. `InflightRecord`, `FinalizedEntry`) keep their domain name — no `Db` prefix needed.

### Serialization

- Use **minicbor** (`Encode`/`Decode` derives with `#[n(...)]` field tags) for variable-width values. This is the crate-wide standard; no bincode, no serde_json.
- Use raw bytes for fixed-width keys.

### `type_name` strings

- Use a qualified, snake_case name scoped to the module: `"mempool_pending_key"`, `"mempool_inflight_record"`, not just `"key"`.

---

## 2. Table wrappers

### Structure

Every table (or group of related tables) gets a **zero-sized struct** with a `const DEF` and **static methods only**.

```rust
struct FooTable;

impl FooTable {
    const DEF: TableDefinition<'static, KeyType, ValueType> = TableDefinition::new("table_name");
    // all methods are `fn(tx, ...) -> Result<..., ModuleError>`
}
```

### Method signatures

- Read-only operations take `&redb::ReadTransaction`.
- Write operations take `&redb::WriteTransaction`.
- When the same logical operation is needed from both tx types, provide two methods (e.g. `get` for read-tx, `read` for write-tx).
- Each method opens the table internally (`tx.open_table(Self::DEF)?`). Tables are **not** shared across methods.

### `initialize`

Every table wrapper has:
```rust
fn initialize(wx: &redb::WriteTransaction) -> Result<(), ModuleError> {
    wx.open_table(Self::DEF)?;
    Ok(())
}
```
This is called during store construction (idempotent — creates the table if missing, no-ops otherwise).

### Iteration patterns

- **Collect-then-mutate**: When you need to iterate a table and then modify it, first collect entries into a `Vec`, drop the iterator, then mutate. redb does not allow concurrent read iteration and mutation on the same table handle.
- **Peek with limit**: Iterate with `for entry in iter`, break when `result.len() >= limit`.
- **Cursor pagination**: Track `last_seq`, set `next_cursor = last_seq + 1` if `items.len() >= limit`.

### Single-pass filter+remove (`extract_if` / `retain`) — redb ≥ 3.1

redb 3.1 provides four methods on `Table` that combine iteration and removal in one pass, eliminating the need for collect-then-mutate when applicable:

| Method | Removes matching? | Returns removed? | Scoped to range? |
|--------|-------------------|------------------|------------------|
| `extract_if(pred)` | yes (on consume) | yes (iterator of `(AccessGuard<K>, AccessGuard<V>)`) | no (full table) |
| `extract_from_if(range, pred)` | yes (on consume) | yes (iterator) | yes |
| `retain(pred)` | removes where pred=**false** | no | no (full table) |
| `retain_in(range, pred)` | removes where pred=**false** | no | yes |

**When to use:**
- The operation targets a **single table** (filter + remove, optionally collecting the removed entries).
- The predicate is a **pure test** on key/value with no side-effects and no writes to other tables.
- Ideal for drain-style methods that return the removed entries.

```rust
// Example: drain entries whose hash is in a set (PendingTable::drain_by_hashes)
fn drain_by_hashes(wx: &redb::WriteTransaction, hashes: &HashSet<TxHash>)
    -> Result<Vec<(TxHash, EraCbor)>, Error>
{
    let mut table = wx.open_table(Self::DEF)?;
    let extracted = table.extract_if(|key, _value| hashes.contains(&key.hash()))?;
    extracted
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().hash(), value.value().0))
        })
        .collect()
}
```

**When NOT to use:**
- **Cross-table operations**: If the loop body writes to a *different* table (e.g., remove from inflight → insert into finalized), the `extract_if` iterator borrows `&mut table` which blocks opening other tables on the same write transaction. Collecting the iterator first just trades one two-pass pattern for another with no net benefit.
- **In-place updates**: If some entries need to be *modified* rather than removed (e.g., a three-way branch where only one branch removes), `extract_if`/`retain` cannot express this — they can only remove, not update.
- **Side-effects during iteration**: If the predicate or loop body needs mutable access to external state that conflicts with the table borrow, stick with collect-then-mutate.

### Auto-incrementing keys

Derive the next key from `table.last()`:
```rust
let seq = match table.last()? {
    Some(entry) => entry.0.value().seq() + 1,
    None => 0,
};
```

### Lifetime gotcha

When extracting a value from an `AccessGuard`, bind the result to a local **before** returning through `Ok(...)`:
```rust
// WRONG -- table may be dropped before the guard is consumed
Ok(table.get(key)?.map(|e| e.value()))

// RIGHT -- value is extracted while table is still alive
let result = table.get(key)?.map(|e| e.value());
Ok(result)
```

---

## 3. Error types

### Pattern

Each module defines a **transparent newtype** wrapping the domain error from `dolos-core`:

```rust
#[derive(Debug, Error)]
#[error(transparent)]
struct RedbFooError(#[from] DomainError);
```

### Required `From` impls

Every module must convert **all six** redb error types that can surface from table/transaction operations:

| redb type | When it occurs |
|-----------|---------------|
| `redb::Error` | Generic / database open |
| `redb::DatabaseError` | `begin_write()` / `begin_read()` |
| `redb::TransactionError` | `begin_write()` / `begin_read()` |
| `redb::TableError` | `open_table()` |
| `redb::CommitError` | `wx.commit()` |
| `redb::StorageError` | Any table read/write |

Plus a reverse impl: `From<RedbFooError> for DomainError`.

---

## 4. Store struct

### Fields

- `db: Arc<redb::Database>` -- always `Arc`-wrapped, stores are `Clone`.
- Additional domain-specific fields (channels, flatfile stores, etc.).

### Constructors

Two public constructors:
- `open(path, config)` -- on-disk, with `set_repair_callback` and `set_cache_size`.
- `in_memory()` -- uses `redb::backends::InMemoryBackend`, for tests.

Both call a private `from_db(db)` or construct directly, then call `ensure_initialized()`.

### `ensure_initialized`

Opens a write transaction, calls `Table::initialize(&wx)` for every table, commits. Idempotent.

```rust
fn ensure_initialized(&self) -> Result<(), RedbFooError> {
    let wx = self.db.begin_write()?;
    FooTable::initialize(&wx)?;
    BarTable::initialize(&wx)?;
    wx.commit()?;
    Ok(())
}
```

### Transaction helpers

For write operations that produce domain events, use a helper like `with_write_tx` that:
1. Opens a write transaction.
2. Calls the closure.
3. Commits.
4. Dispatches events (notifications, broadcasts).
5. Logs errors without panicking.

---

## 5. General rules

- **No `&[u8]` table definitions** -- always use typed keys and values via `redb::Value` / `redb::Key` impls. This eliminates manual `serialize()`/`deserialize()` methods scattered through business logic.
- **No standalone `serialize`/`deserialize` methods** on record types -- the `redb::Value` impl replaces them.
- **No free-standing table constants** (`const FOO_TABLE: TableDefinition<...>`) -- they belong as `const DEF` inside the table wrapper struct.
- **Domain logic stays on the record**, storage logic stays on the table wrapper. E.g. `record.confirm(point)` is domain; `InflightTable::write(wx, hash, &record)` is storage.
- **Tests exercise the public trait**, not internal table/record methods. Table wrappers and redb types are private implementation details.

---

## Reference files

| Pattern | File | Lines |
|---------|------|-------|
| Fixed-width key+value | `crates/redb3/src/wal/mod.rs` | `DbChainPoint` ~64-131 |
| Variable-width value (minicbor) | `crates/redb3/src/mempool.rs` | `InflightRecord` redb::Value impl ~232-257 |
| Foreign-type wrapper | `crates/redb3/src/mempool.rs` | `DbEraCbor` ~176-204 |
| Table wrapper (simple) | `crates/redb3/src/state/utxoset.rs` | `UtxosTable` ~38-80 |
| Table wrapper (multi-step) | `crates/redb3/src/archive/tables.rs` | `BlocksTable` ~10-100 |
| Table wrapper (mempool) | `crates/redb3/src/mempool.rs` | `PendingTable`, `InflightTable`, `FinalizedTable` |
| `extract_if` single-pass drain | `crates/redb3/src/mempool.rs` | `PendingTable::drain_by_hashes` |
| Error newtype | `crates/redb3/src/mempool.rs` | `RedbMempoolError` ~18-62 |
| Store struct + constructors | `crates/redb3/src/mempool.rs` | `RedbMempool` ~667-714 |
| Multimap tables | `crates/redb3/src/archive/indexes.rs` | `FilterIndexes`, archive index tables |
