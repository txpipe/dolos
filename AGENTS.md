# Dolos: Cardano Data Node - Rust Crate Organization

## Project Overview

Dolos is a lightweight Cardano node designed specifically for keeping an updated copy of the ledger and responding to queries from trusted clients while requiring minimal resources compared to a full node. It serves as a data provider rather than a consensus-validating node, focusing on efficiency and compatibility with existing Cardano ecosystem tools.

## Storage Concepts

Dolos uses three distinct storage backends, each serving a specific purpose:

### StateStore
- **Purpose**: Current ledger state (the "world view")
- **Contents**: UTxO set, entity state, chain cursor position
- **Traits**: `StateStore` (reads) + `StateWriter` (batched writes)
- **Database**: `<storage.path>/state`

### ArchiveStore
- **Purpose**: Historical block storage with temporal indexing
- **Contents**: Block bodies indexed by slot (one zstd frame per body in flat segment files, compressed with the dictionary bundled in `dolos-flatfiles`), entity logs keyed by `LogKey` (slot + entity key), and the lookups over them: archive tags (by address, payment, stake, policy, asset, datum, …) and exact lookups (by block hash, block number, tx hash), written in the same batch as the blocks they project
- **Traits**: `ArchiveStore` (reads) + `ArchiveWriter` (batched writes)
- **Database**: `<storage.path>/archive` (index plus flat block segment files)

### WalStore (Write-Ahead Log)
- **Purpose**: Crash recovery and rollback support
- **Contents**: Log entries with block data, entity deltas, and input UTxOs
- **Traits**: `WalStore`
- **Database**: `<storage.path>/wal`

### Where the indexes live
There is no standalone index store — it was removed in v1.7. Every index is a
projection, and lives in the store that holds what it projects:
- the live-UTxO tags (by address, payment, stake, policy, asset, script ref) project the UTxO set and live in the `StateStore` (`StateStore::utxos_by_tag`, written through `StateWriter::apply_utxo_tags` in the same batch as the set)
- the archive tags and the exact lookups (by block hash, block number, tx hash) project the block history and live in the `ArchiveStore` (`ArchiveStore::slots_by_tag` / `slot_by_*`, written through `ArchiveWriter::apply_index` in the same batch as the blocks)

### Database File Organization

```
<storage.path>/
├── wal      # Write-Ahead Log database
├── state    # Ledger state database
├── archive  # Archive index plus flat block segment files
└── scratch  # not a store: stele layers staged in flight by a registry transfer
```

The persistent WAL uses Redb, while state and archive use separate Fjall databases with independent cache and durability configuration. Builtin memory stores serve ephemeral nodes, tooling, and tests.

## Crate Architecture

The project follows a modular workspace architecture with clear separation of concerns and trait-based extensibility.

### Core Crates

#### `dolos` (Main Binary)
- **Purpose**: Main application binary and CLI interface
- **Role**: Application layer that orchestrates all services
- **Key Modules**:
  - `sync`: Chain synchronization from upstream nodes
  - `serve`: gRPC (UTxO RPC) and Ouroboros network services
  - `relay`: Upstream relay connection handling
  - `mempool`: Transaction mempool implementation
  - `facade`: High-level domain operations (extends `dolos-core` facade)
- **CLI Commands**: daemon, sync, serve, bootstrap (relay/mithril/snapshot), data, doctor
- **Features**: Configurable service compilation (grpc, minibf, trp, mithril, utils)

#### `dolos-core` (Foundation)
- **Purpose**: Core traits, types, and abstractions common to all Dolos components
- **Key Modules**:
  - `state`: `StateStore` and `StateWriter` traits, entity system
  - `archive`: `ArchiveStore` and `ArchiveWriter` traits, `SlotTags` for indexing metadata
  - `indexes`: the chain-agnostic index record types (`Tag`, `UtxoIndexDelta`, `ArchiveIndexDelta`, `TagRecord`, `ExactRecord`, `IndexRecord`) the state and archive stores consume
  - `wal`: `WalStore` trait for write-ahead logging
  - `batch`: `WorkBatch`, `WorkBlock`, `WorkDeltas` for batch processing pipeline
  - `facade`: High-level operations (`execute_batch`, `roll_forward`, `import_blocks`)
  - `query`: `QueryHelpers` trait and `SparseBlockIter` for joining indexes with archive
  - `Domain`: Central trait tying all storage backends together
  - `ChainLogic`: Trait for blockchain-specific processing logic
  - `mempool`: Transaction mempool interface
- **Role**: Foundation layer providing the architecture that other crates implement

#### `dolos-cardano` (Blockchain Logic)
- **Purpose**: Cardano-specific implementation of the core traits
- **Components**:
  - `CardanoLogic`: Implementation of `ChainLogic` for Cardano
  - `CardanoEntity` / `CardanoDelta`: Entity-delta implementations
  - Block processing, validation, and era handling
  - Genesis configuration management and bootstrap
  - Reward distribution processing
  - UTxO set delta computation
- **Dependencies**: `dolos-core`, Pallas library for Cardano protocol support

#### `dolos-redb3` (Storage Backend)
- **Purpose**: Redb v3 implementations retained for the WAL, mempool, and legacy state tests
- **Components**:
  - `wal`: `WalStore` implementation for crash recovery
  - `mempool`: persistent mempool storage
  - `state`: legacy `StateStore` implementation used by tests, not a supported node configuration backend
- **Role**: Persistence layer for the WAL and mempool

#### `dolos-fjall` (State and Archive Storage)
- **Purpose**: Persistent state and archive implementation using the Fjall LSM-tree embedded database
- **Design Philosophy**: Optimized for write-heavy workloads with many keys, ideal for blockchain data
- **Components**:
  - `state`: `StateStore` implementation with four-keyspace design:
    - **`state-cursor`**: Chain position tracking (single key-value)
    - **`state-utxos`**: UTxO set storage with `[tx_hash:32][index:4]` keys
    - **`state-entities`**: All entity types with `[ns_hash:8][entity_key:32]` keys
    - **`state-tags`**: Live-UTxO tags with `[dim_hash:8][lookup_key:var][txo_ref:36]` keys
  - `archive`: `ArchiveStore` implementation with four-keyspace design:
    - **`archive-blocks`**: Slot -> packed physical frame locations in the flat segment files
    - **`archive-logs`**: All log namespaces with `[ns_hash:8][log_key:40]` keys
    - **`archive-tags`**: Tag-based prefix scans for block tags with `[dim_hash:8][key_hash:8][slot:8]` keys
    - **`index-exact`**: Exact-match lookups with `[dim_hash:8][key_data:var]` -> `[slot:8]`
  - `keys`: Shared key encoding utilities
- **Key Advantages**:
  - Reduced segment files compared to per-entity keyspaces
  - Chain-agnostic design using dimension hashing
  - LSM-tree optimization for high-write blockchain workloads
- **Role**: Primary persistence layer implementing `StateStore` and `ArchiveStore` traits

### Service Crates

#### `dolos-minibf` (Blockfrost API)
- **Purpose**: Blockfrost-compatible HTTP API service
- **Components**:
  - REST API endpoints mimicking Blockfrost
  - Cardano data mapping and transformation
  - HTTP server using Axum framework
- **Role**: API compatibility layer for existing Blockfrost clients

#### `dolos-trp` (Transaction Resolver Protocol)
- **Purpose**: Transaction Resolver Protocol implementation (Tx3 framework integration)
- **Components**:
  - JSON-RPC server for transaction processing
  - Integration with Tx3 SDK for transaction resolution
- **Role**: Transaction processing service leveraging the Tx3 framework

### Development Crates

#### `dolos-testing` (Testing Utilities)
- **Purpose**: Testing utilities and mock implementations
- **Features**:
  - `ToyDomain`: Minimal in-memory `Domain` implementation for testing
  - Test data generation (fake UTxOs, blocks, deltas)
  - Test address utilities
- **Role**: Development and testing support

#### `xtask` (Development Automation)
- **Purpose**: Development task automation following cargo-xtask pattern
- **Role**: Build scripts and development utilities, including `cargo xtask archive-bench` — the archive segment benchmarks (store-level presets, node-level paired runs of two `dolos` binaries, dictionary tooling, report); see `xtask/archive-bench/README.md`

## Dependency Flow

```
dolos (main binary)
├── dolos-core (foundation)
├── dolos-cardano (Cardano logic) → dolos-core
├── dolos-redb3 (storage) → dolos-core
├── dolos-fjall (storage) → dolos-core
├── dolos-minibf (API) → dolos-core + dolos-cardano
├── dolos-trp (TX resolver) → dolos-core + dolos-cardano (Tx3 integration)
└── dolos-testing (dev) → dolos-core + dolos-cardano + dolos-redb3
```

## Architecture Patterns

### Layered Architecture
1. **Core Layer** (`dolos-core`): Abstract traits and interfaces
2. **Implementation Layer** (`dolos-cardano`, `dolos-redb3`, `dolos-fjall`): Concrete implementations
3. **Service Layer** (`dolos-minibf`, `dolos-trp`): API services
4. **Application Layer** (`dolos`): Main binary and CLI

### Work Unit Pipeline

Dolos processes blockchain data through a pipeline of **work units**. Each work unit functions as a mini-ETL job that extracts data from storage, transforms it using chain-specific logic, and loads results into the appropriate stores (state, archive).

#### WorkUnit Trait

The `WorkUnit<D: Domain>` trait (`dolos-core/src/work_unit.rs`) defines the contract for all processing units. An **executor** component manages each work unit's lifecycle by calling methods in a specific sequence:

1. `load()` - Extract required data from storage (UTxOs, entities)
2. `compute()` - Perform chain-specific transformations
3. `commit_wal()` - Write to WAL for crash recovery
4. `commit_state()` - Persist state changes to StateStore
5. `commit_archive()` - Persist block data, with the index entries it projects, to ArchiveStore

The executor implementations live in `dolos-core/src/sync.rs` (full lifecycle) and `dolos-core/src/import.rs` (bulk import, skips WAL).

#### Cardano Work Units

The `CardanoWorkUnit` enum (`dolos-cardano/src/lib.rs`) defines Cardano-specific work unit variants:

- `GenesisWorkUnit` - Bootstrap chain from genesis configuration
- `RollWorkUnit` - Process block batches (primary work unit)
- `RupdWorkUnit` - Compute rewards at stability window
- `EwrapWorkUnit` - Apply computed rewards at epoch end
- `EstartWorkUnit` - Handle era transitions at epoch start

Each variant implements `WorkUnit` and determines which stores it modifies during its commit phases.

### Domain Trait
The `Domain` trait is the central abstraction that ties all components together:

```rust
pub trait Domain: Send + Sync + Clone + 'static {
    type Entity: Entity;
    type EntityDelta: EntityDelta<Entity = Self::Entity>;
    type Chain: ChainLogic<Delta = Self::EntityDelta, Entity = Self::Entity>;

    type Wal: WalStore<Delta = Self::EntityDelta>;
    type State: StateStore;
    type Archive: ArchiveStore;
    type Mempool: MempoolStore;
    type TipSubscription: TipSubscription;

    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn mempool(&self) -> &Self::Mempool;
    // ... configuration and chain access methods
}
```

### Writer Pattern (Transactional Batching)
All storage traits follow a consistent pattern for batched, atomic writes:

```rust
// 1. Start a writer (begins transaction)
let writer = store.start_writer()?;

// 2. Perform multiple operations
writer.apply_something(&data)?;
writer.apply_another(&more_data)?;

// 3. Commit atomically (consumes the writer)
writer.commit()?;
```

This pattern is used by:
- `StateStore` → `StateWriter`
- `ArchiveStore` → `ArchiveWriter`

### Entity-Delta Pattern
State mutations use a reversible delta pattern:

```rust
pub trait EntityDelta {
    type Entity: Entity;
    
    fn key(&self) -> NsKey;                           // Namespace + key
    fn apply(&mut self, entity: &mut Option<Self::Entity>);  // Forward application
    fn undo(&self, entity: &mut Option<Self::Entity>);       // Rollback
}
```

- Entities are keyed by `NsKey(Namespace, EntityKey)`
- Deltas describe changes, not final states
- `apply()` can store "before" values for later `undo()`
- Enables efficient rollbacks without full state snapshots

### QueryHelpers and Lazy Iteration
The `QueryHelpers` trait (auto-implemented for all `Domain` types) joins index lookups with archive fetches:

```rust
// Index returns slots, QueryHelpers fetches the actual blocks
fn blocks_with_address(&self, address, start, end) -> SparseBlockIter;
```

`SparseBlockIter` is lazy - it only fetches blocks from archive when iterated, enabling efficient pagination and early termination.

### Trait-Based Extensibility
- `ChainLogic` trait allows different blockchain implementations
- `StateStore`, `ArchiveStore`, `WalStore` for storage components
- `MempoolStore` for transaction mempool
- Service feature flags enable modular functionality

## Key Design Decisions

- **Lightweight Architecture**: Intentionally avoids full consensus validation for minimal resource usage
- **Trust Model**: Relies on trusted upstream peers rather than independent validation
- **Indexes Beside What They Project**: Each index is a projection, so it lives in the store holding its source and commits in the same batch — no separate database, no third cursor, no cross-store join to keep in sync
- **Primitive-Value Indexes**: Index queries return slots/refs rather than full data; join with archive separately via `QueryHelpers`
- **Batched Writes**: All storage writes go through transactional writers for atomicity and performance
- **Entity-Delta System**: State changes are represented as reversible deltas for efficient rollbacks
- **Parallel Processing**: Batch operations use Rayon for parallel UTxO decoding and entity loading
- **Modular Services**: Different API endpoints (gRPC, Blockfrost, TRP) can be enabled/disabled via features
- **Future Extensibility**: Architecture supports planned P2P features and light consensus validation

This crate organization enables Dolos to serve as a lightweight, efficient Cardano data node while maintaining flexibility for different use cases and future enhancements.

## Agent Development Guidelines

### Code Verification Requirements

All agents working on this repository must verify their modifications by running the following checks before considering any changes complete:

1. **Clippy Linting**: Run `cargo clippy` and ensure no warnings appear
   ```bash
   cargo clippy --workspace --all-targets --all-features
   ```

2. **Clean Build**: Ensure the project builds without warnings
   ```bash
   cargo build --workspace --all-targets --all-features
   ```

3. **Testing**: Run tests to verify functionality
   ```bash
   cargo test --workspace --all-targets
   cargo test --workspace --all-features --exclude dolos-minibf --exclude dolos-minikupo --exclude dolos-trp
   ```

   The first command is what CI runs on every platform. The second adds the
   feature-gated code the default run never exercises — most importantly the
   `strict` feature, dolos-cardano's epoch-coherence assertions. The three
   service crates are excluded from the all-features run because their test
   fixtures import synthetic chains that jump from a fresh genesis domain
   straight to epoch 2, which trips those assertions inside the fixture itself
   (the `EpochState` entity is still at epoch 0 when an epoch-2 block rolls);
   they keep full coverage under the first command. Remove the exclusions once
   the fixtures build epoch-coherent domains. CI (`.github/workflows/ci.yml`)
   runs both commands, so a verification that passes locally cannot drift from
   what the repository keeps green.

4. **Registry round trip** (requires Docker): the `#[ignore]`d suites that
   spawn a real OCI registry
   ```bash
   cargo test -p dolos-snapshot --test publish -- --ignored --test-threads=1
   cargo test -p dolos-snapshot --test restore_registry -- --ignored --test-threads=1
   ```

   Each test spawns its own registry container via `docker run` and tears it
   down on the way out; the suites are `#[ignore]`d so plain `cargo test`
   stays green without a container runtime. Run them when touching the
   stelae pin or `crates/snapshot`'s registry publish/restore paths;
   `STELAE_TEST_REGISTRY_IMAGE` selects the server.

   These are local verification tools, deliberately not a CI job here.
   Registry interaction — transport and publish lifecycle — is implemented
   by the stelae crates, so testing that integration in CI is
   `github.com/txpipe/stelae`'s responsibility, and its `Registry` workflow
   runs against `registry:2`, `registry:3` and a pinned `zot`. Dolos's test
   subject is the profile, and the profile is transport-blind by
   construction: the directory-transport suites in the workspace gate cover
   it, and these two suites exist to double-check the composition when the
   seam itself is in question.

### Code Quality Standards

- All warnings from `cargo clippy` must be resolved before committing changes
- Code should follow existing Rust conventions and patterns established in the codebase
- New implementations should follow the trait-based architecture patterns
- Storage implementations that share a trait should maintain the same observable contract across backends

These verification steps ensure code quality, maintain consistency across storage backends, and prevent introducing technical debt into the codebase.
