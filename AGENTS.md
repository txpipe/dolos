# Dolos: Cardano Data Node - Rust Crate Organization

## Project Overview

Dolos is a lightweight Cardano node designed specifically for keeping an updated copy of the ledger and responding to queries from trusted clients while requiring minimal resources compared to a full node. It serves as a data provider rather than a consensus-validating node, focusing on efficiency and compatibility with existing Cardano ecosystem tools.

## Storage Concepts

Dolos uses four distinct storage backends, each serving a specific purpose:

### StateStore
- **Purpose**: Current ledger state (the "world view")
- **Contents**: UTxO set, entity state, chain cursor position
- **Traits**: `StateStore` (reads) + `StateWriter` (batched writes)
- **Database**: `<storage.path>/state`

### ArchiveStore
- **Purpose**: Historical block storage with temporal indexing
- **Contents**: Raw blocks indexed by slot, entity logs keyed by `LogKey` (slot + entity key)
- **Traits**: `ArchiveStore` (reads) + `ArchiveWriter` (batched writes)
- **Database**: `<storage.path>/chain`

### WalStore (Write-Ahead Log)
- **Purpose**: Crash recovery and rollback support
- **Contents**: Log entries with block data, entity deltas, and input UTxOs
- **Traits**: `WalStore`
- **Database**: `<storage.path>/wal`

### IndexStore
- **Purpose**: Cross-cutting indexes for fast lookups
- **Contents**: Two types of indexes:
  - **UTxO Filter Indexes**: Current state queries (by address, payment, stake, policy, asset)
  - **Archive Indexes**: Historical queries (by block hash, tx hash, slots with address/asset/etc.)
- **Traits**: `IndexStore` (reads) + `IndexWriter` (batched writes)
- **Database**: `<storage.path>/index` (isolated from other stores)
- **Design Note**: Returns primitive values (slots, UTxO refs) not block data. Use `QueryHelpers` to join with archive for full data.

### Database File Organization

```
<storage.path>/
├── wal      # Write-Ahead Log database
├── state    # Ledger state database
├── chain    # Archive/block storage database
└── index    # Consolidated index database
```

Each database is a separate Redb file with independent configuration for cache size and durability.

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
  - `indexes`: `IndexStore` and `IndexWriter` traits for cross-cutting indexes
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
- **Purpose**: Storage backend implementation using the Redb v3 embedded database
- **Components**:
  - `state`: `StateStore` implementation with UTxO storage and entity tables
  - `archive`: `ArchiveStore` implementation with block and log storage
  - `wal`: `WalStore` implementation for crash recovery
  - `indexes`: `IndexStore` implementation (isolated database) with:
    - UTxO filter indexes (by address, payment, stake, policy, asset)
    - Archive indexes (by block hash, tx hash, address, asset, datum, etc.)
- **Role**: Persistence layer implementing the core storage traits

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
- **Role**: Build scripts and development utilities

## Dependency Flow

```
dolos (main binary)
├── dolos-core (foundation)
├── dolos-cardano (Cardano logic) → dolos-core
├── dolos-redb3 (storage) → dolos-core
├── dolos-minibf (API) → dolos-core + dolos-cardano
├── dolos-trp (TX resolver) → dolos-core + dolos-cardano (Tx3 integration)
└── dolos-testing (dev) → dolos-core + dolos-cardano + dolos-redb3
```

## Architecture Patterns

### Layered Architecture
1. **Core Layer** (`dolos-core`): Abstract traits and interfaces
2. **Implementation Layer** (`dolos-cardano`, `dolos-redb3`): Concrete implementations
3. **Service Layer** (`dolos-minibf`, `dolos-trp`): API services
4. **Application Layer** (`dolos`): Main binary and CLI

### Work Unit Pipeline

Dolos processes blockchain data through a pipeline of **work units**. Each work unit functions as a mini-ETL job that extracts data from storage, transforms it using chain-specific logic, and loads results into the appropriate stores (state, archive, index).

#### WorkUnit Trait

The `WorkUnit<D: Domain>` trait (`dolos-core/src/work_unit.rs`) defines the contract for all processing units. An **executor** component manages each work unit's lifecycle by calling methods in a specific sequence:

1. `load()` - Extract required data from storage (UTxOs, entities)
2. `compute()` - Perform chain-specific transformations
3. `commit_wal()` - Write to WAL for crash recovery
4. `commit_state()` - Persist state changes to StateStore
5. `commit_archive()` - Persist block data to ArchiveStore
6. `commit_indexes()` - Update IndexStore

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
    type Indexes: IndexStore;
    type Mempool: MempoolStore;
    type TipSubscription: TipSubscription;

    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn indexes(&self) -> &Self::Indexes;
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
- `IndexStore` → `IndexWriter`

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
- `StateStore`, `ArchiveStore`, `IndexStore`, `WalStore` for storage components
- `MempoolStore` for transaction mempool
- Service feature flags enable modular functionality

## Key Design Decisions

- **Lightweight Architecture**: Intentionally avoids full consensus validation for minimal resource usage
- **Trust Model**: Relies on trusted upstream peers rather than independent validation
- **Separate Index Database**: Indexes live in their own database file (`index`) for independent scaling, tuning, and rebuilding without touching primary data
- **Primitive-Value Indexes**: Index queries return slots/refs rather than full data; join with archive separately via `QueryHelpers`
- **Batched Writes**: All storage writes go through transactional writers for atomicity and performance
- **Entity-Delta System**: State changes are represented as reversible deltas for efficient rollbacks
- **Parallel Processing**: Batch operations use Rayon for parallel UTxO decoding and entity loading
- **Modular Services**: Different API endpoints (gRPC, Blockfrost, TRP) can be enabled/disabled via features
- **Future Extensibility**: Architecture supports planned P2P features and light consensus validation

This crate organization enables Dolos to serve as a lightweight, efficient Cardano data node while maintaining flexibility for different use cases and future enhancements.
