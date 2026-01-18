# Dolos: Cardano Data Node - Rust Crate Organization

## Project Overview

Dolos is a lightweight Cardano node designed specifically for keeping an updated copy of the ledger and responding to queries from trusted clients while requiring minimal resources compared to a full node. It serves as a data provider rather than a consensus-validating node, focusing on efficiency and compatibility with existing Cardano ecosystem tools.

## Crate Architecture

The project follows a modular workspace architecture with clear separation of concerns and trait-based extensibility.

### Core Crates

#### `dolos` (Main Binary)
- **Purpose**: Main application binary and CLI interface
- **Role**: Application layer that orchestrates all services
- **Features**: Configurable service compilation (grpc, minibf, trp, mithril, utils)
- **Documentation**: [README.md](README.md)

#### `dolos-core` (Foundation)
- **Purpose**: Core traits, types, and abstractions common to all Dolos components
- **Key Modules**:
  - `Domain`: High-level abstraction for storage backends
  - `ChainLogic`: Trait for blockchain processing logic
  - Storage traits: `Block`, `State`, `Archive`, `Wal`
  - `Mempool`: Transaction mempool interface
- **Role**: Foundation layer providing the architecture that other crates implement
- **Documentation**: [README.md](crates/core/README.md)

#### `dolos-cardano` (Blockchain Logic)
- **Purpose**: Cardano-specific implementation of the core traits
- **Components**:
  - `CardanoLogic`: Implementation of `ChainLogic` for Cardano
  - Block processing, validation, and era handling
  - Genesis configuration management
  - Reward distribution processing
- **Dependencies**: `dolos-core`, Pallas library for Cardano protocol support
- **Documentation**: [README.md](crates/cardano/README.md)

#### `dolos-redb3` (Storage Backend)
- **Purpose**: Storage backend implementation using the Redb v3 embedded database
- **Components**:
  - `state`: Ledger state storage
  - `archive`: Historical block storage
  - `wal`: Write-Ahead Log for crash recovery
- **Role**: Persistence layer implementing the core storage traits
- **Documentation**: [README.md](crates/redb3/README.md)

### Service Crates

#### `dolos-minibf` (Blockfrost API)
- **Purpose**: Blockfrost-compatible HTTP API service
- **Components**:
  - REST API endpoints mimicking Blockfrost
  - Cardano data mapping and transformation
  - HTTP server using Axum framework
- **Role**: API compatibility layer for existing Blockfrost clients
- **Documentation**: [README.md](crates/minibf/README.md)

#### `dolos-trp` (Transaction Resolver Protocol)
- **Purpose**: Transaction Resolver Protocol implementation (Tx3 framework integration)
- **Components**:
  - JSON-RPC server for transaction processing
  - Integration with Tx3 SDK for transaction resolution
- **Role**: Transaction processing service leveraging the Tx3 framework
- **Documentation**: [README.md](crates/trp/README.md)

### Development Crates

#### `dolos-testing` (Testing Utilities)
- **Purpose**: Testing utilities and mock implementations
- **Features**:
  - `toy-domain`: Minimal in-memory implementation for testing
  - Test data generation and utilities
- **Role**: Development and testing support
- **Documentation**: [README.md](crates/testing/README.md)

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

### Domain-Driven Design
- The `Domain` trait in `dolos-core` defines the high-level architecture
- Different implementations can plug in different storage backends
- Clear separation between blockchain logic (`ChainLogic`) and storage (`Domain`)

### Trait-Based Extensibility
- `ChainLogic` trait allows different blockchain implementations
- `MempoolStore`, `StateStore`, `ArchiveStore`, `WalStore` for storage components
- Service feature flags enable modular functionality

## Key Design Decisions

- **Lightweight Architecture**: Intentionally avoids full consensus validation for minimal resource usage
- **Trust Model**: Relies on trusted upstream peers rather than independent validation
- **Modular Services**: Different API endpoints (gRPC, Blockfrost, TRP) can be enabled/disabled via features
- **Future Extensibility**: Architecture supports planned P2P features and light consensus validation

This crate organization enables Dolos to serve as a lightweight, efficient Cardano data node while maintaining flexibility for different use cases and future enhancements.