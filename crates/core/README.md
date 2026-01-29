# Dolos Core

Foundation library providing core traits, types, and abstractions for the Dolos Cardano data node.

## Overview

`dolos-core` serves as the architectural foundation for the entire Dolos project, defining the essential traits and abstractions that enable modular, extensible blockchain data processing.

## Core Components

### Domain Abstraction
- **`Domain`**: High-level abstraction that combines all storage and processing components
- Enables pluggable storage backends and blockchain logic implementations

### Chain Logic Trait
- **`ChainLogic`**: Trait defining blockchain processing behavior
- Allows different blockchain implementations (Cardano, test chains, etc.)

### Storage Traits
- **`StateStore`**: Ledger state storage interface
- **`ArchiveStore`**: Historical block storage interface
- **`WalStore`**: Write-Ahead Log for crash recovery
- **`MempoolStore`**: Transaction mempool interface

### Key Types
- Common data structures shared across all Dolos components
- Error handling types and utilities
- Configuration abstractions

## Usage

This crate is primarily used as a dependency by other Dolos crates:

```rust
use dolos_core::{Domain, ChainLogic};

// Implement ChainLogic for a specific blockchain
struct MyBlockchainLogic;

impl ChainLogic for MyBlockchainLogic {
    // Implementation details
}
```

## Design Philosophy

- **Trait-based architecture**: Enables pluggable components and testing
- **Separation of concerns**: Clear boundaries between storage, processing, and application logic
- **Extensibility**: Designed to support multiple blockchain implementations

## Dependencies

Minimal dependencies - focuses on providing abstractions rather than concrete implementations.

## Related Crates

- [`dolos-cardano`](../dolos-cardano/README.md) - Cardano-specific ChainLogic implementation
- [`dolos-redb3`](../dolos-redb3/README.md) - Storage backend implementing these traits
- [`dolos-testing`](../dolos-testing/README.md) - Test utilities and mock implementations
