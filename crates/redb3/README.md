# Dolos Redb3

Storage backend implementation for Dolos using the Redb v3 embedded database.

## Overview

`dolos-redb3` provides a concrete implementation of the storage traits from `dolos-core` using Redb v3, an embedded key-value database optimized for performance and reliability in blockchain applications.

## Storage Components

### State Store (`state`)
- **Purpose**: Stores the current ledger state
- **Features**:
  - UTxO set management
  - Account state tracking
  - Protocol parameters storage
  - Fast point-in-time queries

### Archive Store (`archive`)
- **Purpose**: Historical block storage
- **Features**:
  - Complete block history retention
  - Efficient block retrieval by hash or slot
  - Compact storage format
  - Configurable retention policies

### Write-Ahead Log (`wal`)
- **Purpose**: Crash recovery and durability
- **Features**:
  - Atomic transaction logging
  - Recovery from unexpected shutdowns
  - Log compaction and cleanup
  - Consistency guarantees

## Key Features

### Performance
- **Embedded Design**: No external database dependencies
- (planned) **Compression**: Configurable data compression
- **Concurrent Access**: Multi-threaded read/write operations

### Reliability
- **ACID Transactions**: Full transaction support
- **Backup Support**: Hot backup capabilities

## Data Organization

### Schema Design
- **Tables**: Organized by data type and access patterns
- **Indexes**: Optimized for common blockchain queries
- **Keys**: Efficient binary encoding for fast lookups

### Storage Layout
```
data/
├── state
├── archive
└── wal
```

## Dependencies

- **dolos-core**: Storage trait implementations
- **redb**: v3 embedded database engine
- **tokio**: Async runtime integration
- **serde**: Data serialization

## Performance Characteristics

- **Write Throughput**: Optimized for high-frequency blockchain updates
- **Read Latency**: Sub-millisecond query responses
- **Storage Efficiency**: Compressed storage with configurable trade-offs
- **Memory Usage**: Bounded memory footprint with caching

## Related Crates

- [`dolos-core`](../dolos-core/README.md) - Storage traits this crate implements
- [`dolos-cardano`](../dolos-cardano/README.md) - Provides data for storage
