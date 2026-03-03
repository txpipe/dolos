# Dolos Cardano

Cardano-specific implementation of the Dolos core traits for blockchain processing and validation.

## Overview

`dolos-cardano` provides the concrete implementation of Cardano blockchain logic, implementing the `ChainLogic` trait from `dolos-core` with Cardano-specific behavior including block processing, validation, and era handling.

## Key Components

### CardanoLogic
- **Purpose**: Main implementation of the `ChainLogic` trait for Cardano
- **Features**:
  - Block validation and processing
  - Era-specific logic handling (Shelley, Allegra, Mary, Alonzo, etc.)
  - Transaction validation
  - State transitions

### Block Processing
- **Chain Synchronization**: Following the Cardano chain tip
- **Block Validation**: Cryptographic and rule-based validation
- **Era Boundaries**: Handling transitions between Cardano eras
- **Protocol Updates**: Supporting Cardano protocol parameter changes

### Genesis Configuration
- **Shelley Genesis**: Network parameters and initial state
- **Byron Genesis**: Legacy era support
- **Configuration Management**: Dynamic protocol parameter updates

### Reward Distribution
- **Stake Pool Rewards**: Calculating and distributing rewards
- **Epoch Boundaries**: Processing epoch transitions
- **Treasury Management**: Handling treasury funds

## Usage

```rust
use dolos_cardano::CardanoLogic;
use dolos_core::ChainLogic;

// Create Cardano logic implementation
let cardano_logic = CardanoLogic::new(genesis_config);

// Use as ChainLogic trait object
let logic: Box<dyn ChainLogic> = Box::new(cardano_logic);
```

## Dependencies

- **dolos-core**: Core traits and abstractions
- **Pallas**: Cardano protocol implementation library
- **Cryptographic libraries**: For Cardano-specific cryptographic operations

## Design Considerations

- **Lightweight Validation**: Focused on data processing rather than full consensus
- **Trust Model**: Relies on trusted peers for block validity
- **Efficiency**: Optimized for minimal resource usage
- **Compatibility**: Maintains compatibility with Cardano mainnet and testnets

## Related Crates

- [`dolos-core`](../dolos-core/README.md) - Core traits this crate implements
- [`dolos-redb3`](../dolos-redb3/README.md) - Storage backend for Cardano data
- [`dolos-minibf`](../dolos-minibf/README.md) - API layer exposing Cardano data

For more information about the overall architecture, see the main [AGENTS.md](../../AGENTS.md) documentation.