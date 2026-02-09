# Dolos Testing

Testing utilities and mock implementations for the Dolos Cardano data node.

## Overview

`dolos-testing` provides comprehensive testing infrastructure for Dolos, including mock implementations, test data generation, and utilities to facilitate unit and integration testing across all Dolos components.

## Features

### Toy Domain
- **Purpose**: Minimal in-memory implementation for testing
- **Components**:
  - `ToyDomain`: In-memory storage implementation
- **Use Case**: Fast unit tests without external dependencies

### Test Data Generation
- **Block Generation**: Realistic test block creation
- **Transaction Generation**: Varied transaction scenarios
- **State Mocks**: Pre-configured ledger states
- **Genesis Data**: Test genesis configurations

### Utilities
- **Test Harness**: Common setup/teardown procedures
- **Assertions**: Dolos-specific test assertions
- **Properties**: Property-based testing support

## Dependencies

- **dolos-core**: Core trait implementations for testing
- **dolos-cardano**: Cardano-specific test utilities
- **proptest**: Property-based testing framework
- **quickcheck**: Randomized testing support
- **tokio-test**: Async testing utilities

## Related Crates

- [`dolos-core`](../dolos-core/README.md) - Core traits being tested
- [`dolos-cardano`](../dolos-cardano/README.md) - Cardano logic under test
- [`dolos-redb3`](../dolos-redb3/README.md) - Storage backend testing
