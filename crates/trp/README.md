# Dolos TRP

Transaction Resolver Protocol service implementing the Tx3 framework for transaction processing.

## Overview

`dolos-trp` provides a JSON-RPC server that implements the Transaction Resolver Protocol (TRP), integrating with the Tx3 framework to handle transaction resolution, validation, and submission for Cardano transactions.

## Features

### Tx3 Framework Integration
- **Transaction Resolution**: Resolves transaction inputs and dependencies
- **Validation**: Transaction validation against current ledger state
- **Submission**: Transaction submission to the Cardano network
- **Status Tracking**: Transaction status monitoring and updates

### JSON-RPC API
- **Standard Interface**: JSON-RPC 2.0 compliant endpoints
- **Method Mapping**: Tx3 method implementations for Cardano
- **Error Handling**: Standardized error responses and codes
- **Batch Processing**: Support for batch transaction operations

### Protocol Methods
- **resolve**: Resolve transaction inputs and calculate fees
- **validate**: Validate transaction against current state
- **submit**: Submit transaction to the mempool
- **status**: Check transaction submission status
- **estimate**: Estimate transaction fees and gas

## Architecture

### JSON-RPC Server
- **Framework**: Built on JSON-RPC standards
- **Validation**: Request/response validation
- **Concurrency**: Parallel request processing

### Tx3 Integration
- **Transaction Builder**: Construct Cardano transactions
- **Input Resolution**: Resolve UTxO inputs and references
- **Fee Calculation**: Calculate accurate transaction fees
- **Witness Management**: Handle transaction witnesses

### State Access
- **Mempool Integration**: Interface with Cardano mempool
- **State Queries**: Access current ledger state
- **Validation**: Transaction validation logic
- **Submission**: Network transaction submission

## Dependencies

- **dolos-core**: Core domain and storage abstractions
- **dolos-cardano**: Cardano-specific transaction logic
- **tx3-sdk**: Tx3 framework integration
- **jsonrpsee**: JSON-RPC server implementation
- **tokio**: Async runtime

## Performance

- **Low Latency**: Sub-millisecond transaction resolution
- **High Throughput**: Thousands of transactions per second
- **Concurrent Processing**: Parallel transaction validation
- **Memory Efficient**: Streaming transaction processing

## Integration

### Tx3 Ecosystem
- **Tx3 SDK**: Full compatibility with Tx3 client libraries
- **Developer Tools**: Integration with Tx3 development toolchain
- **Framework Support**: Works with Tx3-supported languages

### Cardano Integration
- **Mempool**: Direct integration with Cardano mempool
- **Network**: Transaction submission to Cardano network
- **State**: Access to real-time ledger state

## Related Crates

- [`dolos-core`](../dolos-core/README.md) - Core storage abstractions
- [`dolos-cardano`](../dolos-cardano/README.md) - Cardano transaction processing
