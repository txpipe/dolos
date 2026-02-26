//! Redis-backed storage for Dolos mempool.
//!
//! Provides a `MempoolStore` implementation backed by Redis with
//! leader election for the `confirm()` operation to support multi-node
//! deployments.

pub mod mempool;

pub use mempool::RedisMempool;
