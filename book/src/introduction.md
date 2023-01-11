# Dolos

Dolos is a new type of Cardano node, fine-tuned to solve a very narrow scope: keeping an updated copy of the ledger and replying to queries from trusted clients, while requiring a small fraction of the resources

# Motivation
Cardano nodes can assume one of two roles:

- block producer: in charge of minting blocks
- relay node: in charge relaying blocks from / to peers.

Each of these roles has concrete responsibilities and runtime requirements. Criteria such as network topology, resource allocation, backup procedures, etc vary by role.

We argue that there’s a 3rd role that should be treated independently with the goal of optimizing its workload: nodes that are used with the sole purpose of resolving local state queries or serving as data source for downstream tools that require ledger data.

There are many potential optimizations for nodes performing this type of workload that are not currently possible with the Cardano node:

- drastically limiting the amount of memory required to execute the node
- switching to storage solutions with different trade-offs (eg: S3, NFS, etc)
- providing alternative wire protocols more friendly for data queries (eg: REST, gRPC)
- providing an auth layer in front of the API endpoints  

The goal of this project is to provide a very limited and focused version of the Cardano node that can be used by DevOps as a cost-effective, performant option to deploy **data nodes** side-by-side with the producer / relay nodes.

This new role would be useful in the following scenarios:

- As data source for well-known tools such as DB-sync, Ogmios, CARP, Oura, etc.
- As a fast, low resource node for syncing other producer / relay nodes.
- As a ledger data source that scales dynamically according to query load.
- As a node that leverages network / cloud storage technology instead of mounted drives.
- As a node that scales horizontally, allowing high-availability topologies.
- As a low resource local node for resolving local state queries.

# Detailed design
Data nodes will share some of the features with the mainstream Cardano node:

- Node-to-Node and Node-to-Client Chain-Sync mini-protocol
- Node-to-Node Block-Fetch mini-protocol
- Node-to-Client Local-State-Query mini-protocol

This new type of node will also provide features not currently available in the mainstream Cardano node:

- HTTP/JSON endpoint for common local state queries
- gRPC endpoint for local state queries and chain-sync procedure
- Different storage options including NFS, S3 & GCP Buckets
- Low memory consumption (allowed by the trade-offs in scope) 

# Drawbacks
- Although the scope is very narrow compared to a real, full-blown node, this tool has a large LoE.
- There's overlap with some TxPipe tools such as Oura and Scrolls. The mitigation plan is to hoist individual components into Pallas to achieve DRY.
- Some components, such as the gRPC interface, might be useful even in environment running the full-blown Cardano node. To mitigate this we will architect the system in such a way that different entry-points (aka: binaries) can perform different roles. The gRPC bridge would be one of this.

# Alternatives
- Use the full-blown Cardano node even for the scenarios described in this RFC.
- Split the project into sub-components that can be orchestrated to achieve the same result.

# Unresolved questions
- Performance gains and resource allocation optimizations are theoretical, these were extrapolated from our experience implementing Cardano data processing pipelines using components written in Rust. We won’t have a strict, quantifiable measurement until we develop a PoC of this project. To mitigate this issue, our development process will include performance benchmarks execution at each development milestone. Reports will be included as part of each release.
- There’s some documentation lacking regarding local state queries wire-format which will need some reverse engineering from the mainstream Cardano node. We have experience with this approach but the level-of-effort associated with the task is hard to anticipate. To try mitigate this issue, we'll reach out to IOG for advise and documentation in case it's available.
