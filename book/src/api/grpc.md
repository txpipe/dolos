# gRPC


## Introduction

- Dolos exposes a gRPC endpoint allowing clients to query data
- The endpoint adheres to the UTxO RPC interface definition found in https://utxorpc.org
- The current implemented module is the `Sync` module that allows clients to sync with the state of the chain stored by Dolos
- Dolos endpoint also supports gRPC-web, a variant of gRPC that can be used directly from browsers

## Connecting to the server

- Once started, the server exposes TCP port
- The default port number is `50051`, but can be changed via configuration
- This port accepts http/2 connections following the standard gRPC mechanism
- The port also accepts http/1.1 connections following the gRPC-web protocol
- Developers can make use of [UTxO-RPC](https://utxorpc.org) SDK libraries to interact with the endpoint programmatically

### Authentication Mechanism

- Dolos has a built-in mechanism for authenticating clients using TLS
- By specifying a specific CA authority, Dolos can allows clients that provide a matching certificate
- The CA authority is specified by pointing to the corresponding `.pem` file through configuration