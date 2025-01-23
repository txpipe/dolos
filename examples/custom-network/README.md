# Custom Network Example

This example demonstrates how run a custom network with Dolos. This example folder contains config and genesis files for a custom network which were generated using [Yaci DevKit](https://github.com/bloxbean/yaci-devkit).

## Procedure

To run this example, first you need to "bootstrap" the instance by running the following command:

```bash
cargo run -- bootstrap relay
```

This will initialize the data directory starting the chain from scratch using the genesis files.

Once the instance is bootstrapped, you can start the node by running the following command:

```bash
cargo run -- daemon
```

This will start the node and you'll be able to query the node using the different APIs (gRPC or Ouroboros).

Note that since there's no upstream node to support the custom network, the node will keep trying to connect to the upstream node and will keep failing. You can ignore the errors and the node will keep running.

## Example Query

You can query the protocol parameters of the custom network using the following command:

```bash
grpcurl -plaintext localhost:50051 utxorpc.v1alpha.query.QueryService.ReadParams
```

This will return the protocol parameters of the custom network.

```json
{
  "values": {
    "cardano": {
      "maxTxSize": "4096",
      "maxBlockHeaderSize": "2000000"
    }
  }
}
```

You can also search the UTxOs of the custom network using the following command:

```bash
grpcurl -plaintext -d '{
    "predicate": {
        "match": {
            "cardano": {
                "address": {
                    "exact_address": "gtgYWCWDWBxz2ZOaWfQqHzyjtqulb4LlLeKMcMvjaHDP9aGXoQJCGCoAGqp9dp8=",
                    "payment_part": "",
                    "delegation_part": ""
                }
            }
        }
    }
}' localhost:50051 utxorpc.v1alpha.query.QueryService.SearchUtxos
```

This will return the UTxOs of the address.

```json
{
  "items": [
    {
      "nativeBytes": "goLYGFglg1gcc9mTmln0Kh88o7arpW+C5S3ijHDL42hwz/Whl6ECQhgqABqqfXafGscUWwA=",
      "txoRef": {
        "hash": "jBywwGtI3EI4d1+B8ydmNPvCQxMTI76VdnBV7z0JVRU="
      },
      "cardano": {
        "address": "gtgYWCWDWBxz2ZOaWfQqHzyjtqulb4LlLeKMcMvjaHDP9aGXoQJCGCoAGqp9dp8=",
        "coin": "3340000000",
        "datum": {}
      }
    }
  ]
}
```

The `gtgYWCWDWBxz2ZOaWfQqHzyjtqulb4LlLeKMcMvjaHDP9aGXoQJCGCoAGqp9dp8=` is the address of the account created by the genesis files.