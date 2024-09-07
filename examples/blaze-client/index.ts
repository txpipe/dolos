// Import Blaze SDK and U5C provider
import { Core, U5C } from "@blaze-cardano/sdk";

// Create a new U5C provider pointing to the local Dolos instance
const provider = new U5C({
  url: "http://localhost:50051",
});

// Query Utxos for the given address (the address in the example is a preview address randomly chosen from an explorer, use your own address)
const utxos = await provider.getUnspentOutputs(
  Core.Address.fromBech32(
    "addr_test1vpetczxy5uc9tkkqhrxgj6t0sggthyg8dd0qp22fte6wdtgvau4rn"
  )
);

// Log the UTXOs to the console
utxos.map((utxo) => {
  console.log(utxo.toCbor());
});
