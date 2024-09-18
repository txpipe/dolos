// Step #1
// Import Blaze SDK and U5C provider
import {
  Bip32PrivateKey,
  mnemonicToEntropy,
  wordlist,
} from "@blaze-cardano/core";
import { HotWallet, Core, Blaze } from "@blaze-cardano/sdk";
import { U5C } from "@utxorpc/blaze-provider";

async function main() {
  // Step #2
  // Create a new U5C provider
  // In this example we use Demeter hosted UTXO provider
  // but you can run a local Dolos https://github.com/txpipe/dolos instance and connect to its UTxO endpoint
  // If this is the case then you can remove the headers field
  const provider = new U5C({
    url: "https://preview.utxorpc-v0.demeter.run",
    headers: {
      "dmtr-api-key": "dmtr_utxorpc19r0r7x8stkzejplyyra8n6d70gw276un",
    },
  });

  // Step #3
  // Create a new wallet from a mnemonic
  const mnemonic =
    "end link visit estate sock hurt crucial forum eagle earn idle laptop wheat rookie when hard suffer duty kingdom clerk glide mechanic debris jar";
  const entropy = mnemonicToEntropy(mnemonic, wordlist);
  const masterkey = Bip32PrivateKey.fromBip39Entropy(Buffer.from(entropy), "");
  const wallet = await HotWallet.fromMasterkey(masterkey.hex(), provider);

  // Step #4
  // Create a Blaze instance from the wallet and provider
  const blaze = await Blaze.from(provider, wallet);

  // Optional: Print the wallet address
  console.log("Wallet address", wallet.address.toBech32());

  // Optional: Print the wallet balance
  console.log("Wallet balance", (await wallet.getBalance()).toCore());

  // Step #5
  // Create a example transaction that sends 5 ADA to an address
  const tx = await blaze
    .newTransaction()
    .payLovelace(
      Core.Address.fromBech32(
        "addr_test1qrnrqg4s73skqfyyj69mzr7clpe8s7ux9t8z6l55x2f2xuqra34p9pswlrq86nq63hna7p4vkrcrxznqslkta9eqs2nsmlqvnk",
      ),
      5_000_000n,
    )
    .complete();

  // Step #6
  // Sign the transaction
  const signexTx = await blaze.signTransaction(tx);

  // Step #7
  // Submit the transaction to the blockchain network
  const txId = await blaze.provider.postTransactionToChain(signexTx);

  // Optional: Print the transaction ID
  console.log("Transaction ID", txId);
}

main().catch(console.error);
