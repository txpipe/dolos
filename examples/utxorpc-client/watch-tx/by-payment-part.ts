// Watch transactions by an address's PAYMENT CREDENTIAL rather than the exact
// address.
//
// A Cardano address is a payment credential plus (usually) a staking credential.
// Filtering on just the payment part matches every address that shares the same
// payment key — the base address, the enterprise address, and every variant
// delegated to a different stake pool. This is what you usually want when you're
// tracking "a wallet" rather than one specific address string.
//
//   ADDRESS=addr_test1... npm run watch:payment

import {
  watchClient,
  paymentPartOf,
  runStream,
  toHex,
  startPoint,
  DEFAULT_ADDRESS,
  c,
  printTx,
} from "./shared";

const ADDRESS = process.env.ADDRESS ?? DEFAULT_ADDRESS;

async function main() {
  const paymentPart = paymentPartOf(ADDRESS);
  console.log(
    `👀 watching txs on payment credential ${c.txHash(toHex(paymentPart))}\n` +
      `   ${c.dim("(every address sharing this payment key, any staking part)")}\n`,
  );

  await runStream(watchClient().watchTxForPaymentPart(paymentPart, startPoint()), printTx);
}

main();
