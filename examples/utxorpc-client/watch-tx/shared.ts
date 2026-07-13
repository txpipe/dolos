// Shared helpers for the WatchTx examples.
//
// These keep each example lean: connection, byte/hex plumbing, Cardano address
// rendering, and the (slightly fiddly) decoding of protobuf-wrapped integers.
// The examples themselves only have to describe the filter they care about.

import { CardanoSyncClient, CardanoWatchClient } from "@utxorpc/sdk";
import { Core } from "@blaze-cardano/sdk";
import pc from "picocolors";

// ---------------------------------------------------------------------------
// semantic styling
// ---------------------------------------------------------------------------
// A tiny layer over `picocolors` so every example uses the SAME color per
// concept (tx hash cyan, address magenta, ADA yellow, …). picocolors already
// no-ops when stdout isn't a TTY or `NO_COLOR` is set, so piping output to a
// file or `cat` yields plain text with no stray escape codes.
export const c = {
  dim: (s: string) => pc.dim(s),
  bold: (s: string) => pc.bold(s),
  txHash: (s: string) => pc.bold(pc.cyan(s)),
  address: (s: string) => pc.magenta(s),
  ada: (s: string) => pc.yellow(s),
  asset: (s: string) => pc.blue(s),
  fee: (s: string) => pc.dim(s),
  count: (s: string | number) => pc.bold(String(s)),
  // A short fixed-width colored tag, e.g. APPLY (green) / UNDO (yellow).
  badge: (label: string, color: "green" | "yellow" | "red") =>
    pc.bold(color === "green" ? pc.green(label) : color === "yellow" ? pc.yellow(label) : pc.red(label)),
};

// The UTxORPC endpoint exposed by your local Dolos instance.
export const URL = process.env.URL ?? "http://localhost:50051";

export const watchClient = () => new CardanoWatchClient({ uri: URL });
export const syncClient = () => new CardanoSyncClient({ uri: URL });

// A fixed intersection point: a real mainnet block this Dolos instance retains.
// The watch examples replay FORWARD from here, so filters produce matches
// immediately instead of waiting for the next block at the tip. The baked-in
// pattern defaults (policy ids / addresses) were all observed at or after this
// block, so they are guaranteed to reappear on replay.
//
// Override with `SLOT=<n> HASH=<hex>` to start elsewhere, or `NO_INTERSECT=1` to
// stream live from the current tip.
export const FIXED_INTERSECT = {
  slot: 192378839,
  hash: "11bf2d55f22f833c05526f5c639387e46be250e427eb8eb4569fb2e8a74c9070",
};

export function startPoint(): { slot: number | string; hash: string }[] | undefined {
  if (process.env.NO_INTERSECT) return undefined; // live from tip
  const { SLOT, HASH } = process.env;
  if (SLOT && HASH) return [{ slot: Number(SLOT), hash: HASH }];
  return [FIXED_INTERSECT];
}

// Pattern values seen at/after FIXED_INTERSECT during capture, used as the
// examples' defaults so they match on replay out of the box. Override with the
// ADDRESS / POLICY_ID env vars.
export const DEFAULT_ADDRESS =
  "addr1qxasnapjg46en92yqwydh8hnlznpgfrw3ksamx7s2vnpx37mhqv8f4lgc96cj6q4upk62yfa0qm3l5fr6er5z5s7p80s8nnsfx";
export const DEFAULT_POLICY = "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa"; // NIGHT

// ---------------------------------------------------------------------------
// bytes <-> hex
// ---------------------------------------------------------------------------

export const toHex = (bytes: Uint8Array): string => Core.toHex(bytes);
export const hexToBytes = (hex: string): Uint8Array =>
  Core.fromHex(hex.replace(/^0x/, ""));

// ---------------------------------------------------------------------------
// addresses
// ---------------------------------------------------------------------------

// Accepts either a bech32 address (`addr...`) or raw address bytes as hex, and
// returns the raw address bytes the SDK's watch filters expect.
//
// Gotcha: `Address.toBytes()` returns a hex STRING (a HexBlob), not a
// Uint8Array — so it has to go back through `Core.fromHex` before the SDK.
export function addrToBytes(input: string): Uint8Array {
  if (input.startsWith("addr")) {
    return Core.fromHex(Core.Address.fromBech32(input).toBytes());
  }
  return hexToBytes(input);
}

// The 28-byte payment credential of a bech32 address. Filtering on this matches
// every address that shares the same payment key — base, enterprise, and every
// stake-delegation variant — not just one exact address.
export function paymentPartOf(bech32: string): Uint8Array {
  const cred = Core.Address.fromBech32(bech32).getProps().paymentPart;
  if (!cred) throw new Error(`address has no payment credential: ${bech32}`);
  return Core.fromHex(cred.hash);
}

// Render raw address bytes (as they arrive on a decoded output) back to a
// human-readable bech32 string, falling back to hex if it can't be parsed.
export function renderAddr(bytes: Uint8Array): string {
  try {
    return Core.Address.fromBytes(Core.HexBlob.fromBytes(bytes)).toBech32();
  } catch {
    return toHex(bytes);
  }
}

// ---------------------------------------------------------------------------
// protobuf-wrapped integers (coin / asset quantities)
//
// `coin` and asset quantities are NOT plain JS bigints — they are a protobuf
// `BigInt` message with a oneof. The common case is `int`; very large values
// arrive as `bigUInt` / `bigNInt` byte blobs.
// ---------------------------------------------------------------------------

type ProtoBigInt =
  | { bigInt: { case: "int"; value: bigint } }
  | { bigInt: { case: "bigUInt"; value: Uint8Array } }
  | { bigInt: { case: "bigNInt"; value: Uint8Array } }
  | { bigInt: { case: undefined; value?: undefined } };

function unwrapBigInt(wrapped?: ProtoBigInt): bigint {
  const n = wrapped?.bigInt;
  if (!n || n.case === undefined) return 0n;
  if (n.case === "int") return n.value;
  const magnitude = n.value.length ? BigInt("0x" + Core.toHex(n.value)) : 0n;
  return n.case === "bigNInt" ? -magnitude : magnitude;
}

// Lovelace in a TxOutput's `coin` field.
export const lovelace = (coin?: ProtoBigInt): bigint => unwrapBigInt(coin);

// Pretty-print lovelace as ADA with 6 decimals.
export function ada(coin?: ProtoBigInt): string {
  const l = lovelace(coin);
  const neg = l < 0n;
  const abs = neg ? -l : l;
  const whole = abs / 1_000_000n;
  const frac = (abs % 1_000_000n).toString().padStart(6, "0");
  return `${neg ? "-" : ""}${whole}.${frac}`;
}

// Quantity of a single asset entry (works for both output and mint entries;
// mint quantities are negative for burns).
export function assetQty(asset: { quantity?: { value?: ProtoBigInt } }): bigint {
  return unwrapBigInt(asset.quantity?.value);
}

// ---------------------------------------------------------------------------
// tx rendering (shared by all examples)
// ---------------------------------------------------------------------------

// Render an asset name as ASCII when it's printable, otherwise fall back to hex.
function tryAscii(bytes: Uint8Array): string | null {
  if (bytes.length === 0) return null;
  let s = "";
  for (const b of bytes) {
    if (b < 0x20 || b > 0x7e) return null;
    s += String.fromCharCode(b);
  }
  return s;
}

function assetLabel(policyId: Uint8Array, name: Uint8Array): string {
  const nameHex = toHex(name);
  const ascii = tryAscii(name);
  const shown = ascii ? `${nameHex} ("${ascii}")` : nameHex || "(empty)";
  return `${shortPolicy(toHex(policyId))}.${shown}`;
}

// Truncate long hex strings (tx hashes, asset policies) to a readable window:
// `00112233…aabbccdd`.
function shortHex(hex: string, head = 8, tail = 8): string {
  return hex.length <= head + tail + 1 ? hex : `${hex.slice(0, head)}…${hex.slice(-tail)}`;
}

// Truncate a bech32 address to a readable window, e.g.
// `addr1qxasnapjg46en92…s8nnsfx`.
export function shortAddr(addr: string, head = 20, tail = 8): string {
  return addr.length <= head + tail + 1 ? addr : `${addr.slice(0, head)}…${addr.slice(-tail)}`;
}

const shortPolicy = (hex: string): string => shortHex(hex);

const DIVIDER = c.dim("─".repeat(72));

// Full multi-line breakdown of a matched tx: hash, fee, inputs (with resolved
// outputs), outputs with ADA + native assets, mint/burn, certificates. Used by
// every example so they all show the same level of detail.
export function printTx(tx: any, action: "apply" | "undo") {
  const badge = c.badge(action === "apply" ? "Tx Match:" : "UNDO", action === "apply" ? "green" : "yellow");
  const failed = tx.successful === false ? ` ${c.badge("🛑 FAILED", "red")}` : "";
  console.log(`\n${DIVIDER}`);
  console.log(`${badge}  ${c.txHash(shortHex(toHex(tx.hash)))}${failed}`);
  console.log(`  ⛓ fee  ${c.fee(ada(tx.fee))} ADA`);

  // Inputs — with the resolved output when the node includes it.
  console.log(`  📥 inputs ${c.count(`(${tx.inputs.length})`)}:`);
  for (const i of tx.inputs) {
    const ref = c.txHash(`${toHex(i.txHash).slice(0, 12)}…#${i.outputIndex}`);
    if (i.asOutput) {
      console.log(`    ${ref}`);
      console.log(`      ← ${c.address(shortAddr(renderAddr(i.asOutput.address)))}`);
      console.log(`      🪙 ${c.ada(ada(i.asOutput.coin))} ADA`);
    } else {
      console.log(`    ${ref}`);
    }
  }

  // Outputs — address, ADA, and any native assets.
  console.log(`  📤 outputs ${c.count(`(${tx.outputs.length})`)}:`);
  for (const o of tx.outputs) {
    console.log(`    ${c.address(shortAddr(renderAddr(o.address)))}`);
    console.log(`      🪙 ${c.ada(ada(o.coin))} ADA`);
    for (const ma of o.assets) {
      for (const a of ma.assets) {
        console.log(`      🎨 ${c.asset(assetLabel(ma.policyId, a.name))}  ×${c.count(assetQty(a))}`);
      }
    }
  }

  // Mint / burn.
  if (tx.mint.length) {
    console.log(`  ${c.count("✨ mint / 🔥 burn")}:`);
    for (const ma of tx.mint) {
      for (const a of ma.assets) {
        const qty = assetQty(a);
        const isBurn = qty < 0n;
        const verb = isBurn ? "🔥 BURN" : "✨ MINT";
        const abs = isBurn ? -qty : qty;
        console.log(`    ${c.badge(verb, isBurn ? "yellow" : "green")} ${c.asset(assetLabel(ma.policyId, a.name))}  ×${c.count(abs)}`);
      }
    }
  }

  if (tx.certificates.length) {
    console.log(`  📋 certificates ${c.count(`(${tx.certificates.length})`)}`);
  }
}

// ---------------------------------------------------------------------------
// stream consumption
// ---------------------------------------------------------------------------

// Drive a WatchTx stream: invoke `onTx` for every matched apply/undo, and print
// a dim heartbeat for `idle` events (blocks with no match — proof the stream is
// alive between hits; preprod blocks are ~20s apart). Ctrl-C exits cleanly.
// Paces output to one event per second so it stays readable.
export async function runStream(
  stream: AsyncIterable<any>,
  onTx: (tx: any, action: "apply" | "undo") => void,
): Promise<void> {
  process.on("SIGINT", () => {
    console.log("\n👋 stopped");
    process.exit(0);
  });

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  try {
    for await (const evt of stream) {
      if (evt.action === "idle") {
        console.log(c.dim(`💤 idle @ slot ${evt.BlockRef?.slot}`));
      } else {
        // "apply" (new block) or "undo" (rollback)
        onTx(evt.Tx, evt.action);
      }
      await sleep(1000);
    }
  } catch (err) {
    console.error("stream error:", err instanceof Error ? err.message : err);
    process.exit(1);
  }
}
