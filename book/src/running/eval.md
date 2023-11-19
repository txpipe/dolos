# `eval` command

The `eval` is an utility for evaluating transactions. It takes tx data from an external source and uses the current ledger state to evaluate phase-1 validation rules.

## Usage

To execute the evaluation, run the following command from your terminal:

```bash
dolos eval --file <FILE> --era <ERA> --magic <MAGIC> --slot <SLOT>
```

The args should be interpreted as:

- `--file <FILE>`: the path to the file containing the tx data as hex-encoded cbor.
- `--era <ERA>`: the id of the era that should be used to interpret the transaction data.
- `--magic <MAGIC>`: the protocol magic of the network.
- `--slot <SLOT>`: the slot that should be used for retrieving protocol parameters.