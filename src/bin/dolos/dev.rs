use pallas::ledger::traverse::MultiEraTx;

pub fn main() -> miette::Result<()> {
    let cbor = hex::decode("84a400d90102818258209b371440636a6ee266c48ef2d56fb14e872391b001827c98c772a62850714e2b000182a200581d60464d4cb029fc90cf720600b2f271a2d433517ad32a67eac5eb9bba5c011a000493e0a200581d60464d4cb029fc90cf720600b2f271a2d433517ad32a67eac5eb9bba5c011a3b90b1af021a000584710f00a200d9010281825820270289d964e91013d551bc58621c962f0b6214ee13ddbd3b794040b846e238765840db924cb0962e0021ffd9015ca69effe631a25e5f6181c74b8482e0e19af5ea7689bcf04544eb446ab67fcb7ce1ca70fdfa5b97fb5d530ff0ffc96c3fff6b7b040581840000d87980820000f5f6").unwrap();
    let tx = MultiEraTx::decode(&cbor).unwrap();

    for output in tx.outputs() {
        println!("output: {}", output.era());
        println!("conway?: {:?}", output.as_conway());
    }

    Ok(())
}
