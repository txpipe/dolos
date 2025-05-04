use pallas::ledger::traverse::MultiEraOutput;
use tx3_lang::ir::Expression;

use crate::ledger::TxoRef;

pub fn into_tx3_utxo(
    txoref: &TxoRef,
    parsed: &MultiEraOutput<'_>,
) -> Result<tx3_lang::Utxo, tx3_cardano::Error> {
    let coin = parsed.value().coin() as i128;
    let address = parsed
        .address()
        .map_err(tx3_cardano::Error::InvalidAddress)?
        .to_vec();

    let mut assets = vec![tx3_lang::ir::AssetExpr {
        policy: Expression::None,
        asset_name: Expression::None,
        amount: Expression::Number(coin),
    }];

    assets.extend(parsed.value().assets().into_iter().flat_map(|x| {
        let policy = x.policy().to_vec();
        x.assets()
            .into_iter()
            .map(|y| {
                let asset_name = y.name().to_vec();
                let amount = y.any_coin();
                tx3_lang::ir::AssetExpr {
                    policy: Expression::Bytes(policy.clone()),
                    asset_name: Expression::Bytes(asset_name),
                    amount: Expression::Number(amount),
                }
            })
            .collect::<Vec<_>>()
    }));

    Ok(tx3_lang::Utxo {
        r#ref: tx3_lang::UtxoRef {
            txid: txoref.0.to_vec(),
            index: txoref.1,
        },
        address,
        datum: match parsed.datum() {
            Some(pallas::ledger::primitives::conway::DatumOption::Data(x)) => {
                Some(Expression::Bytes(x.raw_cbor().to_vec()))
            }
            _ => None,
        },
        assets,
        script: None,
    })
}
