use super::Context;

mod inputs;
mod pparams;
mod utxos;

impl tx3_cardano::Ledger for Context {
    async fn get_pparams(&self) -> Result<tx3_cardano::PParams, tx3_cardano::Error> {
        pparams::resolve(&self.genesis, &self.ledger)
    }

    async fn resolve_input(
        &self,
        query: &tx3_lang::ir::InputQuery,
    ) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
        let network = pparams::network_id_from_genesis(&self.genesis).unwrap();
        inputs::resolve(&self.ledger, network, query)
    }
}
