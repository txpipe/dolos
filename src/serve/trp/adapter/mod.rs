use dolos_core::Domain;

use super::Context;

mod inputs;
mod pparams;
mod utxos;

impl<D: Domain> tx3_cardano::Ledger for Context<D> {
    async fn get_pparams(&self) -> Result<tx3_cardano::PParams, tx3_cardano::Error> {
        pparams::resolve::<D>(self.domain.genesis(), self.domain.state())
    }

    async fn resolve_input(
        &self,
        query: &tx3_lang::ir::InputQuery,
    ) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
        let network = pparams::network_id_from_genesis(self.domain.genesis()).unwrap();
        inputs::resolve::<D>(self.domain.state(), network, query)
    }
}
