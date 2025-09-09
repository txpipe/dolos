use dolos_core::{BlockSlot, ChainError, Domain};

mod pots;
mod rewards;
mod stake;

pub fn sweep<D: Domain>(domain: &D, _: BlockSlot) -> Result<(), ChainError> {
    // order matters
    stake::sweep(domain)?;
    pots::sweep(domain)?;
    rewards::sweep(domain)?;

    Ok(())
}
