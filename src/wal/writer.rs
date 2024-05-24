use super::reader::ReadUtils;
use super::*;

pub trait WalWriter: WalReader {
    fn append_entries(&mut self, logs: impl Iterator<Item = LogValue>) -> Result<(), WalError>;

    fn roll_forward(&mut self, blocks: impl Iterator<Item = RawBlock>) -> Result<(), WalError> {
        self.append_entries(blocks.map(LogValue::Apply))
    }

    fn roll_back(&mut self, until: &ChainPoint) -> Result<(), WalError> {
        let seq = self.assert_point(until)?;

        // find all of the "apply" event in the wall and gather the contained block
        // data.
        let applies: Vec<_> = self
            .crawl_from(Some(seq))?
            .rev()
            .filter_apply()
            .as_blocks()
            .flatten()
            .collect();

        // take all of the applies, except the last one, and turn them into undo.
        let undos: Vec<_> = applies
            .into_iter()
            .filter(|x| ChainPoint::from(x).eq(until))
            .map(LogValue::Undo)
            .collect();

        // the last one (which is the point the chain is at) is turned into a mark.
        let mark = std::iter::once(LogValue::Mark(until.clone()));

        self.append_entries(undos.into_iter().chain(mark))?;

        Ok(())
    }
}
