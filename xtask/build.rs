use vergen_gitcl::{Emitter, GitclBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let git = GitclBuilder::default().sha(false).dirty(true).build()?;
    Emitter::default().add_instructions(&git)?.emit()?;
    Ok(())
}
