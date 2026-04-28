use vergen_gitcl::{Emitter, GitclBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gitcl = GitclBuilder::default().sha(true).build()?;

    Emitter::default().add_instructions(&gitcl)?.emit()?;

    Ok(())
}
