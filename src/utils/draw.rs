use std::io::stdout;

use crossterm::{ExecutableCommand, terminal::{Clear, ClearType}};

pub fn init_terminal() -> anyhow::Result<()> {
    stdout().execute(Clear(ClearType::All))?;
    Ok(())
}