use std::io::stdout;

use crossterm::{ExecutableCommand, terminal::{Clear, ClearType}};
use labo::export::anyhow;

pub fn init_terminal() -> anyhow::Result<()> {
    stdout().execute(Clear(ClearType::All))?;
    Ok(())
}