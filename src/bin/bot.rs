use std::{collections::HashMap, env};

use clap::Parser;
use bot::{config::{Strategy, self}, logger, global_vars::{DEBUG, debug_is_none}};
use labo::export::anyhow::{Context, self};
use log::LevelFilter;
use once_cell::sync::Lazy;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    name: String,
    #[clap(short, long)]
    debug: Option<String>,
    #[clap(short, long)]
    quiet: bool,
}

static LOGGER: logger::BotLogger = logger::BotLogger;
static CONFIG: Lazy<HashMap<String,Strategy>> = Lazy::new(|| {
    config::load_config().context("failed to load config").unwrap()
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    env::set_var("NAME", &args.name);
    DEBUG.set(args.debug).unwrap();

    if !args.quiet {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Info))?;
    }

    let strategy = CONFIG.get(&args.name).with_context(|| format!("{} is not found in config", args.name))?;
    match strategy {
        Strategy::Shannon(strategy_config) => {
            bot::strategy::shannon_gmo::start_shannon_gmo(strategy_config).await;
        },
        Strategy::Abcdf(strategy_config) => {
            bot::strategy::abcdf_tachibana::start_abcdf(strategy_config).await;
        },
    }
    Ok(())
}
