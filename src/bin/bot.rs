use std::{collections::HashMap, env};

use clap::Parser;
use bot::{client::mail::send_mail, config::{self, Strategy}, global_vars::{debug_is_none, DEBUG}, logger};
use anyhow::{Context, self};
use log::{info, LevelFilter};
use once_cell::sync::Lazy;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    name: String,
    #[clap(short, long)]
    command: Option<String>,
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
    match run_strategy(strategy, args.command).await {
        Ok(_) => Ok(()),
        Err(e) if debug_is_none() => {
            info!("send error message mail");
            send_mail(format!("{} - {}", e, env::var("NAME")?), format!("{:?}", e))?;
            Err(e)
        }
        Err(e) => Err(e),
    }?;
    Ok(())
}

async fn run_strategy(strategy: &'static Strategy, command: Option<String>) -> anyhow::Result<()> {
    match strategy {
        Strategy::Shannon(strategy_config) => {
            bot::strategy::shannon_gmo::start_shannon_gmo(strategy_config).await;
        },
        Strategy::Abcdf(strategy_config) => {
            bot::strategy::abcdf_tachibana::action_abcdf(strategy_config, command.context("command arg is none")?.as_str()).await?;
        },
    }
    Ok(())
}
