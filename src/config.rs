use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;

use crate::symbol::{Symbol};

pub type Config = HashMap<String, Strategy>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "strategy")]
pub enum Strategy {
    Shannon(ShannonConfig),
}

#[derive(Debug, Deserialize)]
pub struct ShannonConfig {
    pub symbol: Symbol,
}

pub fn load_config() -> Result<Config> {
    let config = std::fs::read_to_string("config.bot.yaml").unwrap();
    let config: Config = serde_yaml::from_str(&config).unwrap();
    Ok(config)
}