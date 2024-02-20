use std::collections::HashMap;

use anyhow;
use labo::export::chrono::Duration;
use serde::{Deserialize, Deserializer};

use crate::symbol::{Symbol, Currency};

pub type Config = HashMap<String, Strategy>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "strategy")]
pub enum Strategy {
    Shannon(ShannonConfig),
    Abcdf(AbcdfConfig),
}

#[derive(Debug, Deserialize)]
pub struct ShannonConfig {
    pub symbol: Symbol,
    pub virtual_amount: VirtualAmount,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VirtualAmount {
    pub base: f64,
    pub quote: f64,
}

pub fn load_config() -> anyhow::Result<Config> {
    let config = std::fs::read_to_string("config.bot.yaml").unwrap();
    let config: Config = serde_yaml::from_str(&config).unwrap();
    Ok(config)
}

#[derive(Debug, Clone, Copy)]
pub struct Timeframe(pub Duration);

impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // "\d+s" or "\d+m" or "\d+h"
        let s = String::deserialize(deserializer)?;
        let duration = match s.chars().last().unwrap() {
            's' => Duration::seconds(s[0..s.len()-1].parse::<i64>().unwrap()),
            'm' => Duration::minutes(s[0..s.len()-1].parse::<i64>().unwrap()),
            'h' => Duration::hours(s[0..s.len()-1].parse::<i64>().unwrap()),
            _ => panic!("invalid timeframe"),
        };
        Ok(Timeframe(duration))
    }
}

impl From<Timeframe> for Duration {
    fn from(val: Timeframe) -> Self {
        val.0
    }
}

#[derive(Debug, Deserialize)]
pub struct AbcdfConfig {
    pub symbol: Symbol,
    pub ref_symbols: Vec<Currency>,
    pub model_path: String,
}