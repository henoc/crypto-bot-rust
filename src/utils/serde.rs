use serde::{Deserializer, Deserialize};
use std::str::FromStr;

pub fn deserialize_f64_from_str<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let f = f64::from_str(&s).map_err(serde::de::Error::custom)?;
        Ok(f)
    }