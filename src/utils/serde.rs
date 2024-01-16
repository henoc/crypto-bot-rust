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

pub fn serialize_u32_to_str<S>(x: &u32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&x.to_string())
    }

pub fn serialize_f64_to_str<S>(x: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&x.to_string())
    }
