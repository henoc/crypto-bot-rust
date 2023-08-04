use once_cell::sync::OnceCell;
use serde::Deserialize;
use strum::EnumString;

pub static DEBUG: OnceCell<DebugFlag> = OnceCell::new();

#[derive(Debug, Clone, Copy, EnumString, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum DebugFlag {
    None,
    Kline,
    Orderbook,
}

pub fn get_debug() -> DebugFlag {
    *DEBUG.get().unwrap()
}