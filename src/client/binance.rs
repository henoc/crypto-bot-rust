use anyhow::Context;
use anyhow;
use serde::Deserialize;

use crate::{symbol::Symbol, order_types::Side};

use super::types::TradeRecord;


#[derive(Deserialize, Debug, Clone)]
pub struct WsAggTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "l")]
    pub last_trade_id: i64,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
    #[serde(rename = "M")]
    pub is_best_match: bool,
}

impl WsAggTrade {
    pub fn to_trade_record(&self, symbol: Symbol) -> anyhow::Result<TradeRecord> {
        Ok(TradeRecord {
            symbol,
            timestamp: self.trade_time,
            price: self.price.parse().context("price parse error")?,
            amount: self.quantity.parse().context("amount parse error")?,
            side: if self.is_buyer_maker { Side::Sell} else { Side::Buy },
        })
    }
}