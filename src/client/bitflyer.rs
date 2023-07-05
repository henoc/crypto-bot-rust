use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::order_types::Side;

#[derive(Deserialize, Debug, Clone)]
pub struct WsResponse {
    pub jsonrpc: String,
    pub method: String,
    pub params: WsResponseParams,
}

#[derive(Deserialize, Debug, Clone)]
pub struct WsResponseParams {
    pub channel: String,
    /// channelが lightning_executionsのときはVec<ExecutionItem>
    pub message: Value,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExecutionItem {
    pub id: i64,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub exec_date: ExecDate,
    pub buy_child_order_acceptance_id: String,
    pub sell_child_order_acceptance_id: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecDate(pub DateTime<Utc>);

impl<'de> Deserialize<'de> for ExecDate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let datetime = DateTime::parse_from_rfc3339(&s)
            .map_err(serde::de::Error::custom)?
            .with_timezone(&Utc);
        Ok(ExecDate(datetime))
    }
}
