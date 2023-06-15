use clokwerk::{AsyncScheduler, TimeUnits};
use maplit::hashmap;
use tokio::{select, spawn};

use crate::{symbol::{Symbol, SymbolType, Exchange}, utils::time::{sleep_until_next, ScheduleExpr}, client::{coincheck::{CoincheckClient, KLineRequest}, types::{KLines}}};

pub async fn start_crawler_coincheck() {

    let client = CoincheckClient::new();

    select! {
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::EveryMinute {q: 30, r: 0, second: 0}).await;
                fetch_kline(&client).await;
            }
        }) => {}
    }
}

async fn fetch_kline(client: &CoincheckClient) -> anyhow::Result<()> {
    let symbol = Symbol::new("BTC", "JPY", SymbolType::Spot, Exchange::Coincheck);
    let timeframe_min = 1;
    let limit = 300;
    let result: Vec<Vec<Option<f64>>> = client.get("/api/charts/candle_rates", KLineRequest {
        symbol: symbol.clone(),
        timeframe_sec: timeframe_min * 60,
        limit,
    }).await?;
    Ok(())
}