use std::collections::HashMap;

use anyhow::Context;
use chrono::Duration;
use futures::StreamExt;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tap::Pipe;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{utils::{kline_mmap::KLineMMap, strategy_utils::{show_kline_mmap, start_flush_kline_mmap, capture_result}}, config::{CrawlerConfig, KLineBuilderConfig}, symbol::{Symbol, SymbolType}, client::binance::WsAggTrade};



static KLINE_MMAP: OnceCell<RwLock<HashMap<Duration, KLineMMap>>> = OnceCell::new();

pub async fn start_crawler_binance(config: &CrawlerConfig, check: bool) {
    let symbol = config.symbol;
    let kline_config = config.kline_builder.clone();

    KLINE_MMAP.set(RwLock::new(
        kline_config.iter().map(|c| (Duration::seconds(c.timeframe_sec), KLineMMap::new(symbol, Duration::seconds(c.timeframe_sec), c.len).unwrap())).collect()
    )).unwrap();

    if check {
        show_kline_mmap(&KLINE_MMAP, &kline_config).unwrap();
        return;
    }

    start_flush_kline_mmap(&KLINE_MMAP, symbol, &kline_config);

    select! {
        _ = spawn(async move {
            subscribe_trades(symbol, &kline_config).await.pipe(capture_result(&symbol));
        }) => {}
    }
}

async fn subscribe_trades(symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let stream_name = match symbol.r#type {
        SymbolType::Spot => "stream".to_owned(),
        SymbolType::Perp => "fstream".to_owned(),
    };
    let (socket, _) = connect_async(
        Url::parse(&format!("wss://{}.binance.com/ws/{}@aggTrade", stream_name, symbol.to_native().to_lowercase())).unwrap()).await?;
    info!("Connected to websocket");

    let (mut write, mut read) = socket.split();

    while let Some(msg) = read.next().await {
        match handle_trades_msg(msg?, &symbol, kline_config) {
            Ok(_) => (),
            _ => continue,
        }
    }
    Ok(())
}

fn handle_trades_msg(msg: Message, symbol: &Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let ws_agg_trade: WsAggTrade = serde_json::from_str(msg)?;
    for conf in kline_config {
        KLINE_MMAP.get().context("KLINE_MMAP is not initialized")?.write()
            .get_mut(&Duration::seconds(conf.timeframe_sec)).unwrap().update_ohlcv(&ws_agg_trade.to_trade_record(*symbol)?)?;
    }
    Ok(())
}