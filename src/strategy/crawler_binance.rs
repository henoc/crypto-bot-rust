use std::collections::HashMap;

use anyhow::Context;
use chrono::Duration;
use futures::StreamExt;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{utils::{kline_mmap::KLineMMap, strategy_utils::{show_kline_mmap, start_flush_kline_mmap, CaptureResult}}, config::{CrawlerConfig, KLineBuilderConfig}, symbol::{Symbol, SymbolType}, client::binance::WsAggTrade, global_vars::{get_debug, DebugFlag}};



static KLINE_MMAP: OnceCell<RwLock<HashMap<Duration, KLineMMap>>> = OnceCell::new();

pub async fn start_crawler_binance(config: &CrawlerConfig) {
    if config.symbols.len() != 1 {
        panic!("Only one symbol is supported");
    }
    let symbol = config.symbols[0];
    let kline_config = config.kline_builder.clone();

    KLINE_MMAP.set(RwLock::new(
        kline_config.iter().map(|c| (c.timeframe.0, KLineMMap::new(symbol, c.timeframe.0, c.len).unwrap())).collect()
    )).unwrap();

    if get_debug()==DebugFlag::Kline {
        show_kline_mmap(&KLINE_MMAP, &kline_config).unwrap();
        return;
    }

    start_flush_kline_mmap(&KLINE_MMAP, symbol, &kline_config);

    select! {
        _ = spawn(async move {
            subscribe_trades(symbol, &kline_config).await.capture_result(symbol).await.unwrap();
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

    let (mut _write, mut read) = socket.split();

    while let Some(msg) = read.next().await {
        match handle_trades_msg(msg?, &symbol, kline_config) {
            Ok(_) => (),
            _ => continue,
        }
    }
    anyhow::bail!("Websocket disconnected");
}

fn handle_trades_msg(msg: Message, symbol: &Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let ws_agg_trade: WsAggTrade = serde_json::from_str(msg)?;
    for conf in kline_config {
        KLINE_MMAP.get().context("KLINE_MMAP is not initialized")?.write()
            .get_mut(&conf.timeframe.0).unwrap().update_ohlcv(&ws_agg_trade.to_trade_record(*symbol)?)?;
    }
    Ok(())
}