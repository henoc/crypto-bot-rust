use std::collections::HashMap;

use anyhow::Context;
use chrono::Duration;
use futures::{StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tap::Pipe;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{config::{KLineBuilderConfig, CrawlerConfig}, utils::{strategy_utils::{capture_result, start_send_ping}, kline_mmap::KLineMMap, time::{sleep_until_next, ScheduleExpr, KLinesTimeUnit}}, symbol::Symbol, client::{types::{MpackTradeRecord, trades_time_fn, TradeRecord, KLines}, bitflyer::{WsResponse, ExecutionItem}}};

static KLINE_MMAP: OnceCell<RwLock<HashMap<Duration, KLineMMap>>> = OnceCell::new();

pub async fn start_crawler_bitflyer(config: &CrawlerConfig, check: bool) {
    let symbol = config.symbol;
    let kline_config = config.kline_builder.clone();

    KLINE_MMAP.set(RwLock::new(
        kline_config.iter().map(|c| (Duration::seconds(c.timeframe_sec), KLineMMap::new(symbol, Duration::seconds(c.timeframe_sec), c.len).unwrap())).collect()
    )).unwrap();

    // ファイルの中身を表示する
    if check {
        show_kline_mmap(&kline_config).unwrap();
        return;
    }

    for config in kline_config.clone() {
        let timeframe = Duration::seconds(config.timeframe_sec);
        spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(timeframe, Duration::seconds(0))).await;
                flush_kline_mmap(Duration::seconds(config.timeframe_sec)).pipe(capture_result(&symbol));
            }
        });
    }

    select! {
        _ = spawn(async move {
            subscribe_trades(symbol, &kline_config).await.pipe(capture_result(&symbol));
        }) => {}
    }
}

fn show_kline_mmap(kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    for config in kline_config {
        let timeframe = Duration::seconds(config.timeframe_sec);
        let hmap = KLINE_MMAP.get().context("KLINE_MMAP is not initialized")?.read();
        let mmap = hmap.get(&timeframe).unwrap();
        let klines = KLines::new_options(&mmap.mmap_read_all()?, KLinesTimeUnit::MilliSecond)?;
        info!("{}:", mmap.get_mmap_path());
        info!("{:?}", klines.df);
    }
    Ok(())
}

fn flush_kline_mmap(timeframe: Duration) -> anyhow::Result<()> {
    KLINE_MMAP.get().context("KLINE_MMAP is not initialized")?.write().get_mut(&timeframe).unwrap().update_mmap()?;
    info!("Flushed kline mmap, timeframe: {:?}", timeframe);
    Ok(())
}

async fn subscribe_trades(symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let (socket, _) = connect_async(
    Url::parse("wss://ws.lightstream.bitflyer.com/json-rpc").unwrap()).await?;
    info!("Connected to websocket");
    
    let (mut write, mut read) = socket.split();
    
    let op = serde_json::json!({
        "method": "subscribe",
        "params": {"channel": format!("lightning_executions_{}", symbol.to_native())}
    });

    write.send(Message::Text(op.to_string())).await?;

    start_send_ping(symbol, write).await;

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
    let parsed: WsResponse = serde_json::from_str(msg)?;
    if &parsed.method != "channelMessage" {
        return anyhow::bail!("Not channelMessage");
    }
    if &parsed.params.channel != &format!("lightning_executions_{}", symbol.to_native()) {
        return anyhow::bail!("Not channel for lightning_executions_{}", symbol.to_native());
    }
    let trades = serde_json::from_value::<Vec<ExecutionItem>>(parsed.params.message)?;
    let trades = trades.into_iter().map(|t| TradeRecord::new(
        *symbol,
        t.exec_date.0.timestamp_millis(),
        t.price,
        t.size,
        t.side,
    )).collect();
    for conf in kline_config {
        KLINE_MMAP.get().context("KLINE_MMAP is not initialized")?.write()
            .get_mut(&Duration::seconds(conf.timeframe_sec)).unwrap().update_ohlcvs(&trades)?;
    }
    Ok(())
}