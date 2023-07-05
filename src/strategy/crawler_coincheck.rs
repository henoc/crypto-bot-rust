use std::thread::sleep;

use anyhow::{Context, anyhow};
use chrono::{Duration, NaiveDateTime, DateTime, Utc};
use futures::{StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde_json::{Value, json};
use tap::Pipe;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{symbol::{Symbol, SymbolType, Exchange, Currency}, utils::{time::{sleep_until_next, ScheduleExpr, datetime_utc_from_timestamp, KLinesTimeUnit, parse_format_time_utc, format_time_utc, self, now_floor_time}, status_repository::StatusRepository, record_writer::{SerialRecordWriter, SerializerType}, strategy_utils::{capture_result, start_send_ping}}, client::{coincheck::{CoincheckClient, KLineRequest, KLineResponse, WsTradesResponse}, types::{KLines, MpackTradeRecord, trades_time_fn}}};

static STATUS: OnceCell<RwLock<StatusRepository>> = OnceCell::new();
static TRADE_RECORD: OnceCell<RwLock<Vec<MpackTradeRecord>>> = OnceCell::new();

pub async fn start_crawler_coincheck() {

    STATUS.set(RwLock::new(StatusRepository::new("crawler"))).unwrap();
    let client = CoincheckClient::new();
    let symbol = Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Spot, Exchange::Coincheck);

    TRADE_RECORD.set(RwLock::new(Vec::new())).unwrap();

    select! {
        // 1min klineの保存
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::hours(1), Duration::minutes(0))).await;
                fetch_kline(symbol, &client).await.pipe(capture_result(&symbol));
            }
        }) => {}
        // tradesのsubscribe
        _ = spawn(async move {
            subscribe_trades(symbol).await.pipe(capture_result(&symbol));
        }) => {}
        // tradesのファイル出力
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::seconds(5), Duration::seconds(0))).await;
                flush_trade_records(symbol).pipe(capture_result(&symbol));
            }
        }) => {}
    }
}

fn kline_time_fn(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(s) = value["opentime"].as_str() {
        let dt = parse_format_time_utc(s).ok()?;
        Some(dt)
    } else {
        None
    }
}

async fn fetch_kline(symbol: Symbol, client: &CoincheckClient) -> anyhow::Result<()> {
    let timeframe = Duration::minutes(1);
    let limit = 300;
    let result: KLineResponse = client.get("/api/charts/candle_rates", KLineRequest {
        symbol: symbol.clone(),
        timeframe: timeframe.clone(),
        limit,
    }).await?;
    let mut klines = result.to_klines(now_floor_time(timeframe, 0), timeframe)?;

    // last_timeの読み込み
    let obj = STATUS.get().context("STATUS is not initialized")?.write().get(&symbol, None)?;
    let last_time = obj["last_time"].as_str();
    if let Some(last_time) = last_time {
        klines = klines.filter(Some(parse_format_time_utc(last_time)? + timeframe), None)?;
    }

    // klinesの書き込み
    let json_klines = klines.to_json()?;
    SerialRecordWriter::new(
        "klines",
        &symbol,
        "log",
        Box::new(kline_time_fn)
    ).write_json(&json_klines)?;

    // last_timeを更新
    STATUS.get().context("STATUS is not initialized")?.write().update(symbol, json!({
        "last_time": json_klines.as_array().unwrap().last().context("json_klines is empty")?["opentime"].as_str()
            .context("opentime is not string")?
    }))?;
    Ok(())
}

async fn subscribe_trades(symbol: Symbol) -> anyhow::Result<()> {
    let (socket, _) =
        connect_async(Url::parse("wss://ws-api.coincheck.com/").unwrap()).await?;
    info!("Connected to websocket");

    let (mut write, mut read) = socket.split();
    
    let op = serde_json::json!({
        "type": "subscribe",
        "channel": format!("{}-trades", symbol.to_native()),
    });

    write.send(Message::Text(op.to_string())).await?;

    start_send_ping(symbol, write).await;

    while let Some(msg) = read.next().await {
        match handle_trade_msg(msg?) {
            Ok(_) => (),
            Err(_) => continue,
        }
    }
    Ok(())
}

fn handle_trade_msg(msg: Message) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed = serde_json::from_str::<WsTradesResponse>(msg)?;
    let trades = parsed.to_trade_records()?;

    // msgpackで出力する用
    TRADE_RECORD.get().context("TRADE_RECORD is not initialized")?
        .write().extend(trades.iter().cloned().map(|x| x.mpack()));
    Ok(())
}

/// msgpackで出力
fn flush_trade_records(symbol: Symbol) -> anyhow::Result<()> {
    let records = TRADE_RECORD.get().context("TRADE_RECORD is not initialized")?
        .write().drain(..).collect::<Vec<_>>();
    // info!("flush_trade_records: {}", records.len());
    SerialRecordWriter::<MpackTradeRecord>::new(
        "marketTrades",
        &symbol,
        "msgpack",
        Box::new(trades_time_fn)
    ).write_msgpack(&records)
}

#[test]
fn test_timefn() {
    let value = json!({
        "close": 4406641.5, "high": 4406641.5, "low": 4402941.0, "open": 4402941.0, "opentime": "2023-07-01T11:44:00+00:00", "volume": 0.0
    });
    let dt = kline_time_fn(&value).unwrap();
    assert_eq!(format_time_utc(dt), "2023-07-01T11:44:00+00:00");
}