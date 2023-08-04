use anyhow::{Context};
use chrono::{Duration, DateTime, Utc, format};
use std::{time::Duration as StdDuration, collections::HashMap};
use futures::{StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde_json::{Value, json};
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{symbol::{Symbol, SymbolType, Exchange, Currency}, utils::{time::{sleep_until_next, ScheduleExpr, parse_format_time_utc, now_floor_time}, status_repository::StatusRepository, record_writer::{SerialRecordWriter}, strategy_utils::{start_send_ping, CaptureResult, start_flush_kline_mmap, show_kline_mmap}, useful_traits::{StaticVarExt, StaticVarVecExt}, orderbook_repository::{OrderbookRepository, OrderbookBest, orderbook_best_time_fn, apply_diff_once}, draw_orderbook::OrderbookDrawer, draw::init_terminal, kline_mmap::KLineMMap}, client::{coincheck::{CoincheckClient, KLineRequest, KLineResponse, WsResponse, OrderbookRequest}, types::{MpackTradeRecord, trades_time_fn}}, data_structure::time_queue::TimeQueue, order_types::Side, global_vars::{DEBUG, get_debug, DebugFlag}, config::{CrawlerConfig, KLineBuilderConfig}};

static STATUS: OnceCell<RwLock<StatusRepository>> = OnceCell::new();
static KLINE_MMAP: OnceCell<RwLock<HashMap<Duration, KLineMMap>>> = OnceCell::new();
static TRADE_RECORD: OnceCell<RwLock<Vec<MpackTradeRecord>>> = OnceCell::new();
static ORDERBOOK: OnceCell<RwLock<OrderbookRepository>> = OnceCell::new();
static ORDERBOOK_BEST: OnceCell<RwLock<Vec<OrderbookBest>>> = OnceCell::new();
static ORDERBOOK_DIFF: OnceCell<RwLock<[TimeQueue<(f64, f64)>; 2]>> = OnceCell::new();

// for debug
static ORDERBOOK_DRAWER: OnceCell<RwLock<OrderbookDrawer>> = OnceCell::new();

const ORDERBOOK_DIFF_DURATION: StdDuration = StdDuration::from_secs(5);

pub async fn start_crawler_coincheck(config: &CrawlerConfig) {
    if config.symbols.len() != 1 {
        panic!("Only one symbol is supported");
    }
    let symbol = config.symbols[0];
    let kline_config = config.kline_builder.clone();

    KLINE_MMAP.set(RwLock::new(
        kline_config.iter().map(|c| (c.timeframe.0, KLineMMap::new(symbol, c.timeframe.0, c.len).unwrap())).collect()
    )).unwrap();

    STATUS.set(RwLock::new(StatusRepository::new_init("crawler", &symbol, None).unwrap())).unwrap();
    
    TRADE_RECORD.set(RwLock::new(Vec::new())).unwrap();
    ORDERBOOK.set(RwLock::new(OrderbookRepository::new(Duration::seconds(1)))).unwrap();
    ORDERBOOK_BEST.set(RwLock::new(Vec::new())).unwrap();
    ORDERBOOK_DIFF.set(RwLock::new([TimeQueue::new(ORDERBOOK_DIFF_DURATION), TimeQueue::new(ORDERBOOK_DIFF_DURATION)])).unwrap();

    if get_debug()==DebugFlag::Orderbook {
        ORDERBOOK_DRAWER.set(RwLock::new(OrderbookDrawer::new(0, 0, vec![symbol]))).unwrap();
        init_terminal().unwrap();
    }
    if get_debug()==DebugFlag::Kline {
        show_kline_mmap(&KLINE_MMAP, &kline_config).unwrap();
        return;
    }

    start_flush_kline_mmap(&KLINE_MMAP, symbol, &kline_config);

    select! {
        // 1min klineの保存
        _ = spawn(async move {
            let client = CoincheckClient::new(None);
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::hours(1), Duration::minutes(0))).await;
                fetch_kline(symbol, &client).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        // wsのsubscribe
        _ = spawn(async move {
            subscribe_ws(symbol, &kline_config).await.capture_result(symbol).await.unwrap();
        }) => {}
        _ = spawn(async move {
            let client = CoincheckClient::new(None);
            replace_orderbook_state(&client).await.capture_result(symbol).await.unwrap();
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::minutes(1), Duration::minutes(0))).await;
                replace_orderbook_state(&client).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        // trades,orderbookのファイル出力
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::seconds(5), Duration::seconds(0))).await;
                flush_trade_records(symbol).capture_result(symbol).await.unwrap();
                flush_orderbook_best(symbol).capture_result(symbol).await.unwrap();
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
    let result: KLineResponse = client.get_public(KLineRequest {
        symbol: symbol.clone(),
        timeframe: timeframe.clone(),
        limit,
    }).await?;
    let mut klines = result.to_klines(now_floor_time(timeframe, 0), timeframe)?;

    // last_timeの読み込み
    let obj = STATUS.get().context("STATUS is not initialized")?.read().get(&symbol).clone();
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

/// orderbookのsnapshotを取得して直近の差分をすべて適用する
async fn replace_orderbook_state(client: &CoincheckClient) -> anyhow::Result<()> {
    let res = client.get_public(OrderbookRequest {}).await?;
    let mut snapshot = vec![];
    for &side in &[Side::Buy, Side::Sell] {
        snapshot.push(apply_diff_once(
            res.by_side(side).iter().map(|item| (item.price.into(), item.size.into())).collect(),
            ORDERBOOK_DIFF.read()[side as usize].get_data_iter().map(|&(price, size)| (price.into(), size.into())),
        ))
    }
    ORDERBOOK.write().replace_state(snapshot);
    Ok(())
}

async fn subscribe_ws(symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let (socket, _) =
        connect_async(Url::parse("wss://ws-api.coincheck.com/").unwrap()).await?;
    info!("Connected to websocket");

    let (mut write, mut read) = socket.split();

    let channels = vec![
        format!("{}-trades", symbol.to_native()),
        format!("{}-orderbook", symbol.to_native()),
    ];
    
    for channel in channels {
        let op = serde_json::json!({
            "type": "subscribe",
            "channel": channel,
        });
        write.send(Message::Text(op.to_string())).await?;
    }

    start_send_ping(symbol, write).await;

    while let Some(msg) = read.next().await {
        match handle_ws_msg(msg?, symbol, kline_config) {
            Ok(_) => (),
            Err(_) => continue,
        }
    }
    anyhow::bail!("WebSocket disconnected");
}

fn handle_ws_msg(msg: Message, symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed = serde_json::from_str::<WsResponse>(msg)?;
    match parsed {
        WsResponse::Trade(trade) => {
            let trades = trade.to_trade_records()?;
            for conf in kline_config {
                KLINE_MMAP.write()
                .get_mut(&conf.timeframe.0).unwrap().update_ohlcvs(&trades)?;
            }
            TRADE_RECORD.write().extend(trades.iter().cloned().map(|x| x.mpack()));
        },
        WsResponse::Orderbook(res) => {
            let mut orderbook = ORDERBOOK.write();
            let mut orderbook_diff = ORDERBOOK_DIFF.write();
            if let Some(best) = orderbook.snapshot_on_update(res.last_update_at) {
                ORDERBOOK_BEST.write().push(best);
            }
            for &side in &[Side::Buy, Side::Sell] {
                for item in res.by_side(side) {
                    if item.size == 0. {
                        orderbook.remove(side, item.price);
                    } else {
                        orderbook.insert(side, item.price, item.size);
                    }
                }
                orderbook_diff[side as usize].extend(res.by_side(side).iter().map(|item| (item.price, item.size)));
                orderbook_diff[side as usize].retain();
            }

            // orderbookの描画
            if get_debug()==DebugFlag::Orderbook {
                ORDERBOOK_DRAWER.write().print_orderbook(orderbook.get_best(), symbol)?;
            }
        }
    }
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

fn flush_orderbook_best(symbol: Symbol) -> anyhow::Result<()> {
    SerialRecordWriter::<OrderbookBest>::new(
        "orderbook",
        &symbol,
        "msgpack",
        Box::new(orderbook_best_time_fn)
    ).write_msgpack(&ORDERBOOK_BEST.drain())?;
    Ok(())
}

#[test]
fn test_timefn() {
    use crate::utils::time::format_time_utc;
    let value = json!({
        "close": 4406641.5, "high": 4406641.5, "low": 4402941.0, "open": 4402941.0, "opentime": "2023-07-01T11:44:00+00:00", "volume": 0.0
    });
    let dt = kline_time_fn(&value).unwrap();
    assert_eq!(format_time_utc(dt), "2023-07-01T11:44:00+00:00");
}