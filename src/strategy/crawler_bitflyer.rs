use std::collections::HashMap;

use chrono::{Duration, DateTime, Utc};
use futures::{StreamExt, SinkExt, channel::mpsc::{unbounded, UnboundedSender}};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde_json::json;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{config::{KLineBuilderConfig, CrawlerConfig}, utils::{strategy_utils::{start_send_ping, show_kline_mmap, start_flush_kline_mmap, CaptureResult, connect_into_sink}, kline_mmap::KLineMMap, time::{sleep_until_next, ScheduleExpr, UnixTimeUnit, datetime_utc_from_timestamp}, useful_traits::{StaticVarExt, StaticVarVecExt}, orderbook_repository::{OrderbookRepository, OrderbookBest, orderbook_best_time_fn}, record_writer::SerialRecordWriter, status_repository::StatusRepository}, symbol::Symbol, client::{types::{MpackTradeRecord, trades_time_fn}, bitflyer::{WsResponse, ExecutionItem, BoardResult}}, data_structure::float_exp::FloatExp, order_types::Side};

static KLINE_MMAP: OnceCell<RwLock<HashMap<Duration, KLineMMap>>> = OnceCell::new();
static ORDERBOOK: OnceCell<RwLock<OrderbookRepository>> = OnceCell::new();
static ORDERBOOK_BEST: OnceCell<RwLock<Vec<OrderbookBest>>> = OnceCell::new();
static SERVER_TIME: OnceCell<RwLock<ServerTimeState>> = OnceCell::new();
static STATUS: OnceCell<RwLock<StatusRepository>> = OnceCell::new();
static TRADE_RECORD: OnceCell<RwLock<Vec<MpackTradeRecord>>> = OnceCell::new();

pub async fn start_crawler_bitflyer(config: &CrawlerConfig, check: bool) {
    if config.symbols.len() != 1 {
        panic!("Only one symbol is supported");
    }
    let symbol = config.symbols[0];
    let kline_config = config.kline_builder.clone();

    KLINE_MMAP.set(RwLock::new(
        kline_config.iter().map(|c| (c.timeframe.0, KLineMMap::new(symbol, c.timeframe.0, c.len).unwrap())).collect()
    )).unwrap();
    ORDERBOOK.set(RwLock::new(OrderbookRepository::new(Duration::seconds(1)))).unwrap();
    ORDERBOOK_BEST.set(RwLock::new(Vec::new())).unwrap();
    STATUS.set(RwLock::new(StatusRepository::new_init("crawler", &symbol, None).unwrap())).unwrap();
    SERVER_TIME.set(RwLock::new(ServerTimeState {
        server_time: STATUS.read().get(&symbol)["server_time"].as_i64().map(|t| datetime_utc_from_timestamp(t, UnixTimeUnit::MilliSecond)),
        client_time: STATUS.read().get(&symbol)["client_time"].as_i64().map(|t| datetime_utc_from_timestamp(t, UnixTimeUnit::MilliSecond)),
    })).unwrap();
    TRADE_RECORD.set(RwLock::new(Vec::new())).unwrap();

    // ファイルの中身を表示する
    if check {
        show_kline_mmap(&KLINE_MMAP, &kline_config).unwrap();
        return;
    }

    start_flush_kline_mmap(&KLINE_MMAP, symbol, &kline_config);

    select! {
        _ = spawn(async move {
            subscribe_trades(symbol, &kline_config).await.capture_result(symbol).await.unwrap();
        }) => {}
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::seconds(5), Duration::seconds(0))).await;
                flush_orderbook_best(symbol).capture_result(symbol).await.unwrap();
                flush_trade_records(symbol).capture_result(symbol).await.unwrap();
            }
        }) => {}
    }
}

#[derive(Debug, Clone)]
struct ServerTimeState {
    server_time: Option<DateTime<Utc>>,
    client_time: Option<DateTime<Utc>>,
}

impl ServerTimeState {
    pub fn new(server_time: DateTime<Utc>) -> Self {
        Self {
            server_time: Some(server_time),
            client_time: Some(Utc::now())
        }
    }
    pub fn now_server_time(&self) -> Option<DateTime<Utc>> {
        match (self.server_time, self.client_time) {
            (Some(s), Some(c)) => Some(s + (Utc::now() - c)),
            _ => None
        }
    }
}

async fn subscribe_trades(symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    let (socket, _) = connect_async(
    Url::parse("wss://ws.lightstream.bitflyer.com/json-rpc").unwrap()).await?;
    info!("Connected to websocket");
    
    let (mut write, mut read) = socket.split();
    
    let channels = vec![
        format!("lightning_executions_{}", symbol.to_native()),
        format!("lightning_board_snapshot_{}", symbol.to_native()),
        format!("lightning_board_{}", symbol.to_native()),
    ];
    
    for channel in channels {
        let op = serde_json::json!({
            "method": "subscribe",
            "params": {"channel": channel}
        });

        write.send(Message::Text(op.to_string())).await?;
    }

    let (us1, ur1) = unbounded::<Message>();
    let (mut us2, ur2) = unbounded::<Message>();

    start_send_ping(symbol, us1).await;

    connect_into_sink(symbol, write, vec![ur1, ur2]);

    while let Some(msg) = read.next().await {
        match handle_trades_msg(msg?, &symbol, kline_config, &mut us2).await {
            Ok(_) => (),
            _ => continue,
        }
    }
    anyhow::bail!("WebSocket disconnected");
}

async fn handle_trades_msg(msg: Message, symbol: &Symbol, kline_config: &Vec<KLineBuilderConfig>, write: &mut UnboundedSender<Message>) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed: WsResponse = serde_json::from_str(msg)?;
    if &parsed.method != "channelMessage" {
        anyhow::bail!("Not channelMessage");
    }
    if parsed.params.channel == format!("lightning_executions_{}", symbol.to_native()) {
        let execution_items = serde_json::from_value::<Vec<ExecutionItem>>(parsed.params.message)?;
        let trades = execution_items.iter().map(|t| t.to_trade_record(*symbol)).collect();
        for conf in kline_config {
            KLINE_MMAP.write()
            .get_mut(&conf.timeframe.0).unwrap().update_ohlcvs(&trades)?;
        }
        TRADE_RECORD.write().extend(trades.into_iter().map(|t| t.mpack()));
        // サーバー時刻の更新
        // (ticker,)execution,boardが順序通りに受信されることは確認しているのでexecutionの時刻で確認する
        *SERVER_TIME.write() = ServerTimeState::new(execution_items.last().unwrap().exec_date);
    } else if parsed.params.channel == format!("lightning_board_snapshot_{}", symbol.to_native()) {
        info!("Board snapshot received");
        let board_snapshot = serde_json::from_value::<BoardResult>(parsed.params.message)?;
        let state = vec![
            board_snapshot.bids.iter().map(|t| (FloatExp::from_f64(t.price, symbol.price_precision()), FloatExp::from_f64(t.size, symbol.amount_precision()))).collect(),
            board_snapshot.asks.iter().map(|t| (FloatExp::from_f64(t.price, symbol.price_precision()), FloatExp::from_f64(t.size, symbol.amount_precision()))).collect(),
        ];
        ORDERBOOK.write().replace_state(state);
        // pybottersの実装準拠、pybottersは公式webの実装準拠らしい
        write.send(Message::Text(serde_json::json!({
                "method": "unsubscribe",
                "params": {"channel": format!("lightning_board_snapshot_{}", symbol.to_native())}
            }).to_string())).await?;
    } else if parsed.params.channel == format!("lightning_board_{}", symbol.to_native()) {
        let board_diff = serde_json::from_value::<BoardResult>(parsed.params.message)?;
        let mut orderbook = ORDERBOOK.write();
        // server_timeがtimeframeの区切りをまたいているとき、snapshotを取る
        if let Some(server_time) =  SERVER_TIME.read().now_server_time() {
            if let Some(snapshot) = orderbook.snapshot_on_update(server_time) {
                // info!("Orderbook snapshot saved. Timestamp: {}, bid price: {}, ask price: {}", snapshot.timestamp, snapshot.snapshot[0][0].0, snapshot.snapshot[1][0].0);
                ORDERBOOK_BEST.write().push(snapshot);
            }
        }
        for &side in &[Side::Buy, Side::Sell] {
            for item in board_diff.by_side(side) {
                if item.price == 0. {
                    continue;
                }
                if item.size == 0. {
                    orderbook.remove(side, FloatExp::from_f64(item.price, symbol.price_precision()));
                } else {
                    orderbook.insert(side, FloatExp::from_f64(item.price, symbol.price_precision()), FloatExp::from_f64(item.size, symbol.amount_precision()));
                }
            }
        }
        let removed = orderbook.arrange(FloatExp::from_f64(board_diff.mid_price, symbol.price_precision()));
        if removed != 0 {
            info!("Arranged orderbook. Removed size: {}", removed);
        }
    // } else if parsed.params.channel == format!("lightning_ticker_{}", symbol.to_native()) {
    //     // サーバー時刻取得用
    //     // ticker,execution,boardが順序通りに受信されることは確認している
    //     let ticker = serde_json::from_value::<TickerResult>(parsed.params.message)?;
    //     *state = Some(ServerTimeState {
    //         server_time: ticker.timestamp,
    //         client_time: Instant::now()
    //     });
    } else {
        anyhow::bail!("Unknown channel: {}", parsed.params.channel);
    }
    Ok(())
}

/// orderbook_bestをmsgpackで書き出し、stateにサーバー時刻を記録する
fn flush_orderbook_best(symbol: Symbol) -> anyhow::Result<()> {
    SerialRecordWriter::<OrderbookBest>::new(
        "orderbook",
        &symbol,
        "msgpack",
        Box::new(orderbook_best_time_fn)
    ).write_msgpack(&ORDERBOOK_BEST.drain())?;
    let server_time_state = SERVER_TIME.read().clone();
    if let (Some(s), Some(c)) = (server_time_state.server_time, server_time_state.client_time) {
        STATUS.write().update(symbol, json!({
            "server_time": s.timestamp_millis(),
            "client_time": c.timestamp_millis()
        }))?;
    }
    Ok(())
}

fn flush_trade_records(symbol: Symbol) -> anyhow::Result<()> {
    SerialRecordWriter::<MpackTradeRecord>::new(
        "marketTrades",
        &symbol,
        "msgpack",
        Box::new(trades_time_fn)
    ).write_msgpack(&TRADE_RECORD.drain())?;
    Ok(())
}