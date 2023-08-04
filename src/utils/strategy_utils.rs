use std::{collections::HashMap, env, process::exit};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::Duration;
use futures::{stream::SplitSink, SinkExt, Sink, channel::mpsc::UnboundedReceiver, StreamExt};
use hyper::StatusCode;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde_json::json;
use tap::Pipe;
use tokio::{spawn, net::TcpStream};
use tokio_stream::StreamMap;
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream, tungstenite::Message};

use crate::{symbol::{Symbol, Exchange}, client::{mail::send_mail, types::KLines}, error_types::BotError, utils::time::{UnixTimeUnit, now_floor_time}, config::{KLineBuilderConfig, TracingMMConfig}, data_structure::float_exp::FloatExp, order_types::{PosSide, Side}};

use super::{kline_mmap::KLineMMap, time::{sleep_until_next, ScheduleExpr}, status_repository::StatusRepository, useful_traits::StaticVarExt};

#[async_trait]
pub trait CaptureResult {
    async fn capture_result(self, symbol: Symbol) -> anyhow::Result<()>;
}

#[async_trait]
impl CaptureResult for anyhow::Result<()> {
    async fn capture_result(self, symbol: Symbol) -> anyhow::Result<()> {
        match self {
            Err(e) => {
                match e.downcast_ref::<BotError>() {
                    Some(BotError::Maintenance) => {
                        info!("Maintenance status found");
                        return Ok(());
                    },
                    Some(BotError::MarginInsufficiency) => {
                        info!("Margin insufficiency found");
                        return Ok(());
                    },
                    _ => {},
                };
                match e.downcast_ref::<tokio_tungstenite::tungstenite::Error>() {
                    Some(wse) if symbol.exc == Exchange::Bitflyer || symbol.exc == Exchange::Gmo => {
                        match wse {
                            tokio_tungstenite::tungstenite::Error::Http(res) => {
                                match res.status() {
                                    StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => {
                                        info!("{} websocket disconnected (5xx), wait 60s", symbol.exc);
                                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                                        std::env::remove_var("RUST_BACKTRACE");
                                        return Err(e);
                                    },
                                    _ => {},
                                }
                            },
                            _ => {},
                        }
                    },
                    _ => {},
                }
                send_mail(format!("{} - {}", e, env::var("NAME").unwrap()), format!("{:?}", e)).unwrap();
                return Err(e);
            }
            _ => return Ok(()),
        }
    }
}

/// aiohttpのheartbeat相当。こちらからpingを送信する。pong確認で接続検査が必要かも
pub async fn start_send_ping<E: Into<anyhow::Error> + Send, T: Sink<Message, Error = E> + Unpin + Send + 'static>(symbol: Symbol, mut sink: T) {
    spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            sink.send(Message::Ping(vec![])).await.map_err(|e| anyhow!(e)).capture_result(symbol).await.unwrap();
        }
    });
}

pub fn connect_into_sink<E: Into<anyhow::Error> + Send, T: Sink<Message, Error = E> + Unpin + Send + 'static>(symbol: Symbol, mut sink: T, streams: Vec<UnboundedReceiver<Message>>) {
    let mut all = StreamMap::new();
    for (i, rdr) in streams.into_iter().enumerate() {
        all.insert(i, rdr);
    }
    spawn(async move {
        sink.send_all(&mut all.map(|(_, x)| Ok(x))).await.map_err(|e| anyhow!(e)).capture_result(symbol).await.unwrap();
    });
}

/// timeframeおきにkline_mmapをflushする
pub fn start_flush_kline_mmap(kline_mmap: &'static OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) {
    for config in kline_config.clone() {
        let timeframe = config.timeframe.0;
        spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(timeframe, Duration::seconds(0))).await;
                flush_kline_mmap(kline_mmap, config.timeframe.0).capture_result(symbol).await.unwrap();
            }
        });
    }
}

pub fn show_kline_mmap(kline_mmap: &OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    for config in kline_config {
        let timeframe = config.timeframe.0;
        let hmap = kline_mmap.get().context("KLINE_MMAP is not initialized")?.read();
        let mmap = hmap.get(&timeframe).unwrap();
        let df = mmap.mmap_read_all()?;
        info!("{}:", mmap.get_mmap_path());
        info!("{:?}", df);
    }
    Ok(())
}

fn flush_kline_mmap(kline_mmap: &OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, timeframe: Duration) -> anyhow::Result<()> {
    let head_opentime = now_floor_time(timeframe, -1);
    kline_mmap.get().context("KLINE_MMAP is not initialized")?.write().get_mut(&timeframe).unwrap().update_mmap_with_shift(head_opentime)?;
    info!("Flushed kline mmap, timeframe: {:?}", timeframe);
    Ok(())
}

pub fn is_logical_postonly(side: Side, price: FloatExp, last_close: FloatExp) -> bool {
    match side {
        Side::Buy => price < last_close,
        Side::Sell => price > last_close,
    }
}

pub fn get_liquidity_limited_base(base_volume_1d: f64, exit_mean_period: Duration, unit_count: i64, contract_size: f64, doten: bool) -> f64 {
    let daily_trial = Duration::days(1).num_seconds() as f64 / exit_mean_period.num_seconds() as f64;
    base_volume_1d * contract_size / daily_trial / (unit_count as f64 + doten as i64 as f64) * 0.01
}

pub fn update_assets_inner(status: &OnceCell<RwLock<StatusRepository>>, config: &TracingMMConfig, current_margin: f64, base_volume_1d: f64) -> anyhow::Result<()> {
    let mut fixed_margin = status.read()[&config.symbol]["fixed_margin"].as_f64().unwrap_or(0.0);
    fixed_margin = fixed_margin.max(current_margin * 0.8);
    let available_quote = fixed_margin * config.leverage;
    let liquidity_limited_base = get_liquidity_limited_base(
        base_volume_1d,
        config.timeframe.0 * config.exit_mean_frame, 
        config.max_side_positions, 
        1.0, 
        config.beta.r#in==config.beta.out && config.gamma.r#in==config.gamma.out
    );

    status.write().update(config.symbol, json!({
        "fixed_margin": fixed_margin,
        "available_quote": available_quote,
        "liquidity_limited_base": liquidity_limited_base,
    }))?;

    info!("update_assets. fixed_margin: {}, available_quote: {}, liquidity_limited_base: {}", fixed_margin, available_quote, liquidity_limited_base);
    Ok(())
}