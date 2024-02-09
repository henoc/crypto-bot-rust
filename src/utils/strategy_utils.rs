use std::{collections::HashMap, env, process::exit};

use anyhow::{Context, self};
use async_trait::async_trait;
use labo::export::chrono::Duration;
use futures::{stream::SplitSink, SinkExt, Sink, channel::mpsc::UnboundedReceiver, StreamExt};
use hyper::StatusCode;
use log::info;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::{spawn, net::TcpStream};
use tokio_stream::StreamMap;
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream, tungstenite::Message};

use crate::{symbol::{Symbol, Exchange}, client::{mail::send_mail, types::KLines}, error_types::BotError, utils::time::{UnixTimeUnit, now_floor_time}, data_structure::float_exp::FloatExp, order_types::{PosSide, Side}};


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
            sink.send(Message::Ping(vec![])).await.map_err(|e| anyhow::anyhow!(e)).capture_result(symbol).await.unwrap();
        }
    });
}

pub fn connect_into_sink<E: Into<anyhow::Error> + Send, T: Sink<Message, Error = E> + Unpin + Send + 'static>(symbol: Symbol, mut sink: T, streams: Vec<UnboundedReceiver<Message>>) {
    let mut all = StreamMap::new();
    for (i, rdr) in streams.into_iter().enumerate() {
        all.insert(i, rdr);
    }
    spawn(async move {
        sink.send_all(&mut all.map(|(_, x)| Ok(x))).await.map_err(|e| anyhow::anyhow!(e)).capture_result(symbol).await.unwrap();
    });
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
