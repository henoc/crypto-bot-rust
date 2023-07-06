use std::collections::HashMap;

use anyhow::{anyhow, Context};
use chrono::Duration;
use futures::{stream::SplitSink, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tap::Pipe;
use tokio::{spawn, net::TcpStream};
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream, tungstenite::Message};

use crate::{symbol::Symbol, client::{mail::send_mail, types::KLines}, error_types::BotError, utils::time::KLinesTimeUnit, config::KLineBuilderConfig};

use super::{kline_mmap::KLineMMap, time::{sleep_until_next, ScheduleExpr}};

/// anyhow::Resultのエラーを回収してメールを送信したのちunwrapして再起動させる
pub fn capture_result(symbol: &Symbol) -> impl Fn(anyhow::Result<()>) + '_ {
    let l =  |result: anyhow::Result<()>| {
        match &result {
            Ok(_) => (),
            Err(e) if matches!(e.downcast_ref::<BotError>(), Some(BotError::Maintenance)) => info!("Maintenance status found"),
            Err(e) => {
                send_mail(format!("{} - {} {}", e, symbol.exc, symbol.to_native()), format!("{:?}", e)).unwrap();
                result.unwrap()
            },
        }
    };
    l
}

/// aiohttpのheartbeat相当。こちらからpingを送信する。pong確認で接続検査が必要かも
pub async fn start_send_ping(symbol: Symbol, mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) {
    spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            write.send(Message::Ping(vec![])).await.map_err(|e| anyhow!(e)).pipe(capture_result(&symbol));
        }
    });
}

/// timeframeおきにkline_mmapをflushする
pub fn start_flush_kline_mmap(kline_mmap: &'static OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, symbol: Symbol, kline_config: &Vec<KLineBuilderConfig>) {
    for config in kline_config.clone() {
        let timeframe = Duration::seconds(config.timeframe_sec);
        spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(timeframe, Duration::seconds(0))).await;
                flush_kline_mmap(kline_mmap, Duration::seconds(config.timeframe_sec)).pipe(capture_result(&symbol));
            }
        });
    }
}

pub fn show_kline_mmap(kline_mmap: &OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, kline_config: &Vec<KLineBuilderConfig>) -> anyhow::Result<()> {
    for config in kline_config {
        let timeframe = Duration::seconds(config.timeframe_sec);
        let hmap = kline_mmap.get().context("KLINE_MMAP is not initialized")?.read();
        let mmap = hmap.get(&timeframe).unwrap();
        let klines = KLines::new_options(&mmap.mmap_read_all()?, KLinesTimeUnit::MilliSecond)?;
        info!("{}:", mmap.get_mmap_path());
        info!("{:?}", klines.df);
    }
    Ok(())
}

fn flush_kline_mmap(kline_mmap: &OnceCell<RwLock<HashMap<Duration, KLineMMap>>>, timeframe: Duration) -> anyhow::Result<()> {
    kline_mmap.get().context("KLINE_MMAP is not initialized")?.write().get_mut(&timeframe).unwrap().update_mmap()?;
    info!("Flushed kline mmap, timeframe: {:?}", timeframe);
    Ok(())
}