use chrono::Duration;
use futures::{StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use polars::export::ahash::HashMap;
use serde_json::json;
use tokio::{select, spawn};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{config::CrawlerConfig, utils::{orderbook_repository::{OrderbookBest, OrderbookRepository, orderbook_best_time_fn}, strategy_utils::CaptureResult, useful_traits::{TupledResultTranspose, StaticVarExt, StaticVarHashVecExt}, time::{sleep_until_next, ScheduleExpr}, record_writer::SerialRecordWriter, draw_orderbook::OrderbookDrawer, draw::init_terminal}, client::gmo::{WsResponse, OrderbooksResult, WsOkResponse}, symbol::Symbol, data_structure::float_exp::FloatExp, error_types::BotError, global_vars::{get_debug, DebugFlag}};

// HashMap自体はVecへの書き込み時もreadしか要求しないので並列でアクセスできるはず
// https://stackoverflow.com/questions/50282619/is-it-possible-to-share-a-hashmap-between-threads-without-locking-the-entire-has
static ORDERBOOK_BEST: OnceCell<RwLock<HashMap<Symbol, RwLock<Vec<OrderbookBest>>>>> = OnceCell::new();

// for debug
static ORDERBOOK_DRAWER: OnceCell<RwLock<OrderbookDrawer>> = OnceCell::new();

pub async fn start_crawler_gmo(config: &'static CrawlerConfig) {
    let symbol = config.symbols[0];
    ORDERBOOK_BEST.set(RwLock::new(config.symbols.iter().map(|s| (*s, RwLock::new(vec![]))).collect())).unwrap();

    if get_debug()==DebugFlag::Orderbook {
        ORDERBOOK_DRAWER.set(RwLock::new(OrderbookDrawer::new(0, 0, config.symbols.clone()))).unwrap();
        init_terminal().unwrap();
    }

    select! {
        _ = spawn(async move {
            subscribe_ws(config).await.capture_result(symbol).await.unwrap();
        }) => {}
        _ = spawn(async move {
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::seconds(5), Duration::seconds(0))).await;
                flush_orderbook_best(config).capture_result(symbol).await.unwrap();
            }
        }) => {}
    }
}

async fn subscribe_ws(config: &CrawlerConfig) -> anyhow::Result<()> {
    let (socket, _) = connect_async(Url::parse("wss://api.coin.z.com/ws/public/v1")?).await?;
    info!("Connected to websocket");

    let (mut write, mut read) = socket.split();

    for &symbol in &config.symbols {
        write.send(Message::Text(json!({
            "command": "subscribe",
            "channel": "orderbooks",
            "symbol": symbol.to_native(),
        }).to_string())).await?;
        // 連続でsubscribeすると無視されるので少し待つ
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
    }

    while let Some(msg) = read.next().await {
        match handle_ws_msg(msg?, config).await {
            Ok(_) => (),
            Err(e) if e.is::<BotError>() => return Err(e),
            _ => continue,
        }
    }

    anyhow::bail!("Websocket disconnected");
}

async fn handle_ws_msg(msg: Message, _config: &CrawlerConfig) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed: WsResponse = serde_json::from_str(msg)?;
    let res = match parsed {
        WsResponse::Ok(x) => x,
        WsResponse::Err(x) if x.is_too_many_request() => {
            // subscribe失敗なので識別可能なエラーを投げる
            anyhow::bail!(BotError::WsTooManyRequest);
        }
        WsResponse::Err(x) => anyhow::bail!("Websocket error response: {}", x.error),
    };
    match res {
        WsOkResponse::Orderbooks(orderbooks) => {
            let symbol = orderbooks.symbol;
            let repo = OrderbookRepository::new_with_state(Duration::seconds(1), vec![
                orderbooks.bids.into_iter().map(|x| (x.price.into(), x.size.into())).collect(),
                orderbooks.asks.into_iter().map(|x| (x.price.into(), x.size.into())).collect(),
            ]);
            ORDERBOOK_BEST.push(
                symbol,
                OrderbookBest::new(
                    orderbooks.timestamp,
                    repo.get_best()
                )
            );

            if get_debug()==DebugFlag::Orderbook {
                ORDERBOOK_DRAWER.write().print_orderbook(repo.get_best(), symbol)?;
            }
        }
    }
    Ok(())
}

fn flush_orderbook_best(config: &CrawlerConfig) -> anyhow::Result<()> {
    for &symbol in &config.symbols {
        SerialRecordWriter::<OrderbookBest>::new(
            "orderbook",
            &symbol,
            "msgpack",
            Box::new(orderbook_best_time_fn)
        ).write_msgpack(&ORDERBOOK_BEST.drain(symbol))?;
    }
    Ok(())
}