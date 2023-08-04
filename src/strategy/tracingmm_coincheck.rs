use anyhow::anyhow;
use chrono::Duration;
use std::time::Duration as StdDuration;
use futures::{future::join_all, StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tokio::{select, spawn, try_join, join};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::{config::TracingMMConfig, utils::{status_repository::StatusRepository, kline_mmap::KLineMMap, tracingmm_utils::{TracingMMPosition, tracing_price, read_kline, TracingPriceResult, next_open_amount}, reserved_orders::{ReservedOrdersManager, ReservedOrder}, time::{ScheduleExpr, sleep_until_next}, useful_traits::{StaticVarExt, ResultFlatten}, strategy_utils::{CaptureResult, is_logical_postonly, update_assets_inner, start_send_ping}, orderbook_repository::{OrderbookRepository, apply_diff_once}, draw_orderbook::OrderbookDrawer, draw::init_terminal}, client::{coincheck::{CoincheckClient, OpenOrderRequest, RestResponse, BalanceRequest, TransactionsRequest, TickerRequest, WsResponse, OrderRequest, LimitOrderRequest, TimeInForce, OrderbookRequest}, credentials::CREDENTIALS}, symbol::Symbol, data_structure::{float_exp::FloatExp, time_queue::TimeQueue}, order_types::{Side, OrderType}, global_vars::{get_debug, DebugFlag}};

static STATUS: OnceCell<RwLock<StatusRepository>> = OnceCell::new();
static KLINE: OnceCell<RwLock<KLineMMap>> = OnceCell::new();
static REF_KLINE: OnceCell<RwLock<KLineMMap>> = OnceCell::new();
static POS: OnceCell<RwLock<[TracingMMPosition; 2]>> = OnceCell::new();
static RESERVED: OnceCell<RwLock<ReservedOrdersManager>> = OnceCell::new();

static ORDERBOOK: OnceCell<RwLock<OrderbookRepository>> = OnceCell::new();
static ORDERBOOK_DIFF: OnceCell<RwLock<[TimeQueue<(f64, f64)>; 2]>> = OnceCell::new();

// for debug
static ORDERBOOK_DRAWER: OnceCell<RwLock<OrderbookDrawer>> = OnceCell::new();

const MAX_SIDE_POSITIONS: i64 = 3;

const MAPPING_SIZE: i64 = 100;

/// 0.005
const ORDER_MIN_AMOUNT: FloatExp = FloatExp::new(5, -3);

const ORDERBOOK_DIFF_DURATION: StdDuration = StdDuration::from_secs(5);

/// orderbookのn番目の価格を交差していたらreserved_orderを発火させる
const ORDERBOOK_NTH: usize = 4;

pub async fn start_tracingmm_coincheck(config: &'static TracingMMConfig) {

    STATUS.set(RwLock::new(StatusRepository::new_init("tracingmm", &config.symbol, Some(Duration::days(3))).unwrap())).unwrap();
    KLINE.set(RwLock::new(KLineMMap::new(config.symbol, config.timeframe.0, 300).unwrap())).unwrap();
    REF_KLINE.set(RwLock::new(KLineMMap::new(config.ref_symbol, config.timeframe.0, 300).unwrap())).unwrap();
    POS.set(RwLock::new([TracingMMPosition::new(config.symbol.price_precision(), config.symbol.amount_precision()), TracingMMPosition::new(config.symbol.price_precision(), config.symbol.amount_precision())])).unwrap();
    RESERVED.set(RwLock::new(ReservedOrdersManager::new(config.symbol.price_precision()))).unwrap();

    ORDERBOOK.set(RwLock::new(OrderbookRepository::new(Duration::seconds(1)))).unwrap();
    ORDERBOOK_DIFF.set(RwLock::new([TimeQueue::new(ORDERBOOK_DIFF_DURATION), TimeQueue::new(ORDERBOOK_DIFF_DURATION)])).unwrap();

    if get_debug()==DebugFlag::Orderbook {
        ORDERBOOK_DRAWER.set(RwLock::new(OrderbookDrawer::new(0, 0, vec![config.symbol]))).unwrap();
        init_terminal().unwrap();
    }

    let symbol = config.symbol;

    let cancel_ahead = StdDuration::from_secs(1);

    select! {
        _ = spawn(async move {
            let client = CoincheckClient::new(Some(CREDENTIALS.coincheck.clone()));
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::minutes(1), Duration::minutes(0))).await;
                cancel_all_orders(&client, symbol).await.capture_result(symbol).await.unwrap();
                tokio::time::sleep(cancel_ahead).await;
                update_order(&client, config).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        _ = spawn(async move {
            let client = CoincheckClient::new(Some(CREDENTIALS.coincheck.clone()));
            update_assets(&client, config).await.capture_result(symbol).await.unwrap();
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::minutes(1), Duration::minutes(0))).await;
                update_assets(&client, config).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        _ = spawn(async move {
            subscribe_ws(config.symbol).await.unwrap();
        }) => {}
        _ = spawn(async move {
            let client = CoincheckClient::new(None);
            replace_orderbook_state(&client).await.capture_result(symbol).await.unwrap();
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::minutes(1), Duration::seconds(1))).await;
                replace_orderbook_state(&client).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
    }
}

async fn cancel_all_orders(client: &CoincheckClient, symbol: Symbol) -> anyhow::Result<()> {
    RESERVED.write().cancel_all_orders();
    let res = client.get_public(OpenOrderRequest {}).await?;
    spawn(async move {
        let client = CoincheckClient::new(Some(CREDENTIALS.coincheck.clone()));
        join_all(
            res.orders.iter().filter(|o| o.pair == symbol).map(|o| o.id).enumerate()
            .map(|(i, order_id)|
                client.cancel_order(order_id, i as i64 * 10))
        ).await.into_iter().map(
            |r| r.map(
                |rr|
                        rr.into_result().map(|_| ())
            ).flatten_()
        ).collect::<anyhow::Result<()>>()
        .capture_result(symbol).await.unwrap();
    });
    info!("cancel all orders");
    Ok(())
}

async fn update_position(client: &CoincheckClient, symbol: Symbol) -> anyhow::Result<()> {
    let (balance, trades) = try_join!(client.get_private(BalanceRequest, 0), client.get_private(TransactionsRequest, 10))?;
    let mut next_pos = [TracingMMPosition::new(symbol.price_precision(), symbol.amount_precision()), TracingMMPosition::new(symbol.price_precision(), symbol.amount_precision())];
    next_pos[0].pos = FloatExp::from_f64(balance.btc + balance.btc_reserved, symbol.amount_precision());
    // 約定履歴を逆順にたどる
    // amount == 0になるところで終わり
    let mut amount = next_pos[0].pos;
    for trade in trades.transactions {
        if amount.is_zero() {break;}
        // trades.funds.btcは符号付きの値
        next_pos[0].init_notional += FloatExp::from_f64(trade.rate, symbol.price_precision()) * FloatExp::from_f64(trade.funds.btc, symbol.amount_precision());
        amount -= FloatExp::from_f64(trade.funds.btc, symbol.amount_precision());
    }

    if !next_pos[0].pos.is_zero() {
        next_pos[0].entry_price = (next_pos[0].init_notional / next_pos[0].pos).round(symbol.price_precision());
    }
    info!("update position: {:?}", next_pos);
    *POS.write() = next_pos;
    Ok(())
}

async fn update_order(client: &CoincheckClient, config: &TracingMMConfig) -> anyhow::Result<()> {
    update_position(client, config.symbol).await?;
    let (klines, ref_klines) = try_join!(
        read_kline(&KLINE, config.timeframe.into()),
        read_kline(&REF_KLINE, config.timeframe.into())
    )?;
    let prices = tracing_price(klines.df, ref_klines.df, MAPPING_SIZE, config.atr_period, &config.beta, &config.gamma)?;
    info!("update_order price: {:?}", prices);
    send_new_orders(config, &prices).await?;
    Ok(())
}

async fn send_new_orders(config: &TracingMMConfig, prices: &TracingPriceResult) -> anyhow::Result<()> {
    let pos = POS.read().clone();
    let last_close = FloatExp::from_f64(prices.last_close, config.symbol.price_precision());
    let mut close_orders = vec![];
    let mut open_orders = vec![];
    // close order
    for &side in &[Side::Buy] {
        if pos[side.inv() as usize].pos.is_zero() {
            continue;
        }
        let price = FloatExp::from_f64(prices.by_side(side).out, config.symbol.price_precision());
        close_orders.push(close_order(config, side, price, pos[side.inv() as usize].pos, last_close));
    }
    // open order
    for &side in &[Side::Buy] {
        let price = FloatExp::from_f64(prices.by_side(side).r#in, config.symbol.price_precision());
        let amount = next_open_amount(&STATUS, &POS, MAX_SIDE_POSITIONS, &config.symbol, side, price);
        if let Some(amount) = amount {
            open_orders.push(open_order(side, price, amount, last_close));
        }
    }
    let (a, b) = join!(join_all(close_orders), join_all(open_orders));
    a.into_iter().chain(b.into_iter()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}

async fn open_order(side: Side, price: FloatExp, amount: FloatExp, last_close: FloatExp) -> anyhow::Result<()> {
    if amount < ORDER_MIN_AMOUNT {
        info!("open_order amount too small: {}", amount);
        return Ok(());
    }
    
    if !is_logical_postonly(side, price, last_close) {
        info!("open_order not logical postonly, side: {:?}", side);
        return Ok(());
    }

    RESERVED.write().add_reserved_order(OrderType::Limit, side, side.to_pos(), price, amount, None);
    info!("open_order(reserved). side: {:?}, price: {}, amount: {}", side, price, amount);
    Ok(())
}

async fn close_order(config: &TracingMMConfig, side: Side, price: FloatExp, amount: FloatExp, last_close: FloatExp) -> anyhow::Result<()> {
    if amount < ORDER_MIN_AMOUNT {
        info!("close_order amount too small: {}", amount);
        return Ok(());
    }
    
    if !is_logical_postonly(side, price, last_close) {
        info!("close_order not logical postonly, side: {:?}", side);
        return Ok(());
    }

    let rid = RESERVED.write().add_reserved_order(OrderType::Limit, side, side.inv().to_pos(), price, amount, None);
    info!("close_order(reserved). side: {:?}, price: {}, amount: {}", side, price, amount);
    
    // ロスカット逆指値
    if let Some(losscut_rate) = config.losscut_rate {
        let pos = POS.read().clone();
        let pos_side = side.inv().to_pos();
        let losscut_price = pos[pos_side as usize].entry_price * (1.0 - losscut_rate * pos_side.sign() as f64);
        let losscut_id = RESERVED.write().add_reserved_order(
            OrderType::Stop, side, side.inv().to_pos(), losscut_price, amount, None
        );
        RESERVED.write().get_mut(&losscut_id).unwrap().pair_rsv_order_id = Some(rid);
    }
    Ok(())
}

async fn update_assets(client: &CoincheckClient, config: &TracingMMConfig) -> anyhow::Result<()> {
    let (balance, ticker) = try_join!(
        client.get_private(BalanceRequest, 0),
        client.get_public(TickerRequest {pair: config.symbol})
    )?;
    update_assets_inner(&STATUS, config, balance.jpy + balance.jpy_reserved, ticker.volume)?;
    Ok(())
}

/// orderbookのsnapshotを取得して直近の差分をすべて適用する
async fn replace_orderbook_state(client: &CoincheckClient) -> anyhow::Result<()> {
    let res = client.get_public(OrderbookRequest).await?;
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

async fn subscribe_ws(symbol: Symbol) -> anyhow::Result<()> {
    let (socket, _) =
        connect_async(Url::parse("wss://ws-api.coincheck.com/").unwrap()).await?;
    info!("Connected to websocket");

    let (mut write, mut read) = socket.split();

    let channels = vec![
        format!("{}-trades", symbol.to_native()),
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
        match handle_ws_msg(msg?, symbol) {
            Ok(_) => (),
            Err(e) => {
                info!("catched error in handle_ws_msg: {}", e);
                continue;
            },
        }
    }
    anyhow::bail!("WebSocket disconnected");
}

fn handle_ws_msg(msg: Message, symbol: Symbol) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed = serde_json::from_str::<WsResponse>(msg)?;
    let orders = match parsed {
        WsResponse::Trade(trade) => {
            let trades = trade.to_trade_records()?;
            RESERVED.write().trades_handler(&trades)
        },
        WsResponse::Orderbook(res) => {
            let mut orderbook = ORDERBOOK.write();
            let mut orderbook_diff = ORDERBOOK_DIFF.write();
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
            let best = orderbook.get_best::<ORDERBOOK_NTH>();
            let best_nth = [best[0][ORDERBOOK_NTH-1], best[1][ORDERBOOK_NTH-1]];
            RESERVED.write().orderbook_handler(best_nth)
        },
    };
    spawn(async move {
        let client = CoincheckClient::new(Some(CREDENTIALS.coincheck.clone()));
        join_all(
            orders.into_iter().map(|o| {
                fire_reserved_order(&client, symbol, o)
            })
        ).await.into_iter().collect::<anyhow::Result<()>>()
        .capture_result(symbol).await.unwrap();
    });
    Ok(())
}

async fn fire_reserved_order(client: &CoincheckClient, symbol: Symbol, reserved_order: ReservedOrder) -> anyhow::Result<()> {
    let req = match reserved_order.order_type {
        OrderType::Limit | OrderType::StopLimit => {
            OrderRequest::limit_order(
                reserved_order.side,
                symbol,
                reserved_order.price,
                reserved_order.amount,
                None,   // orderbookが速いとpost_onlyでは間に合わないこともありそうなので無し（post_onlyにする必要もない）
            )
        },
        OrderType::Market | OrderType::Stop => {
            if reserved_order.side == Side::Buy {
                anyhow::bail!("invalid market order: {:?}", reserved_order);
            }
            OrderRequest::market_order(
                reserved_order.side,
                symbol,
                reserved_order.amount,
                None,
            )
        }
    };
    if let Some(pair_rsv_order_id) = reserved_order.pair_rsv_order_id {
        RESERVED.write().remove(&pair_rsv_order_id);
    }
    let res = client.post(&req, 0).await?.into_result()?;
    info!("fire_reserved_order. type: {:?}, side: {:?}, price: {}, amount: {}, id: {}", reserved_order.order_type, reserved_order.side, reserved_order.price, reserved_order.amount, res.id);
    Ok(())
}