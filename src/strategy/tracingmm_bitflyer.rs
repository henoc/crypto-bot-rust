use anyhow::Context;
use chrono::Duration;
use futures::{future::join_all, StreamExt, SinkExt};
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use serde_json::{Value, json};
use tap::Pipe;
use tokio::{select, spawn, try_join, join};
use tokio_tungstenite::{tungstenite::Message, connect_async};
use url::Url;

use crate::{utils::{status_repository::StatusRepository, strategy_utils::{is_logical_postonly, get_liquidity_limited_base, start_send_ping, CaptureResult}, time::{ScheduleExpr, sleep_until_next, UnixTimeUnit, now_floor_time}, kline_mmap::KLineMMap, tracingmm_utils::{TracingMMPosition, tracing_price, TracingPriceResult}, reserved_orders::{ReservedOrdersManager, ReservedOrder}, useful_traits::StaticVarExt}, config::TracingMMConfig, symbol::{Symbol, Currency, SymbolType, Exchange}, client::{bitflyer::{BitflyerClient, CancelAllOrdersRequest, GetPositionRequest, GetPositionResponse, ChildOrderRequest, ChildOrderType, GetCollateralRequest, TickerRequest, ExecutionItem, WsResponse, CancelChildOrderRequest}, credentials::CREDENTIALS, types::KLines}, data_structure::float_exp::FloatExp, order_types::Side};


static STATUS: OnceCell<RwLock<StatusRepository>> = OnceCell::new();
static KLINE: OnceCell<RwLock<KLineMMap>> = OnceCell::new();
static REF_KLINE: OnceCell<RwLock<KLineMMap>> = OnceCell::new();
static SPOT_KLINE: OnceCell<RwLock<KLineMMap>> = OnceCell::new(); // sfd
static POS: OnceCell<RwLock<[TracingMMPosition; 2]>> = OnceCell::new();
static RESERVED: OnceCell<RwLock<ReservedOrdersManager>> = OnceCell::new();

const MAX_SIDE_POSITIONS: i64 = 3;

const MAPPING_SIZE: i64 = 100;


/// 0.01
const ORDER_MIN_AMOUNT: FloatExp = FloatExp::new(1, -2);

// sfd
const SPOT_SYMBOL: Symbol = Symbol::new(Currency::BTC, Currency::JPY, SymbolType::Spot, Exchange::Bitflyer);
const SFD_LIMIT_RATE: FloatExp = FloatExp::new(4, -2);

pub async fn start_tracingmm_bitflyer(config: &'static TracingMMConfig, check: bool) {

    STATUS.set(RwLock::new(StatusRepository::new_init("tracingmm", &config.symbol, Some(Duration::days(3))).unwrap())).unwrap();
    KLINE.set(RwLock::new(KLineMMap::new(config.symbol, config.timeframe.0, 300).unwrap())).unwrap();
    REF_KLINE.set(RwLock::new(KLineMMap::new(config.ref_symbol, config.timeframe.0, 300).unwrap())).unwrap();
    SPOT_KLINE.set(RwLock::new(KLineMMap::new(SPOT_SYMBOL, config.timeframe.0, 300).unwrap())).unwrap(); // sfd
    POS.set(RwLock::new([TracingMMPosition::new(config.symbol.price_precision(), config.symbol.amount_precision()), TracingMMPosition::new(config.symbol.price_precision(), config.symbol.amount_precision())])).unwrap();
    RESERVED.set(RwLock::new(ReservedOrdersManager::new(config.symbol.price_precision()))).unwrap();

    let symbol = config.symbol;
    let timeframe = config.timeframe.0;

    let cancel_ahead = Duration::seconds(1);

    select! {
        _ = spawn(async move {
            let client = BitflyerClient::new(Some(CREDENTIALS.bitflyer.clone()));
            loop {
                sleep_until_next(ScheduleExpr::new_ahead(timeframe, cancel_ahead)).await;
                cancel_all_orders(&client, symbol).await.capture_result(symbol).await.unwrap();
                tokio::time::sleep(cancel_ahead.to_std().unwrap()).await;
                update_order(&client, config).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        _ = spawn(async move {
            let client = BitflyerClient::new(Some(CREDENTIALS.bitflyer.clone()));
            update_assets(&client, config).await.capture_result(symbol).await.unwrap();
            loop {
                sleep_until_next(ScheduleExpr::new(Duration::hours(1), Duration::minutes(7))).await;
                update_assets(&client, config).await.capture_result(symbol).await.unwrap();
            }
        }) => {}
        _ = spawn(async move {
            subscribe_trades(symbol).await.capture_result(symbol).await.unwrap();
        }) => {}
    }
}

async fn cancel_all_orders(client: &BitflyerClient, symbol: Symbol) -> anyhow::Result<()> {
    RESERVED.get().unwrap().write().cancel_all_orders();
    client.post_no_parse(&CancelAllOrdersRequest {
        product_code: symbol.to_native(),
    }).await?;
    info!("cancel all orders");
    Ok(())
}

async fn update_position(client: &BitflyerClient, symbol: Symbol) -> anyhow::Result<()> {
    let res = client.get_private(GetPositionRequest {
        product_code: symbol.to_native(),
    }).await?;
    let mut next_pos = [TracingMMPosition::new(symbol.price_precision(), symbol.amount_precision()), TracingMMPosition::new(symbol.price_precision(), symbol.amount_precision())];
    for pos_detail in res {
        let idx = pos_detail.side as usize;
        next_pos[idx].pos += FloatExp::from_f64(pos_detail.size, symbol.amount_precision());
        next_pos[idx].init_notional += FloatExp::from_f64(pos_detail.price, symbol.price_precision()) * FloatExp::from_f64(pos_detail.size, symbol.amount_precision());
    }
    for idx in 0..2 {
        next_pos[idx].entry_price = if next_pos[idx].pos.is_zero() {
            FloatExp::new(0, symbol.price_precision())
        } else {
            (next_pos[idx].init_notional / next_pos[idx].pos).round(symbol.price_precision())
        };
    }
    info!("update position: {:?}", next_pos);
    *POS.write() = next_pos;
    Ok(())
}

async fn read_kline(kline: &'static OnceCell<RwLock<KLineMMap>>, timeframe: Duration) -> anyhow::Result<KLines> {
    let prev_opentime = now_floor_time(timeframe, -1);
    let mut header_opentime = None;
    for _ in 0..10 {
        header_opentime = Some(kline.read().mmap_read_header());
        if prev_opentime <= header_opentime.unwrap() {
            let klines: KLines = kline.read().mmap_read_all()?.into();
            let klines = klines.reindex(prev_opentime + timeframe, timeframe)?;
            if klines.df.height() < 200 {
                anyhow::bail!("kline is too short. path: {}, len: {}", kline.read().get_mmap_path(), klines.df.height());
            }
            return Ok(klines);
        }
        tokio::time::sleep(Duration::milliseconds(10).to_std().unwrap()).await;
    }
    anyhow::bail!("failed to update kline. curr: {:?}, header: {:?}", prev_opentime, header_opentime);
}

async fn update_order(client: &BitflyerClient, config: &TracingMMConfig) -> anyhow::Result<()> {
    update_position(client, config.symbol).await?;
    let (klines, ref_klines, spot_klines) = try_join!(
        read_kline(&KLINE, config.timeframe.into()),
        read_kline(&REF_KLINE, config.timeframe.into()),
        read_kline(&SPOT_KLINE, config.timeframe.into()), // sfd
    )?;
    let sfd = get_sfd(&klines, &spot_klines, config.timeframe.into())?;
    let prices = tracing_price(klines.df, ref_klines.df, MAPPING_SIZE, config.atr_period, &config.beta, &config.gamma)?;
    info!("update_order prices: {:?}, sfd: {}", prices, sfd);
    send_new_orders(client, config, &prices, sfd).await?;
    Ok(())
}

fn get_sfd(klines: &KLines, spot_klines: &KLines, timeframe: Duration) -> anyhow::Result<f64> {
    let opentime = now_floor_time(timeframe, -1);
    let close = klines.at(opentime, "close")?;
    let spot_close = spot_klines.at(opentime, "close")?;
    Ok(close.context("close is empty")? / spot_close.context("spot_close is empty")? - 1.0)
}

async fn send_new_orders(client: &BitflyerClient, config: &TracingMMConfig, prices: &TracingPriceResult, sfd: f64) -> anyhow::Result<()> {
    let pos = POS.read().clone();
    let last_close = FloatExp::from_f64(prices.last_close, config.symbol.price_precision());
    let mut close_orders = vec![];
    let mut open_orders = vec![];
    // close order
    for &side in &[Side::Buy, Side::Sell] {
        if pos[side.inv() as usize].pos.is_zero() {
            continue;
        }
        let price = FloatExp::from_f64(prices.by_side(side).out, config.symbol.price_precision());
        close_orders.push(close_order(client, config, side, price, pos[side.inv() as usize].pos, last_close));
    }
    // open order
    let sfd_cond = [sfd < SFD_LIMIT_RATE.to_f64(), -SFD_LIMIT_RATE.to_f64() < sfd];
    for &side in &[Side::Buy, Side::Sell] {
        if !sfd_cond[side as usize] {
            info!("SFD is out of range. side: {:?}, SFD: {}", side, sfd);
            continue;
        }
        let price = FloatExp::from_f64(prices.by_side(side).r#in, config.symbol.price_precision());
        let amount = next_open_amount(&config.symbol, side, price);
        if let Some(amount) = amount {
            open_orders.push(open_order(client, config, side, price, amount, last_close));
        }
    }
    let (a, b) = join!(join_all(close_orders), join_all(open_orders));
    // into_iter -> collect で anyhow::Result<Vec<()>> になる
    // https://stackoverflow.com/questions/63798662/how-do-i-convert-a-vecresultt-e-to-resultvect-e
    a.into_iter().chain(b.into_iter()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}

/// 使用可能な注文量を計算する
fn next_open_amount(symbol: &Symbol, side: Side, price: FloatExp) -> Option<FloatExp> {
    let status = STATUS.read()[symbol].clone();
    let quote_for_order = status["available_quote"].as_f64()?.min(status["liquidity_limited_base"].as_f64()? * MAX_SIDE_POSITIONS as f64 * price.to_f64());
    let order_amount = quote_for_order / MAX_SIDE_POSITIONS as f64 / price.to_f64();

    let init_notional = POS.read()[side as usize].init_notional.to_f64();
    if init_notional + order_amount * price.to_f64() < quote_for_order {
        Some(FloatExp::from_f64(order_amount, symbol.amount_precision()))
    } else {
        info!("next_open_amount not enough quote. side: {:?}, quote_for_order: {}, init_notional: {}, order_amount: {}", side, quote_for_order, init_notional, order_amount);
        None
    }
}

async fn open_order(client: &BitflyerClient, config: &TracingMMConfig, side: Side, price: FloatExp, amount: FloatExp, last_close: FloatExp) -> anyhow::Result<()> {
    if amount < ORDER_MIN_AMOUNT {
        info!("open_order amount too small: {}", amount);
        return Ok(());
    }
    
    if !is_logical_postonly(side, price, last_close) {
        info!("open_order not logical postonly, side: {:?}", side);
        return Ok(());
    }
    let res = client.post(&ChildOrderRequest {
        product_code: config.symbol.to_native(),
        child_order_type: ChildOrderType::Limit,
        side,
        price: Some(price),
        size: amount,
        minute_to_expire: None,
    }).await?;
    info!("open_order. side: {:?}, price: {}, amount: {}, id: {}", side, price, amount, res.child_order_acceptance_id);
    Ok(())
}

async fn close_order(client: &BitflyerClient, config: &TracingMMConfig, side: Side, price: FloatExp, amount: FloatExp, last_close: FloatExp) -> anyhow::Result<()> {
    if amount < ORDER_MIN_AMOUNT {
        info!("close_order amount too small: {}", amount);
        return Ok(());
    }

    if !is_logical_postonly(side, price, last_close) {
        info!("close_order not logical postonly, side: {:?}", side);
        return Ok(());
    }

    let res = client.post(&ChildOrderRequest {
        product_code: config.symbol.to_native(),
        child_order_type: ChildOrderType::Limit,
        side,
        price: Some(price),
        size: amount,
        minute_to_expire: None,
    }).await?;
    info!("close_order. side: {:?}, price: {}, amount: {}, id: {}", side, price, amount, res.child_order_acceptance_id);

    // ロスカット逆指値
    if let Some(losscut_rate) = config.losscut_rate {
        let pos = POS.read().clone();
        let pos_side = side.inv().to_pos();
        let losscut_price = pos[pos_side as usize].entry_price * (1.0 - losscut_rate * pos_side.sign() as f64);
        RESERVED.write().add_reserved_order(
            side, side.inv().to_pos(), losscut_price, amount, true, Some(res.child_order_acceptance_id)
        );
    }
    Ok(())
}

async fn update_assets(client: &BitflyerClient, config: &TracingMMConfig) -> anyhow::Result<()> {
    let (balance, ticker) = try_join!(
        client.get_private(GetCollateralRequest {}),
        client.get_public(TickerRequest { product_code: config.symbol.to_native() })
    )?;
    let mut fixed_margin = STATUS.read()[&config.symbol]["fixed_margin"].as_f64().unwrap_or(0.0);
    fixed_margin = fixed_margin.max(balance.collateral * 0.8);
    let available_quote = fixed_margin * config.leverage;
    let liquidity_limited_base = get_liquidity_limited_base(
        ticker.volume_by_product,
        config.timeframe.0 * config.exit_mean_frame, 
        MAX_SIDE_POSITIONS, 
        1.0, 
        config.beta.r#in==config.beta.out && config.gamma.r#in==config.gamma.out
    );

    STATUS.write().update(config.symbol, json!({
        "fixed_margin": fixed_margin,
        "available_quote": available_quote,
        "liquidity_limited_base": liquidity_limited_base,
    }))?;

    info!("update_assets. fixed_margin: {}, available_quote: {}, liquidity_limited_base: {}", fixed_margin, available_quote, liquidity_limited_base);
    Ok(())
}

async fn subscribe_trades(symbol: Symbol) -> anyhow::Result<()> {
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
        match handle_trades_msg(msg?, symbol) {
            Ok(_) => (),
            _ => continue,
        }
    }
    anyhow::bail!("WebSocket disconnected");
}

fn handle_trades_msg(msg: Message, symbol: Symbol) -> anyhow::Result<()> {
    let msg = msg.to_text()?;
    let parsed: WsResponse = serde_json::from_str(msg)?;
    if &parsed.method != "channelMessage" {
        anyhow::bail!("Not channelMessage");
    }
    if &parsed.params.channel != &format!("lightning_executions_{}", symbol.to_native()) {
        anyhow::bail!("Not channel for lightning_executions_{}", symbol.to_native());
    }
    let trades = serde_json::from_value::<Vec<ExecutionItem>>(parsed.params.message)?;
    let trades = trades.into_iter().map(|t| t.to_trade_record(symbol)).collect();

    // reserved ordersの発火
    let orders = RESERVED.write().trades_handler(&trades);
    spawn(async move {
        let client = BitflyerClient::new(Some(CREDENTIALS.bitflyer.clone()));
        join_all(orders.into_iter().map(|o| fire_reserved_order(&client, symbol, o))).await
            .into_iter().collect::<anyhow::Result<()>>()
            .capture_result(symbol).await.unwrap();
    });
    Ok(())
}

async fn fire_reserved_order(client: &BitflyerClient, symbol: Symbol, order: ReservedOrder) -> anyhow::Result<()> {
    let req = ChildOrderRequest {
        product_code: symbol.to_native(),
        child_order_type: ChildOrderType::Market,
        side: order.side,
        price: None,
        size: order.amount,
        minute_to_expire: None,
    };
    let cancel_req = order.pair_order_id.as_ref().map(|id| CancelChildOrderRequest {
        product_code: symbol.to_native(),
        child_order_acceptance_id: id.clone(),
    });
    let res = match cancel_req {
        Some(cancel_req) => {
            try_join!(client.post(&req), client.post(&cancel_req))?.0
        },
        None => client.post(&req).await?,
    };
    info!("fire_reserved_order. side: {:?}, price: {}, amount: {}, id: {}", order.side, order.price, order.amount, res.child_order_acceptance_id);
    Ok(())
}