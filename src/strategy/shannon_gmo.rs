use anyhow::Context;
use clokwerk::AsyncScheduler;
use clokwerk::Job;
use clokwerk::TimeUnits;
use log::info;
use maplit::hashmap;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use anyhow::Result;
use serde_json::Value;
use serde_json::json;
use tap::Pipe;

use crate::client::credentials::CREDENTIALS;
use crate::client::gmo::AccountAssets;
use crate::client::gmo::CreateOrderRequest;
use crate::client::gmo::GmoClient;
use crate::client::gmo::GmoClientResponse;
use crate::client::gmo::GmoTimeInForce;
use crate::client::gmo::Tickers;
use crate::client::mail::send_mail;
use crate::data_structure::float_exp::FloatExp;
use crate::data_structure::num_utils::ceil_int;
use crate::data_structure::num_utils::floor_int;
use crate::error_types::BotError;
use crate::order_types::OrderType;
use crate::order_types::Side;
use crate::symbol::{Symbol, SymbolPrecision};

static BALANCE: OnceCell<RwLock<Balance>> = OnceCell::new();

pub fn start_shannon_gmo(scheduler: &mut AsyncScheduler, symbol: Symbol) {
    
    BALANCE.set(RwLock::new(Balance::new(symbol.clone()))).unwrap();
    let symbol_ref1 = symbol.clone();
    let symbol_ref2 = symbol.clone();

    scheduler.every(5u32.minutes()).run(move || {
        let client = GmoClient::new(Some(CREDENTIALS.gmo.clone()));
        let symbol = symbol_ref1.clone();
        async move {
            update_assets(&client, &symbol).await.pipe(capture_result(&symbol));
            cancel_all_orders(&client, &symbol).await.pipe(capture_result(&symbol));
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            create_order(&client, &symbol).await.pipe(capture_result(&symbol));
        }
    });

    // **:58 ごと
    scheduler.every(1u32.hours()).plus(58u32.minutes()).run(move || {
        let symbol = symbol_ref2.clone();
        async move {
            report(&symbol).await.pipe(capture_result(&symbol));
        }
    });
}

fn capture_result(symbol: &Symbol) -> impl Fn(Result<()>) + '_ {
        let l =  |result: Result<()>| {
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

#[derive(Debug)]
pub struct Balance {
    pub base: FloatExp,
    pub quote: FloatExp,
}

impl Balance {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            base: FloatExp::new(0, symbol.amount_precision()),
            quote: FloatExp::new(0, 0),
        }
    }
}

async fn update_assets(client: &GmoClient, symbol: &Symbol) -> Result<()> {
    info!("update_assets");
    let assets: GmoClientResponse<AccountAssets> = client.get_private("/v1/account/assets", None).await?;
    for asset in assets.into_result()? {
        if asset.symbol == symbol.base {
            BALANCE.get().context("BALANCE failed")?.write().base = asset.amount.parse::<f64>()?.pipe(|x| FloatExp::from_f64(x, symbol.amount_precision()));
        } else if asset.symbol == symbol.quote {
            BALANCE.get().context("BALANCE failed")?.write().quote = asset.amount.parse::<i64>()?.pipe(|x| FloatExp::new(x, 0));
        }
    }
    Ok(())
}

async fn cancel_all_orders(client: &GmoClient, symbol: &Symbol) -> Result<()> {
    let orders: GmoClientResponse<Value> = client.post("/v1/cancelBulkOrder", &json!({"symbols": [symbol.to_native()]})).await?;
    orders.into_result()?;
    Ok(())
}

async fn create_order(client: &GmoClient, symbol: &Symbol) -> Result<()> {
    let ticker: GmoClientResponse<Tickers> = client.get_public("/v1/ticker", Some(&hashmap! {"symbol".to_owned() => symbol.to_native()})).await?;
    let last_price = ticker.into_result()?.first().unwrap().last.parse::<i64>()?;
    let mut handles = vec![];
    for &side in &[Side::Buy, Side::Sell] {
        let base_amount = BALANCE.get().context("BALANCE failed")?.read().base;
        let quote = BALANCE.get().context("BALANCE failed")?.read().quote;
        let target_price = if side == Side::Buy {
            floor_int(last_price, (-symbol.amount_precision()) as u32)
            .min(
                // 最小ロットを超えられるprice
                floor_int(
                    (quote / (base_amount + FloatExp::new(1, symbol.amount_precision()) * 2)).to_i64(),
                    (-symbol.amount_precision()) as u32
                )
            )
        } else {
            ceil_int(last_price, (-symbol.amount_precision()) as u32)
            .max(
                ceil_int(
                    (quote / (base_amount - FloatExp::new(1, symbol.amount_precision()) * 2)).to_i64(),
                    (-symbol.amount_precision()) as u32
                )
            )
        };
        let base_cost = base_amount * target_price;
        let rem = quote.min_exp_sub(base_cost).abs();
        let amount = rem / target_price / 2;
        if amount.value == 0 {
            continue;
        }
        let order = CreateOrderRequest {
            symbol: symbol.clone(),
            side,
            execution_type: OrderType::Limit,
            size: format!("{}", amount),
            price: format!("{}", target_price),
            time_in_force: Some(GmoTimeInForce::SOK),
        };
        let c = client.clone();
        info!("send order: {}", serde_json::to_string(&order)?);
        handles.push(tokio::spawn(async move {
            let order_id: GmoClientResponse<String> = match c.post("/v1/order", &order).await {
                Ok(x) => x,
                Err(e) => return Err(e),
            };
            match order_id.into_result() {
                Ok(x) => info!("order_id: {}", x),
                Err(e) => return Err(e),
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.await??;
    }
    
    Ok(())
}

async fn report(symbol: &Symbol) -> Result<()> {
    let balance = BALANCE.get().context("BALANCE failed")?.read();
    let subject = format!("Bot {} {}", symbol.exc, symbol.to_native());
    let body = format!("Balance: {} {}", balance.base, balance.quote);
    send_mail(subject, body)
}