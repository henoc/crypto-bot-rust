use anyhow::{Context, anyhow};
use chrono::Duration;
use log::info;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use polars::{prelude::{DataFrame, IntoLazy, RollingOptions, EWMOptions}, lazy::dsl::{col, lit}};
use serde::Deserialize;

use crate::{order_types::{PosSide, Side}, data_structure::float_exp::FloatExp, utils::{time::now_floor_time, useful_traits::StaticVarExt}, client::types::KLines, symbol::Symbol};

use super::{kline_mmap::KLineMMap, status_repository::StatusRepository};


#[derive(Debug, Clone)]
pub struct TracingMMPosition {
    pub pos: FloatExp,
    pub entry_price: FloatExp,
    pub init_notional: FloatExp,
}

impl TracingMMPosition {
    pub const fn new(price_exp: i32, amount_exp: i32) -> Self {
        Self {
            pos: FloatExp::new(0, amount_exp),
            entry_price: FloatExp::new(0, price_exp),
            init_notional: FloatExp::new(0, price_exp + amount_exp),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceInOut {
    pub r#in: f64,
    pub out: f64,
}

impl PriceInOut {
    pub fn new(r#in: f64, out: f64) -> Self {
        Self {
            r#in,
            out,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TracingPriceResult {
    pub buy_price: PriceInOut,
    pub sell_price: PriceInOut,
    pub last_close: f64,
}

impl TracingPriceResult {
    pub fn by_side(&self, side: Side) -> &PriceInOut {
        match side {
            Side::Buy => &self.buy_price,
            Side::Sell => &self.sell_price,
        }
    }
}

pub async fn read_kline(kline: &'static OnceCell<RwLock<KLineMMap>>, timeframe: Duration) -> anyhow::Result<KLines> {
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

fn tracing_price_df(df: DataFrame, ref_df: DataFrame, mapping_size: i64, atr_period: i64, beta: &PriceInOut, gamma: &PriceInOut) -> anyhow::Result<DataFrame> {
    if df.column("opentime")? != &df.column("opentime")?.sort(false) {
        anyhow::bail!("opentime must be sorted");
    }
    
    // min_periods = 1 になるが影響はない
    let mut rolling_options = RollingOptions::default();
    rolling_options.window_size = polars::time::Duration::parse(&format!("{}i", mapping_size));
    let mut ewm_options = EWMOptions::default();
    ewm_options.alpha = 1.0 / atr_period as f64;
    ewm_options.adjust = false;

    let df = df.lazy().left_join(ref_df.lazy().select(vec![
        col("opentime"),
        col("close").alias("cl_ref"),
    ]), col("opentime"), col("opentime"))
        .with_columns(vec![
        (col("close").rolling_mean(rolling_options.clone()) / col("cl_ref").rolling_mean(rolling_options)).alias("conv_rate"),
    ]).with_columns(vec![
        (col("cl_ref") * col("conv_rate")).alias("cl_ref_mapped"),
        (col("cl_ref").pct_change(1)).alias("cl_trend"),
        (col("high") - col("low")).ewm_mean(ewm_options).alias("atr")
    ]).with_columns(vec![
        (col("cl_ref_mapped") * (lit(1.0) - col("cl_trend").abs() * lit(gamma.r#in)) - col("atr") * lit(beta.r#in)).alias("buy_price"),
        (col("cl_ref_mapped") * (lit(1.0) + col("cl_trend").abs() * lit(gamma.r#in)) + col("atr") * lit(beta.r#in)).alias("sell_price"),
        (col("cl_ref_mapped") * (lit(1.0) - col("cl_trend").abs() * lit(gamma.r#out)) - col("atr") * lit(beta.r#out)).alias("buy_exit"),
        (col("cl_ref_mapped") * (lit(1.0) + col("cl_trend").abs() * lit(gamma.r#out)) + col("atr") * lit(beta.r#out)).alias("sell_exit"),
    ]).collect()?;
    if df["opentime"] != df["opentime"].sort(false) {
        anyhow::bail!("opentime must be sorted");
    }
    Ok(df)
}

pub fn tracing_price(df: DataFrame, ref_df: DataFrame, mapping_size: i64, atr_period: i64, beta: &PriceInOut, gamma: &PriceInOut) -> anyhow::Result<TracingPriceResult> {
    let df = tracing_price_df(df, ref_df, mapping_size, atr_period, beta, gamma)?;
    let len = df.height();
    Ok(TracingPriceResult {
        buy_price: PriceInOut {
            r#in: df["buy_price"].f64()?.to_vec()[len - 1].context("buy_price is empty")?,
            out: df["buy_exit"].f64()?.to_vec()[len - 1].context("buy_exit is empty")?,
        },
        sell_price: PriceInOut {
            r#in: df["sell_price"].f64()?.to_vec()[len - 1].context("sell_price is empty")?,
            out: df["sell_exit"].f64()?.to_vec()[len - 1].context("sell_exit is empty")?,
        },
        last_close: df["close"].f64()?.to_vec()[len - 1].context("close is empty")?,
    })
}

/// 使用可能な注文量を計算する
pub fn next_open_amount(status: &'static OnceCell<RwLock<StatusRepository>>, pos: &'static OnceCell<RwLock<[TracingMMPosition; 2]>>, max_side_positions: i64, symbol: &Symbol, side: Side, price: FloatExp) -> Option<FloatExp> {
    let status = status.read()[symbol].clone();
    let quote_for_order = status["available_quote"].as_f64()?.min(status["liquidity_limited_base"].as_f64()? * max_side_positions as f64 * price.to_f64());
    let order_amount = quote_for_order / max_side_positions as f64 / price.to_f64();

    let init_notional = pos.read()[side as usize].init_notional.to_f64();
    if init_notional + order_amount * price.to_f64() < quote_for_order {
        Some(FloatExp::from_f64(order_amount, symbol.amount_precision()))
    } else {
        info!("next_open_amount not enough quote. side: {:?}, quote_for_order: {}, init_notional: {}, order_amount: {}", side, quote_for_order, init_notional, order_amount);
        None
    }
}

#[test]
fn test_tracing_price() {
    // https://stackoverflow.com/questions/70830241/rust-polars-how-to-show-all-columns
    std::env::set_var("POLARS_FMT_MAX_COLS", "20");
    use polars::prelude::*;
    let mut file = std::fs::File::open("test/mm_atr_polars.parquet").unwrap();
    let pq = ParquetReader::new(&mut file).finish().unwrap();
    println!("{:?}", pq);
    let df = pq.clone().lazy().select(vec![
        col("opentime"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
    ]).collect().unwrap();
    let ref_df = pq.clone().lazy().select(vec![
        col("opentime"),
        col("cl_ref").alias("close")
    ]).collect().unwrap();
    // {'alpha': 16, 'beta_in': 1.3861126614518735, 'gamma_in': 1.883522715736597, 'losscut_rate': 0.09864901642842279}
    let mapping_size = 100;
    let atr_period = 16;
    let beta = PriceInOut::new(1.3861126614518735, 1.3861126614518735);
    let gamma = PriceInOut::new(1.883522715736597, 1.883522715736597);
    let ret = tracing_price_df(df.clone(), ref_df.clone(), mapping_size, atr_period, &beta, &gamma).unwrap();
    println!("{:?}", ret);
    // f64の値を取り出すと一致していることがわかる
    let prices = tracing_price(df, ref_df, mapping_size, atr_period,& beta, &gamma).unwrap();
    println!("{:?}", prices);
}