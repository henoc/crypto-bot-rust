use std::{time::Duration as StdDuration, fs::File, vec, iter::once, sync::OnceLock};

use log::info;
use anyhow::{self, Context};
use labo::{abcdf::workflow::predict_process, export::{chrono::{Datelike, Duration}, polars::{datatypes::{DataType, TimeUnit}, frame::{DataFrame, UniqueKeepStrategy}, io::{parquet::{ParquetReader, ParquetWriter}, SerReader}, lazy::{dsl::{col, lit, UnionArgs}, frame::IntoLazy, prelude::concat}, prelude::{DataFrameJoinOps, JoinArgs, JoinType, NamedFrom}, series::Series}, serde_json::json}, lightgbm::common::load_lib_lightgbm};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::time::sleep;

use crate::{client::{credentials::CREDENTIALS, tachibana::{BusinessDateResponseItem, CodeResponse, MarginBalanceRequest, MarginPositionRequest, OrderPrice, OrderRequest, OrderTime, PriceHistoryRequest, PriceRequest, PriceType, StockMarket, SystemDataCommand, SystemDataRequest, TachibanaClient, TradingType}}, config::AbcdfConfig, data_structure::float_exp::FloatExp, global_vars::{debug_is_some, debug_is_some_except}, order_types::Side, symbol::Symbol, utils::{dataframe::DataFrameExt, status_repository::StatusRepository, time::today_jst, useful_traits::StaticVarExt}};

static LAST_PRICE: OnceLock<f64> = OnceLock::new();
static POS: OnceLock<RwLock<Vec<Position>>> = OnceLock::new();
static STATUS: Lazy<RwLock<StatusRepository>> = Lazy::new(|| RwLock::new(StatusRepository::new("abcdf")));

const LEVERAGE: i64 = 3;

#[derive(Debug, Clone)]
struct Position {
    amount: FloatExp,
}

/// cronで呼ばれる
pub async fn action_abcdf(config: &'static AbcdfConfig, cmd: &str) -> anyhow::Result<()> {
    load_lib_lightgbm();
    POS.set(RwLock::new(vec![Position { amount: FloatExp::new(0, config.symbol.amount_precision()) }; 2])).unwrap();
    STATUS.write().init(&config.symbol, Some(Duration::days(14)))?;

    let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
    
    if debug_is_some("create_price_history") {
        client.login().await?;
        create_price_history(&client, config).await?;
        return Ok(());
    }
    if debug_is_some("print_price_history") {
        let df = ParquetReader::new(File::open(price_hisotry_file_path(config.symbol))?).finish()?;
        info!("{}", df);
        return Ok(());
    }
    if debug_is_some("modify_price_history") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await?;
        modify_price_history(&client, config).await?;
        return Ok(());
    }
    if debug_is_some("predict_next") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await?;
        let business_date = get_business_date(&client).await?;
        if !is_business_day(&business_date) {
            return Ok(());
        }
        predict_next(&client, config, &business_date).await?;
        return Ok(());
    }
    if debug_is_some("update_position") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await?;
        update_position(&client, config).await?;
        info!("{:?}", POS.read()?);
        return Ok(());
    }
    if debug_is_some_except(&["random_predict"]) {
        anyhow::bail!("Unknown debug option");
    }

    if !tokio::fs::try_exists(&config.model_path).await? {
        anyhow::bail!("Model file not found: {}", &config.model_path);
    }

    match cmd {
        "update_price_history" => {
            client.login().await?;
            create_price_history(&client, config).await?;
        },
        "update_order" => {
            client.login().await?;
            let business_date = get_business_date(&client).await?;
            if !is_business_day(&business_date) {
                return Ok(());
            }
            update_position(&client, config).await?;
            let pred = if debug_is_some("random_predict") {
                let res = client.send(PriceRequest {
                    s_target_issue_code: config.ref_symbols.clone(),
                    s_target_column: vec![PriceType::LastPrice],
                }).await?;
                let last_price = res.a_clm_mfds_market_price.get(&CodeResponse::Defined(config.symbol.base)).with_context(|| format!("Not found {}", config.symbol.base))?.last_price.with_context(|| format!("Last price is None for {}", config.symbol.base))?;
                LAST_PRICE.set(last_price).unwrap();

                let pred = (today_jst().date_naive().day() % 2) as f64 * - 0.5;
                info!("Predicted (random) next day's value: {}", pred);
                pred
            } else {
                predict_next(&client, config, &business_date).await?
            };
            send_new_orders(&client, config, if pred > 0. { Side::Buy } else { Side::Sell }).await?;
        },
        _ => {
            anyhow::bail!("Unknown command: {}", cmd);
        }
    };
    Ok(())
}

const SAVE_HISTORY_LEN: usize = 100;

fn price_hisotry_file_path(symbol: Symbol) -> String {
    format!("{}.parquet", symbol.to_file_form())
}

fn is_business_day(business_date: &BusinessDateResponseItem) -> bool {
    business_date.s_the_day == today_jst().date_naive()
}

async fn get_business_date(client: &TachibanaClient) -> anyhow::Result<BusinessDateResponseItem> {
    let res = client.send(SystemDataRequest::new(vec![SystemDataCommand::BusinessDate])).await?;
    let business_date = res.clm_date_zyouhou.context("Business date not found")?.get(&crate::client::tachibana::DayFlag::Today).context("Today not found")?.clone();
    Ok(business_date)
}

/// 終値の履歴を作成する
async fn create_price_history(client: &TachibanaClient, config: &AbcdfConfig) -> anyhow::Result<()> {
    if tokio::fs::try_exists(price_hisotry_file_path(config.symbol)).await? {
        modify_price_history(client, config).await?;
        return Ok(());
    }

    let mut ret = DataFrame::empty();
    for &c in &config.ref_symbols {
        sleep(StdDuration::from_secs(1)).await;
        info!("Getting price history for {}.", c);
        let res = client.send(PriceHistoryRequest {
            s_issue_code: c,
            s_sizyou_c: StockMarket::Tsc,
        }).await?;
        let mut opentime = vec![];
        let mut close = vec![];
        let len = res.a_clm_mfds_market_price_history.len();
        for item in res.a_clm_mfds_market_price_history.into_iter().skip(len - SAVE_HISTORY_LEN) {
            opentime.push(item.s_date);
            close.push(item.close_adj);
        }
        let df = DataFrame::new(vec![
            Series::new("opentime", opentime),
            Series::new(&c.to_string(), close),
        ])?.lazy().select(&[
            col("opentime").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            col(&c.to_string()),
        ]).collect()?;
        if ret.is_empty() {
            ret = df;
        } else {
            ret = ret.join(&df, ["opentime"], ["opentime"], JoinArgs::new(JoinType::Outer { coalesce: true }))?;
        }
    }
    // 当日終値を追加
    let mut ret = add_last_price_to_price_history(client, config, ret).await?.tail(Some(SAVE_HISTORY_LEN));
    let mut file = File::create(price_hisotry_file_path(config.symbol))?;
    ParquetWriter::new(&mut file).finish(&mut ret)?;
    Ok(())
}

/// 営業日の営業時間後に呼ばれる
async fn modify_price_history(client: &TachibanaClient, config: &AbcdfConfig) -> anyhow::Result<()> {
    let business_date = get_business_date(client).await?;
    if !is_business_day(&business_date) {
        info!("Skip modifying price history.");
        return Ok(());
    }
    info!("Modify price history.");
    let df = read_price_history(config.symbol)?;
    let mut df = add_last_price_to_price_history(client, config, df).await?.tail(Some(SAVE_HISTORY_LEN));
    let mut file = File::create(price_hisotry_file_path(config.symbol))?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

/// 営業時間中に呼ばれる。次営業日の終値を予測する
async fn predict_next(client: &TachibanaClient, config: &AbcdfConfig, business_date: &BusinessDateResponseItem) -> anyhow::Result<f64> {
    let df = read_price_history(config.symbol)?;
    let df = add_last_price_to_price_history(client, config, df).await?;
    // log returnを計算
    let mut df = df.lazy().select(&[
        col("opentime"),
        col("*").exclude(["opentime"]).pct_change(lit(1)).log1p(),
    ]).collect()?;
    // predictで結果を得るために、次営業日前日までのカラムを追加
    let predicable_next_day = business_date.s_yoku_eigyou_day_1.pred_opt().unwrap();
    let mut d = today_jst().date_naive().succ_opt().unwrap();
    while d <= predicable_next_day {
        let row = once(
            Series::new("opentime", vec![d]).cast(&DataType::Datetime(TimeUnit::Milliseconds, None)).context("Failed to cast")
        ).chain(
            df.get_column_names().iter().filter(|&c| *c != "opentime").map(|c| Ok(Series::new(c, vec![Option::<f64>::None])))
        ).collect::<anyhow::Result<DataFrame>>()?;
        df = concat(&[df.lazy(), row.lazy()], UnionArgs::default())?.collect()?;
        d = d.succ_opt().unwrap();
    }
    // predictして、次営業日前日のpred値を取得
    //   今日が金曜日のとき、日曜日に月曜日の予測値が入るため
    let pred = predict_process(df, config.model_path.as_str(), true).await?
        .at::<f64>(&config.symbol.base.to_string(), col("opentime").eq(lit(predicable_next_day)))?;
    info!("Predicted next day's value: {}. Next day: {}", pred, business_date.s_yoku_eigyou_day_1);
    Ok(pred)
}

fn read_price_history(symbol: Symbol) -> anyhow::Result<DataFrame> {
    Ok(ParquetReader::new(File::open(price_hisotry_file_path(symbol))?).finish()?)
}

/// 営業日に呼ばれる
async fn add_last_price_to_price_history(client: &TachibanaClient, config: &AbcdfConfig, mut df: DataFrame) -> anyhow::Result<DataFrame> {
    let res = client.send(PriceRequest {
        s_target_issue_code: config.ref_symbols.clone(),
        s_target_column: vec![PriceType::LastPrice],
    }).await?;

    let last_price = res.a_clm_mfds_market_price.get(&CodeResponse::Defined(config.symbol.base)).with_context(|| format!("Not found {}", config.symbol.base))?.last_price.with_context(|| format!("Last price is None for {}", config.symbol.base))?;
    LAST_PRICE.set(last_price).unwrap();
    
    // 現在値を今日の終値としてdfに追加する
    let next_row = once(
        Series::new("opentime", vec![today_jst().date_naive()]).cast(&DataType::Datetime(TimeUnit::Milliseconds, None)).context("Failed to cast")
    ).chain(
        config.ref_symbols.iter().map(|c|
            res.a_clm_mfds_market_price.get(&CodeResponse::Defined(*c)).with_context(|| format!("Not found {}", c)).map(|v| Series::new(&c.to_string(), vec![v.last_price]))
        )
    ).collect::<anyhow::Result<DataFrame>>()?;
    df = concat(&[df.lazy(), next_row.lazy()], UnionArgs::default())?.unique_stable(Some(vec!["opentime".to_owned()]), UniqueKeepStrategy::Last).collect()?;
    Ok(df)
}

async fn update_position(client: &TachibanaClient, config: &AbcdfConfig) -> anyhow::Result<()> {
    let res = client.send(MarginPositionRequest {
        s_issue_code: crate::client::tachibana::MarginPositionRequestBase::Currency(config.symbol.base)
    }).await?;
    let mut poss = vec![Position { amount: FloatExp::new(0, config.symbol.amount_precision()) }; 2];
    for pos in res.a_shinyou_tategyoku_list {
        poss[pos.s_order_baibai_kubun as usize].amount += FloatExp::from_f64(pos.s_tategyoku_suryou as f64, config.symbol.amount_precision());
    }
    *POS.write()? = poss;
    Ok(())
}

async fn send_new_orders(client: &TachibanaClient, config: &AbcdfConfig, pred_side: Side) -> anyhow::Result<()> {
    let poss = POS.read()?.clone();
    let mut order_result = anyhow::Result::<()>::Ok(());
    if poss[pred_side.inv() as usize].amount.value > 0 {
        let res = client.send(OrderRequest::new(
            config.symbol.base,
            pred_side.into(),
            OrderTime::Closing,
            OrderPrice::Market,
            poss[pred_side.inv() as usize].amount,
            TradingType::CloseSystemMargin,
        )).await;
        if let Ok(res) = &res {
            info!("Close order: {:?}", res);
        }
        order_result = order_result.and(res.map(|_| ()));
    }
    if poss[pred_side as usize].amount.is_zero() {
        let res = client.send(MarginBalanceRequest { 
            s_hituke_index: 0
        }).await?;
        let deposit = STATUS.read().get(&config.symbol)["deposit"].as_i64().unwrap_or_default().max(res.s_azukari_kin);
        STATUS.write().update(config.symbol, json!({"deposit": deposit}))?;
        let all = deposit * LEVERAGE;
        let unit_margin = all as f64 * 0.45;
        let amount = FloatExp::from_f64_floor(unit_margin / *LAST_PRICE.get().with_context(|| "not initialized OnceLock")?, config.symbol.amount_precision());

        let res = client.send(OrderRequest::new(
            config.symbol.base,
            pred_side.into(),
            OrderTime::Closing,
            OrderPrice::Market,
            amount,
            TradingType::OpenSystemMargin,
        )).await;
        if let Ok(res) = &res {
            info!("Open order. all_margin: {}, order_amount: {}, res: {:?}", all, amount, res);
        }
        order_result = order_result.and(res.map(|_| ()));
    }
    order_result
}
