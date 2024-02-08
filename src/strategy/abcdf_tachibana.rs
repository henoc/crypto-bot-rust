use std::{time::Duration as StdDuration, fs::File, vec, iter::once};

use log::info;
use labo::{export::{polars::{frame::{DataFrame, UniqueKeepStrategy}, series::Series, prelude::{NamedFrom, DataFrameJoinOps}, lazy::{frame::IntoLazy, dsl::{col, lit, UnionArgs}, prelude::concat}, datatypes::{DataType, TimeUnit}, io::{parquet::{ParquetWriter, ParquetReader}, SerReader}}, anyhow::{self, Context}}, abcdf::workflow::predict_process};
use tokio::{select, spawn, time::sleep};

use crate::{config::AbcdfConfig, client::{tachibana::{TachibanaClient, PriceHistoryRequest, StockMarket, SystemDataRequest, SystemDataCommand, PriceRequest, PriceType, CodeResponse, BusinessDateResponseItem}, credentials::CREDENTIALS}, utils::{time::{sleep_until_next, ScheduleExpr, JST, today_jst}, strategy_utils::CaptureResult, dataframe::DataFrameExt}, symbol::{Symbol, Currency}, global_vars::{debug_is_some, debug_is_some_any}};


pub async fn start_abcdf(config: &'static AbcdfConfig) {
    if debug_is_some("create_price_history") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await.unwrap();
        create_price_history(&client, config).await.unwrap();
        return;
    } else if debug_is_some("print_price_history") {
        let df = ParquetReader::new(File::open(price_hisotry_file_path(config.symbol)).unwrap()).finish().unwrap();
        info!("{}", df);
        return;
    } else if debug_is_some("modify_price_history") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await.unwrap();
        modify_price_history(&client, config).await.unwrap();
        return;
    } else if debug_is_some("predict_next") {
        let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
        client.login().await.unwrap();
        predict_next(&client, config).await.unwrap();
        return;
    } else if debug_is_some_any() {
        panic!("Unknown debug option");
    }

    if !tokio::fs::try_exists(&config.model_path).await.unwrap() {
        panic!("Model file not found: {}", &config.model_path);
    }

    let symbol = config.symbol.clone();
    select! {
        _ = spawn(async move {
            let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
            loop {
                async {
                    sleep_until_next(ScheduleExpr::daily(21, 3, JST())).await;
                    client.login().await?;
                    create_price_history(&client, config).await?;
                    Ok(())
                }.await.capture_result(symbol).await.unwrap();
            }
        }) => {},
        _ = spawn(async move {
            let client = TachibanaClient::new(CREDENTIALS.tachibana.clone());
            loop {
                async {
                    sleep_until_next(ScheduleExpr::daily(14, 59, JST())).await;
                    client.login().await?;
                    predict_next(&client, config).await?;
                    Ok(())
                }.await.capture_result(symbol).await.unwrap();
            }
        }) => {},
    }
}

const SAVE_HISTORY_LEN: usize = 100;

fn price_hisotry_file_path(symbol: Symbol) -> String {
    format!("/var/opt/{}.parquet", symbol.to_file_form())
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
            ret = ret.outer_join(&df, ["opentime"], ["opentime"])?;
        }
    }
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
    let mut df = read_price_history_and_add_last_price(client, config).await?.tail(Some(SAVE_HISTORY_LEN));
    let mut file = File::create(price_hisotry_file_path(config.symbol))?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

/// 営業時間中に呼ばれる。次営業日の終値を予測する
async fn predict_next(client: &TachibanaClient, config: &AbcdfConfig) -> anyhow::Result<()> {
    let business_date = get_business_date(client).await?;
    if !is_business_day(&business_date) {
        return Ok(());
    }

    let df = read_price_history_and_add_last_price(client, config).await?;
    // log returnを計算
    let mut df = df.lazy().select(&[
        col("opentime"),
        col("*").exclude(&["opentime"]).pct_change(lit(1)).log1p(),
    ]).collect()?;
    // predictで結果を得るために、次営業日前日までのカラムを追加
    let predicable_next_day = business_date.s_yoku_eigyou_day_1.pred_opt().unwrap();
    let mut d = today_jst().date_naive().succ_opt().unwrap();
    while d <= predicable_next_day {
        let row = Series::new("opentime", vec![d]).cast(&DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap().into_frame();
        df = concat(&[df.lazy(), row.lazy()], UnionArgs::default())?.collect()?;
        d = d.succ_opt().unwrap();
    }
    // predictして、次営業日前日のpred値を取得
    //   今日が金曜日のとき、日曜日に月曜日の予測値が入るため
    let pred = predict_process(df, config.model_path.as_str())?
        .at::<f64>(&config.symbol.base.to_string(), col("opentime").eq(lit(predicable_next_day)))?;
    info!("Predicted next day's value: {}. Next day: {}", pred, business_date.s_yoku_eigyou_day_1);
    Ok(())
}

async fn read_price_history_and_add_last_price(client: &TachibanaClient, config: &AbcdfConfig) -> anyhow::Result<DataFrame> {
    let mut df = ParquetReader::new(File::open(price_hisotry_file_path(config.symbol))?).finish()?;
    let res = client.send(PriceRequest {
        s_target_issue_code: config.ref_symbols.clone(),
        s_target_column: vec![PriceType::LastPrice],
    }).await?;
    
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
