use chrono::{DateTime, Utc, Duration};
use polars::{prelude::{DataFrame, NamedFrom, ChunkedArray, TimeUnit, IntoLazy, TakeRandom}, series::{Series, IntoSeries}, time::{PolarsUpsample}, lazy::dsl::{col, lit}};
use serde::{Serialize, Serializer};
use serde_json::{Value, json};

use crate::{utils::{dataframe::chrono_dt_to_series_ms, time::{KLinesTimeUnit, datetime_utc_from_timestamp, UnixTimeMs, format_time_naive}}, symbol::Symbol, order_types::Side};

pub struct KLines {
    pub df: DataFrame
}

impl KLines {
    pub fn empty() -> KLines {
        KLines {
            df: DataFrame::empty()
        }
    }

    pub fn new(ohlcvs: &Vec<Vec<f64>>, time_unit: KLinesTimeUnit) -> anyhow::Result<KLines> {
        let mut opentime = vec![];
        let mut open = vec![];
        let mut high = vec![];
        let mut low = vec![];
        let mut close = vec![];
        let mut volume = vec![];

        for ohlcv in ohlcvs {
            opentime.push(time_unit.to_ms(ohlcv[0] as i64));
            open.push(ohlcv[1]);
            high.push(ohlcv[2]);
            low.push(ohlcv[3]);
            close.push(ohlcv[4]);
            volume.push(ohlcv[5]);
        }

        Ok(KLines { df: DataFrame::new(vec![
                    ChunkedArray::from_vec("opentime", opentime).into_datetime(TimeUnit::Milliseconds, Some("UTC".to_string())).into_series(),
                    Series::new("open", open),
                    Series::new("high", high),
                    Series::new("low", low),
                    Series::new("close", close),
                    Series::new("volume", volume),
                ])?
        })
    }

    pub fn new_options(ohlcvs: &Vec<Vec<Option<f64>>>, time_unit: KLinesTimeUnit) -> anyhow::Result<KLines> {
        let mut opentime = vec![];
        let mut open = vec![];
        let mut high = vec![];
        let mut low = vec![];
        let mut close = vec![];
        let mut volume = vec![];

        for ohlcv in ohlcvs {
            opentime.push(time_unit.to_ms(ohlcv[0].unwrap() as i64));
            open.push(ohlcv[1]);
            high.push(ohlcv[2]);
            low.push(ohlcv[3]);
            close.push(ohlcv[4]);
            volume.push(ohlcv[5]);
        }

        Ok(KLines { df: DataFrame::new(vec![
                    ChunkedArray::from_vec("opentime", opentime).into_datetime(TimeUnit::Milliseconds, Some("UTC".to_string())).into_series(),
                    Series::new("open", open),
                    Series::new("high", high),
                    Series::new("low", low),
                    Series::new("close", close),
                    Series::new("volume", volume),
                ])?
        })
    }

    pub fn sort(&mut self) -> anyhow::Result<()> {
        self.df = self.df.sort(vec!["opentime"], false)?;
        Ok(())
    }

    /// indexを補完し、Noneの行を埋める
    pub fn reindex(&mut self, until: DateTime<Utc>, timeframe: Duration) -> anyhow::Result<()> {
        let last_opentime = until - timeframe;
        let last_df = DataFrame::new(vec![
            chrono_dt_to_series_ms("opentime", vec![last_opentime]),
        ])?;
        let mut df = DataFrame::empty();
        std::mem::swap(&mut df, &mut self.df);
        df = df.lazy().join_builder().on(vec![col("opentime")]).with(last_df.lazy()).how(polars::prelude::JoinType::Outer).finish().collect()?;
        let duration = polars::prelude::Duration::parse(format!("{}s", timeframe.num_seconds()).as_str());
        df = df.upsample_stable::<Vec<&str>>(vec![], "opentime", duration, polars::prelude::Duration::new(0))?;
        self.df = df.lazy().with_columns(vec![
            col("close").forward_fill(None),
            col("volume").fill_null(lit(0.)),
        ]).with_columns(vec![
            col("open").fill_null(col("close")),
            col("high").fill_null(col("close")),
            col("low").fill_null(col("close")),
        ]).drop_nulls(None).collect()?;
        Ok(())
    }

    /// [since, until)の範囲のデータを取得する
    pub fn filter(&self, since: Option<DateTime<Utc>>, until: Option<DateTime<Utc>>) -> anyhow::Result<KLines> {
        let lazy_df = self.df.clone().lazy();
        let lazy_df = match since {
            Some(since) => lazy_df.filter(col("opentime").gt_eq(lit(since.naive_utc()))),
            None => lazy_df
        };
        let lazy_df = match until {
            Some(until) => lazy_df.filter(col("opentime").lt(lit(until.naive_utc()))),
            None => lazy_df
        };
        Ok(KLines { df: lazy_df.collect()? })
    }

    pub fn to_json(&self) -> anyhow::Result<Value> {
        let mut ret = vec![];
        let opentime = self.df.column("opentime")?.datetime()?.as_datetime_iter();
        let open = self.df.column("open")?.f64()?;
        let high = self.df.column("high")?.f64()?;
        let low = self.df.column("low")?.f64()?;
        let close = self.df.column("close")?.f64()?;
        let volume = self.df.column("volume")?.f64()?;
        for (i, t) in opentime.enumerate() {
            ret.push(json!({
                "opentime": t.map(|t| format_time_naive(t)),
                "open": open.get(i),
                "high": high.get(i),
                "low": low.get(i),
                "close": close.get(i),
                "volume": volume.get(i),
            }));
        }
        Ok(Value::Array(ret))
    }
}

#[derive(Clone, Debug)]
pub struct TradeRecord {
    pub symbol: Symbol,
    pub timestamp: UnixTimeMs,
    pub price: f64,
    pub amount: f64,
    pub side: Side
}

impl TradeRecord {
    pub fn new(symbol: Symbol, timestamp: UnixTimeMs, price: f64, amount: f64, side: Side) -> TradeRecord {
        TradeRecord {
            symbol,
            timestamp,
            price,
            amount,
            side
        }
    }

    pub fn mpack(self)->MpackTradeRecord {
        MpackTradeRecord(self)
    }
}

#[derive(Clone, Debug)]
pub struct MpackTradeRecord(pub TradeRecord);

impl Serialize for MpackTradeRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let tpl = (self.0.price, self.0.amount, self.0.timestamp, self.0.side == Side::Sell);
        tpl.serialize(serializer)
    }
}

pub fn trades_time_fn(mpack_trade_record: &MpackTradeRecord) -> Option<DateTime<Utc>> {
    let dt = datetime_utc_from_timestamp(mpack_trade_record.0.timestamp, KLinesTimeUnit::MilliSecond);
    Some(dt)
}

#[test]
fn test_klines() {
    let mut ohlcvs: Vec<Vec<Option<f64>>> = vec![];
    ohlcvs.push(vec![
        Some(1686121920.),
        Some(3743331.0),
        Some(3743906.0),
        Some(3742405.0),
        Some(3742405.0),
        Some(0.30817043)
    ]);
    ohlcvs.push(vec![
        Some(1686121980.),
        None,
        None,
        None,
        None,
        Some(0.)
    ]);
    ohlcvs.push(vec![
            Some(1686122040.),
            Some(3740181.0),
            Some(3741946.0),
            Some(3738559.0),
            Some(3740184.0),
            Some(1.49343964)
    ]);
    let mut klines = KLines::new_options(&ohlcvs, KLinesTimeUnit::Second).unwrap();
    println!("{:?}", klines.df);
    let until = datetime_utc_from_timestamp(1686122100, KLinesTimeUnit::Second);
    klines.reindex(until, Duration::seconds(60)).unwrap();
    println!("{:?}", klines.df);

    println!("{:?}", klines.filter(Some(datetime_utc_from_timestamp(1686121980,KLinesTimeUnit::Second)), Some(datetime_utc_from_timestamp(1686122100,KLinesTimeUnit::Second))).unwrap().df);
}