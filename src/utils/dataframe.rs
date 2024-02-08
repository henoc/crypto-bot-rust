use labo::export::chrono::{DateTime, Utc};
use labo::export::polars::{series::{Series, IntoSeries}, prelude::{ChunkedArray, TimeUnit}, frame::DataFrame, error::PolarsResult, lazy::{frame::IntoLazy, dsl::Expr}, datatypes::AnyValue};
use labo::export::polars::export::num::NumCast;

pub fn chrono_dt_to_series_ms(name: &str, vecs: Vec<DateTime<Utc>>) -> Series {
    let unixtime = vecs.into_iter().map(|dt| dt.timestamp_millis()).collect::<Vec<_>>();
    ChunkedArray::from_vec(name, unixtime)
        .into_datetime(TimeUnit::Milliseconds, Some("UTC".to_string()))
        .into_series()
}

#[easy_ext::ext(DataFrameExt)]
pub impl DataFrame {
    fn at<T: NumCast>(&self, col_name: &str, row_pred: Expr) -> PolarsResult<T> {
        let row = self.clone().lazy().filter(row_pred).collect()?;
        let s = row.column(col_name)?;
        assert!(s.len() == 1, "row_pred must be unique, but found len {}.", s.len());
        Ok(s.get(0)?.try_extract()?)
    }

    fn skip(&self, row_size: usize) -> PolarsResult<DataFrame> {
        self.filter(&(0..self.height()).map(|i| i >= row_size).collect())
    }
}

#[test]
fn test_at() {
    use labo::export::polars::prelude::NamedFrom;
    use labo::export::polars::lazy::dsl::{col, lit};
    let df = DataFrame::new(vec![
        Series::new("opentime", vec![0, 1, 2, 3, 4]).cast(&labo::export::polars::datatypes::DataType::UInt32).unwrap(),
        Series::new("close", vec![1, 2, 3, 4, 5]).cast(&labo::export::polars::datatypes::DataType::UInt32).unwrap(),
    ]).unwrap();
    let ret = df.at::<u32>( "close", col("opentime").eq(lit(2u32))).unwrap();
    assert_eq!(ret, 3u32);
}

#[test]
fn test_skip() {
    use labo::export::polars::prelude::NamedFrom;
    let df = DataFrame::new(vec![
        Series::new("opentime", vec![0, 1, 2, 3, 4]),
    ]).unwrap();
    let ret = df.skip(2).unwrap();
    assert_eq!(ret.column("opentime").unwrap(), &Series::new("opentime", vec![2, 3, 4]));
}
