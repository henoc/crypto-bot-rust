use chrono::{DateTime, Utc};
use polars::{series::{Series, IntoSeries}, prelude::{ChunkedArray, TimeUnit}};

pub fn chrono_dt_to_series_ms(name: &str, vecs: Vec<DateTime<Utc>>) -> Series {
    let unixtime = vecs.into_iter().map(|dt| dt.timestamp_millis()).collect::<Vec<_>>();
    ChunkedArray::from_vec(name, unixtime)
        .into_datetime(TimeUnit::Milliseconds, Some("UTC".to_string()))
        .into_series()
}
