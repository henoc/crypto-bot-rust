use chrono::{DateTime, Utc, TimeZone, NaiveDateTime, FixedOffset, Duration};
use serde::{Deserializer, Deserialize};

pub struct ScheduleExpr {
    q: Duration,
    r: Duration
}

impl ScheduleExpr {
    pub fn new(interval: Duration, rem: Duration) -> ScheduleExpr {
        ScheduleExpr {q: interval, r: rem}
    }

    pub fn new_ahead(interval: Duration, ahead: Duration) -> ScheduleExpr {
        if ahead >= interval {
            panic!("ahead must be less than interval");
        }
        ScheduleExpr {q: interval, r: interval - ahead}
    }
}

/// 呼び出し間隔がschedule以上のときにwarnを出す機能をつけたい
pub async fn sleep_until_next(schedule: ScheduleExpr) {
    let curr_ms = chrono::Utc::now().timestamp_millis();
    let next_ms = next_sleep_duration_ms(curr_ms, schedule);
    tokio::time::sleep(tokio::time::Duration::from_millis(next_ms as u64)).await;
}

pub fn next_sleep_duration_ms(curr_ms: i64, schedule: ScheduleExpr) -> i64 {
    let curr_sec = curr_ms / 1000;
    let mut next_sec = curr_sec;
    next_sec = next_sec / schedule.q.num_seconds() * schedule.q.num_seconds() + schedule.r.num_seconds();
    if next_sec <= curr_sec {
        next_sec += schedule.q.num_seconds();
    }
    next_sec * 1000 - curr_ms
}

pub enum UnixTimeUnit {
    Second,
    MilliSecond,
    MicroSecond,
    NanoSecond,
}

impl UnixTimeUnit {
    pub fn to_ms(&self, time: i64) -> i64 {
        match self {
            UnixTimeUnit::Second => time * 1000,
            UnixTimeUnit::MilliSecond => time,
            UnixTimeUnit::MicroSecond => time / 1000,
            UnixTimeUnit::NanoSecond => time / 1000 / 1000,
        }
    }

    pub fn to_ns(&self, time: i64) -> i64 {
        match self {
            UnixTimeUnit::Second => time * 1000 * 1000 * 1000,
            UnixTimeUnit::MilliSecond => time * 1000 * 1000,
            UnixTimeUnit::MicroSecond => time * 1000,
            UnixTimeUnit::NanoSecond => time,
        }
    }
}

pub fn datetime_naive(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> NaiveDateTime {
    datetime_utc(year, month, day, hour, minute, second).naive_utc()
}

pub fn datetime_utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single().unwrap()
}

pub fn datetime_utc_from_timestamp(timestamp: i64, time_unit: UnixTimeUnit) -> DateTime<Utc> {
    Utc.timestamp_nanos(time_unit.to_ns(timestamp))
}

/// "2023-06-30T00:03:00+09:00"
pub fn format_time_naive(time: NaiveDateTime) -> String {
    time.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
}

pub fn format_time_utc(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}

pub fn parse_format_time_naive(time: &str) -> anyhow::Result<NaiveDateTime> {
    Ok(parse_format_time_utc(time)?.naive_utc())
}

pub fn parse_format_time_utc(time: &str) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_str(time, "%Y-%m-%dT%H:%M:%S%:z")?.with_timezone(&Utc))
}

pub type UnixTimeMs = i64;

#[allow(non_snake_case)]
pub fn JST() -> FixedOffset {
    FixedOffset::east_opt(9 * 60 * 60).unwrap()
}

pub fn floor_time_sec(timestamp: DateTime<Utc>, timeframe: Duration, unit_delta:i64) -> i64 {
    let timeframe_sec = timeframe.num_seconds();
    let mut unix_sec = timestamp.timestamp();
    unix_sec = unix_sec / timeframe_sec * timeframe_sec;
    unix_sec += unit_delta * timeframe_sec;
    unix_sec
}

/// timeframeの最小単位はsecond
pub fn floor_time(timestamp: DateTime<Utc>, timeframe: Duration, unit_delta:i64)-> DateTime<Utc> {
    let unix_sec = floor_time_sec(timestamp, timeframe, unit_delta);
    datetime_utc_from_timestamp(unix_sec, UnixTimeUnit::Second)
}

pub fn now_floor_time(timeframe: Duration, unit_delta:i64)-> DateTime<Utc> {
    floor_time(Utc::now(), timeframe, unit_delta)
}

pub fn deserialize_rfc3339<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let datetime = DateTime::parse_from_rfc3339(&s)
            .map_err(serde::de::Error::custom)?
            .with_timezone(&Utc);
        Ok(datetime)
    }

#[test]
fn test_next_sleep_duration_ms() {
    let curr = datetime_utc(2023, 1, 1, 0, 0, 15);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr {
        q: Duration::seconds(5), r: Duration::seconds(0)
    }), 5000);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::new(
        Duration::seconds(5), Duration::seconds(1)
    )), 1000);

    let curr = datetime_utc(2023, 1, 1, 0, 0, 56);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::new(
        Duration::seconds(5), Duration::seconds(0)
    )), 4000);

    let curr = datetime_utc(2023, 1, 1, 0, 15, 10);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::new(
        Duration::minutes(5), Duration::seconds(0)
    )), (5*60-10)*1000);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::new(
        Duration::minutes(5), Duration::minutes(1)
    )), (1*60-10)*1000);
}

#[test]
fn test_format_time() {
    assert_eq!(format_time_naive(datetime_naive(2023, 1, 1, 0, 0, 5)), "2023-01-01T00:00:05+00:00".to_owned());
    assert_eq!(parse_format_time_naive("2023-01-01T00:00:05+00:00").unwrap(), datetime_naive(2023, 1, 1, 0, 0, 5));
}