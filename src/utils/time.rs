use chrono::{DateTime, Utc, TimeZone};

pub enum ScheduleExpr {
    EveryDay {
        q: i64, r: i64,
        hour: i64, minute: i64, second: i64,
    },
    EveryHour {
        q: i64, r: i64,
        minute: i64,
        second: i64,
    },
    EveryMinute {
        q: i64, r: i64,
        second: i64,
    },
    EverySecond {
        q: i64, r: i64,
    }
}

pub async fn sleep_until_next(schedule: ScheduleExpr) {
    let curr_ms = chrono::Utc::now().timestamp_millis();
    let next_ms = next_sleep_duration_ms(curr_ms, schedule);
    tokio::time::sleep(tokio::time::Duration::from_millis(next_ms as u64)).await;
}

pub fn next_sleep_duration_ms(curr_ms: i64, schedule: ScheduleExpr) -> i64 {
    let curr_sec = curr_ms / 1000;
    let mut next_sec = curr_sec;
    match schedule {
        ScheduleExpr::EveryDay { q, r, hour, minute, second } => {
            debug_assert!(r < q);
            let day_unit_sec = 24 * 60 * 60;
            let q_sec = q * day_unit_sec;
            let r_sec = r * day_unit_sec;
            next_sec = next_sec / day_unit_sec * day_unit_sec;
            next_sec = (next_sec - r_sec) / q_sec * q_sec + r_sec;
            next_sec += hour * 60 * 60 + minute * 60 + second;
            if next_sec <= curr_sec {
                next_sec += q_sec;
            }
        },
        ScheduleExpr::EveryHour { q, r, minute, second } => {
            debug_assert!(0 < q && q < 24);
            debug_assert!(r < q);
            let hour_unit_sec = 60 * 60;
            let q_sec = q * hour_unit_sec;
            let r_sec = r * hour_unit_sec;
            next_sec = next_sec / hour_unit_sec * hour_unit_sec;
            next_sec = (next_sec - r_sec) / q_sec * q_sec + r_sec;
            next_sec += minute * 60 + second;
            if next_sec <= curr_sec {
                next_sec += q_sec;
            }
        },
        ScheduleExpr::EveryMinute { q, r, second } => {
            debug_assert!(0 < q && q < 60);
            debug_assert!(r < q);
            let minute_unit_sec = 60;
            let q_sec = q * minute_unit_sec;
            let r_sec = r * minute_unit_sec;
            next_sec = next_sec / minute_unit_sec * minute_unit_sec;
            next_sec = (next_sec - r_sec) / q_sec * q_sec + r_sec;
            next_sec += second;
            if next_sec <= curr_sec {
                next_sec += q_sec;
            }
        },
        ScheduleExpr::EverySecond { q, r } => {
            debug_assert!(0 < q && q < 60);
            debug_assert!(r < q);
            let q_sec = q;
            let r_sec = r;
            next_sec = next_sec / 1 * 1;
            next_sec = (next_sec - r_sec) / q_sec * q_sec + r_sec;
            if next_sec <= curr_sec {
                next_sec += q_sec;
            }
        }
    };
    next_sec * 1000 - curr_ms
}

pub enum KLinesTimeUnit {
    Second,
    MilliSecond,
    MicroSecond,
    NanoSecond,
}

impl KLinesTimeUnit {
    pub fn to_ms(&self, time: i64) -> i64 {
        match self {
            KLinesTimeUnit::Second => time * 1000,
            KLinesTimeUnit::MilliSecond => time,
            KLinesTimeUnit::MicroSecond => time / 1000,
            KLinesTimeUnit::NanoSecond => time / 1000 / 1000,
        }
    }

    pub fn to_ns(&self, time: i64) -> i64 {
        match self {
            KLinesTimeUnit::Second => time * 1000 * 1000 * 1000,
            KLinesTimeUnit::MilliSecond => time * 1000 * 1000,
            KLinesTimeUnit::MicroSecond => time * 1000,
            KLinesTimeUnit::NanoSecond => time,
        }
    }
}

pub fn datetime_utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single().unwrap()
}

pub fn datetime_utc_from_timestamp(timestamp: i64, time_unit: KLinesTimeUnit) -> DateTime<Utc> {
    Utc.timestamp_nanos(time_unit.to_ns(timestamp))
}

#[test]
fn test_next_sleep_duration_ms() {
    let curr = datetime_utc(2023, 1, 1, 0, 0, 15);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::EverySecond {
        q: 5, r: 0
    }), 5000);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::EverySecond {
        q: 5, r: 1
    }), 1000);

    let curr = datetime_utc(2023, 1, 1, 0, 0, 56);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::EverySecond {
        q: 5, r: 0
    }), 4000);

    let curr = datetime_utc(2023, 1, 1, 0, 15, 10);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::EveryMinute {
        q: 5, r: 0,
        second: 0
    }), (5*60-10)*1000);
    assert_eq!(next_sleep_duration_ms(curr.timestamp_millis(), ScheduleExpr::EveryMinute {
        q: 5, r: 1,
        second: 0
    }), (1*60-10)*1000);
}