use std::{ops::{Add, Sub, Mul, AddAssign, SubAssign, Div}, fmt::{Display, Debug}, str::FromStr};

use serde::Serialize;

#[derive(Clone, Copy, Eq, Hash)]
pub struct FloatExp {
    pub value: i64,
    pub exp: i32,
}

impl FloatExp {
    /// x = value * 10^exp
    pub const fn new(value: i64, exp: i32) -> Self {
        Self {
            value,
            exp,
        }
    }

    pub fn to_i64(&self) -> i64 {
        self.round(0).value
    }

    pub fn to_f64(&self) -> f64 {
        self.value as f64 * 10f64.powi(self.exp)
    }

    pub fn from_f64(raw: f64, exp: i32) -> Self {
        Self::new((raw * 10f64.powi(-exp)).round() as i64, exp)
    }

    pub fn from_f64_floor(raw: f64, exp: i32) -> Self {
        Self::new((raw * 10f64.powi(-exp)).floor() as i64, exp)
    }

    pub fn from_str(raw: String, exp: i32) -> Result<Self, std::num::ParseFloatError> {
        Ok(Self::new((raw.parse::<f64>()? * 10f64.powi(-exp)).round() as i64, exp))
    }

    /// Round to the specified number of decimal places.
    pub fn round(&self, exp: i32) -> Self {
        Self::new((self.value as f64 * 10f64.powi(- exp + self.exp)).round() as i64, exp)
    }

    /// Round down to the specified number of decimal places.
    pub fn floor(&self, exp: i32) -> Self {
        Self::new((self.value as f64 * 10f64.powi(- exp + self.exp)).floor() as i64, exp)
    }

    pub fn abs(&self) -> Self {
        Self::new(self.value.abs(), self.exp)
    }

    pub fn min_exp_sub(&self, rhs: Self) -> Self {
        let exp = self.exp.min(rhs.exp);
        Self::new(self.round(exp).value - rhs.round(exp).value, exp)
    }

    pub fn min_exp_add(&self, rhs: Self) -> Self {
        let exp = self.exp.min(rhs.exp);
        Self::new(self.round(exp).value + rhs.round(exp).value, exp)
    }

    /// Divide and round to the specified number of decimal places.
    pub fn div_round(&self, rhs: Self, new_exp: i32) -> Self {
        let min_exp = self.exp.min(rhs.exp);
        // f64での割り算なので答えが小さいとこのとき既に誤差が出るが、問題になったら修正する
        let raw = self.round(min_exp).value as f64 / rhs.round(min_exp).value as f64;   // この時点ではexpは0
        Self::from_f64(raw, new_exp)
    }

    /// Divide and round down to the specified number of decimal places.
    pub fn div_floor(&self, rhs: Self, new_exp: i32) -> Self {
        let min_exp = self.exp.min(rhs.exp);
        // f64での割り算なので答えが小さいとこのとき既に誤差が出るが、問題になったら修正する
        let raw = self.round(min_exp).value as f64 / rhs.round(min_exp).value as f64;   // この時点ではexpは0
        Self::from_f64_floor(raw, new_exp)
    }

    pub const fn is_zero(&self) -> bool {
        self.value == 0
    }
}

impl Display for FloatExp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{value:.*}", if self.exp<0 {(-self.exp) as usize} else {0}, value=self.to_f64())
    }
}

impl Debug for FloatExp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{value:.*}@exp={exp}", if self.exp<0 {(-self.exp) as usize} else {0}, value=self.to_f64(), exp=self.exp)
    }
}

// rust-decimalの実装を参考にした
// https://docs.rs/rust_decimal/latest/rust_decimal/serde/arbitrary_precision/index.html
impl Serialize for FloatExp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, {
            serde_json::Number::from_str(&self.to_string())
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl Add for FloatExp {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.exp, rhs.exp);
        Self::new(self.value + rhs.value, self.exp)
    }
}

impl Sub for FloatExp {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(self.exp, rhs.exp);
        Self::new(self.value - rhs.value, self.exp)
    }
}

impl Add<f64> for FloatExp {
    type Output = Self;

    fn add(self, rhs: f64) -> Self::Output {
        let rhs = FloatExp::from_f64(rhs, self.exp);
        Self::new(self.value + rhs.value, self.exp)
    }
}

impl Mul<f64> for FloatExp {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self::Output {
        Self::new((self.value as f64 * rhs).round() as i64, self.exp)
    }
}

impl Div<f64> for FloatExp {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        Self::new((self.value as f64 / rhs).round() as i64, self.exp)
    }
}

impl Mul<i64> for FloatExp {
    type Output = Self;

    fn mul(self, rhs: i64) -> Self::Output {
        Self::new(self.value * rhs, self.exp)
    }
}

impl Div<i64> for FloatExp {
    type Output = Self;

    fn div(self, rhs: i64) -> Self::Output {
        Self::new(self.value / rhs, self.exp)
    }
}

impl Mul<Self> for FloatExp {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::new(self.value * rhs.value, self.exp + rhs.exp)
    }
}

impl AddAssign for FloatExp {
    fn add_assign(&mut self, rhs: Self) {
        assert_eq!(self.exp, rhs.exp);
        self.value += rhs.value;
    }
}

impl SubAssign for FloatExp {
    fn sub_assign(&mut self, rhs: Self) {
        assert_eq!(self.exp, rhs.exp);
        self.value -= rhs.value;
    }
}

impl PartialOrd for FloatExp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FloatExp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.exp == other.exp {
            return self.value.cmp(&other.value);
        }
        let exp = self.exp.min(other.exp);
        let a = self.round(exp);
        let b = other.round(exp);
        a.value.cmp(&b.value)
    }
}

impl PartialEq for FloatExp {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

#[test]
fn test_float_exp() {
    let a = FloatExp::from_f64(1.234, -2);
    let b = FloatExp::from_f64(2.345, -2);
    let c = FloatExp::from_f64(3.579, -2);
    assert_eq!(a + b, c);
    assert_eq!(a * 2., FloatExp::from_f64(2.46, -2));
    assert_eq!(a / 2., FloatExp::from_f64(0.62, -2));
    assert_eq!(a.to_f64(), 1.23);
    assert_eq!(a.to_i64(), 1);
    assert_eq!(c.to_i64(), 4);
    let x = FloatExp::new(1, -2);
    assert_eq!(x.to_f64(), 0.01);
    assert_eq!(x.round(0).value, 0);
}

#[test]
fn test_float_exp_display() {
    assert_eq!(format!("{}", FloatExp::from_f64(1.234, -2)), "1.23");
    assert_eq!(format!("{}", FloatExp::from_f64(1.234, -3)), "1.234");
    assert_eq!(format!("{}", FloatExp::from_f64(1.234, -4)), "1.2340");
    assert_eq!(format!("{}", FloatExp::from_f64(1.234, 0)), "1");
    println!("{:?}", FloatExp::from_f64(1.234, -2));
}

#[test]
fn test_float_exp_serialize() {
    use serde_json::json;
    let o = json!({
        "a": FloatExp::from_f64(1.234, -2),
    });
    assert_eq!(o.to_string(), "{\"a\":1.23}");
    let o = json!({
        "a": FloatExp::from_f64(1.234, 0),
    });
    assert_eq!(o.to_string(), "{\"a\":1}");
}