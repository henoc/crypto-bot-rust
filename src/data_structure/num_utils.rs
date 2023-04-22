use num_traits::{Num, FromPrimitive};

/// Round down to the specified number of decimal places.
/// 
/// ```
/// use bot::data_structure::num_utils::floor_int;
/// assert_eq!(floor_int(123456, 3), 123000);
/// ```
pub fn floor_int<T: Num + FromPrimitive + Clone>(x: T, y: u32) -> T {
    let exp = T::from_i64(10i64.pow(y)).unwrap();
    (x / exp.clone()) * exp
}

/// Round up to the specified number of decimal places.
/// 
/// ```
/// use bot::data_structure::num_utils::ceil_int;
/// assert_eq!(ceil_int(123456, 3), 124000);
/// ```
pub fn ceil_int<T: Num + FromPrimitive + Clone>(x: T, y: u32) -> T {
    let exp = T::from_i64(10i64.pow(y)).unwrap();
    ((x + exp.clone() - T::one()) / exp.clone()) * exp
}

#[test]
fn test_ceil_int() {
    assert_eq!(ceil_int(123456, 3), 124000);
}