/// Round down to the specified number of decimal places.
/// 
/// ```
/// use bot::data_structure::num_utils::floor_int;
/// assert_eq!(floor_int(123456, 3), 123000);
/// ```
pub fn floor_int(x: i64, y: u32) -> i64 {
    let exp = 10i64.pow(y);
    (x / exp) * exp
}

/// Round up to the specified number of decimal places.
/// 
/// ```
/// use bot::data_structure::num_utils::ceil_int;
/// assert_eq!(ceil_int(123456, 3), 124000);
/// ```
pub fn ceil_int(x: i64, y: u32) -> i64 {
    let exp = 10i64.pow(y);
    ((x + exp - 1) / exp) * exp
}

#[test]
fn test_ceil_int() {
    assert_eq!(ceil_int(123456, 3), 124000);
}