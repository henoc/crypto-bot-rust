use serde_json::Value;

pub fn object_update(a: &mut Value, b: Value) {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
        },
        (a, b) => {}
    }
}
