use serde_json::Value;

pub fn object_update(a: &mut Value, b: Value) -> anyhow::Result<()> {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                a.insert(k, v);
            }
            Ok(())
        },
        (_a, _b) => {
            Err(anyhow::anyhow!("object_update: a or b is not object"))
        }
    }
}
