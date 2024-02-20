use std::sync::OnceLock;

pub static DEBUG: OnceLock<Option<String>> = OnceLock::new();

pub fn debug_is_none() -> bool {
    DEBUG.get().as_ref().unwrap().is_none()
}

pub fn debug_is_some_any() -> bool {
    DEBUG.get().as_ref().unwrap().is_some()
}

pub fn debug_is_some_except(lst: &[&str]) -> bool {
    DEBUG.get().as_ref().unwrap().as_ref().is_some_and(|s| !lst.contains(&s.as_str()))
}

pub fn debug_is_some(s: &str) -> bool {
    DEBUG.get().as_ref().unwrap().as_ref().map(|s| s.as_str()) == Some(s)
}
