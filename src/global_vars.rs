use once_cell::sync::OnceCell;

pub static DEBUG: OnceCell<bool> = OnceCell::new();

pub fn is_debug() -> bool {
    *DEBUG.get().unwrap()
}