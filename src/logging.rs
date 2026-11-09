//! Structured key=value logs on stderr. The chaos harness greps these to prove
//! which code paths a run actually exercised.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LEVEL: AtomicU8 = AtomicU8::new(2);

pub const ERROR: u8 = 0;
pub const WARN: u8 = 1;
pub const INFO: u8 = 2;
pub const DEBUG: u8 = 3;

pub fn init_from_env() {
    let level = match std::env::var("RAFTKV_LOG").unwrap_or_default().to_lowercase().as_str() {
        "error" => ERROR,
        "warn" => WARN,
        "debug" => DEBUG,
        _ => INFO,
    };
    LEVEL.store(level, Ordering::Relaxed);
}

pub fn enabled(level: u8) -> bool {
    level <= LEVEL.load(Ordering::Relaxed)
}

pub fn log(level: u8, msg: std::fmt::Arguments<'_>) {
    if !enabled(level) {
        return;
    }
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    let name = ["ERROR", "WARN", "INFO", "DEBUG"][level as usize];
    eprintln!("ts_ms={ms} level={name} {msg}");
}

#[macro_export]
macro_rules! info { ($($a:tt)*) => { $crate::logging::log($crate::logging::INFO, format_args!($($a)*)) } }
#[macro_export]
macro_rules! warn_log { ($($a:tt)*) => { $crate::logging::log($crate::logging::WARN, format_args!($($a)*)) } }
#[macro_export]
macro_rules! error_log { ($($a:tt)*) => { $crate::logging::log($crate::logging::ERROR, format_args!($($a)*)) } }
