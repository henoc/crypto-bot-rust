


pub struct BotLogger;

impl log::Log for BotLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S+00:00");
            println!("[{} {}] {}", timestamp, record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

#[test]
fn test_logging() {
    log::set_logger(&BotLogger)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
        .unwrap();
    log::info!("hello world");
}
