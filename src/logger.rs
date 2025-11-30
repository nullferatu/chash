// src/logger.rs
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Logger {
    file: Arc<Mutex<File>>,
}

impl Logger {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn log(&self, msg: String) {
        // Get timestamp in microseconds
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        
        let line = format!("{}: {}", ts, msg);
        
        // Lock the file just long enough to write one line
        let mut f = self.file.lock().unwrap();
        writeln!(f, "{}", line).unwrap();
    }
}