// src/database.rs
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
}

impl RedisDatabase {
    pub fn new() -> Self {
        RedisDatabase {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: RedisValue) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&RedisValue> {
        self.data.get(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.data.remove(key);
    }
}

#[derive(Debug)]
pub struct RedisValue {
    value: String,
    creation_time: Instant,
    ttl: Option<Duration>,
}

impl RedisValue {
    pub fn new(value: String, ttl: Option<u64>, is_milliseconds: bool) -> Self {
        let ttl_duration = if is_milliseconds {
            ttl.map(Duration::from_millis)
        } else {
            ttl.map(Duration::from_secs)
        };
        RedisValue {
            value,
            creation_time: Instant::now(),
            ttl: ttl_duration,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let elapsed = self.creation_time.elapsed();

            println!("Debug: now = {:?}", Instant::now());
            println!("Debug: creation_time = {:?}", self.creation_time);
            println!("Debug: elapsed = {:?}", elapsed);
            println!("Debug: ttl = {:?}", ttl);

            let is_expired = elapsed > ttl;

            println!("Debug: is_expired = {}", is_expired);

            is_expired
        } else {
            false
        }
    }

    pub fn get_value(&self) -> &str {
        &self.value
    }
}