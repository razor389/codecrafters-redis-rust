// src/database.rs
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
    pub replication_info: HashMap<String, String>,
}

impl RedisDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            replication_info: HashMap::new(), // Initialize the replication info
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
enum TtlState {
    Waiting(Duration), // TTL is active, and the key is waiting to expire
    Expired,           // TTL has expired
}

#[derive(Debug)]
pub struct RedisValue {
    value: String,
    creation_time: Instant,
    ttl_state: Option<TtlState>,  // TTL state, either None, Waiting, or Expired
}

impl RedisValue {
    pub fn new(value: String, ttl_millis: Option<u64>) -> Self {
        let ttl_state = ttl_millis.map(|ttl| {
            if ttl == 0 {
                TtlState::Expired
            } else {
                TtlState::Waiting(Duration::from_millis(ttl))
            }
        });

        RedisValue {
            value,
            creation_time: Instant::now(),
            ttl_state,
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.ttl_state {
            Some(TtlState::Waiting(ttl)) => {
                let elapsed = self.creation_time.elapsed();
                println!("Debug: elapsed = {:?}", elapsed);
                println!("Debug: ttl = {:?}", ttl);

                elapsed >= ttl
            }
            Some(TtlState::Expired) => true,
            None => false,
        }
    }

    pub fn get_value(&self) -> &str {
        &self.value
    }
}
