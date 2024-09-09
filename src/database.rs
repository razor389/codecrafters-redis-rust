use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::net::TcpStream;
use std::fmt;

// Define the enum to store either a String or a u32
#[derive(Debug)]
pub enum ReplicationInfoValue {
    StringValue(String),
    CommandBytes(usize), // Number of bytes worth of commands processed
}

// Implement the Display trait for ReplicationInfoValue
impl fmt::Display for ReplicationInfoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicationInfoValue::StringValue(s) => write!(f, "{}", s),
            ReplicationInfoValue::CommandBytes(bytes) => write!(f, "{} bytes", bytes),
        }
    }
}

pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
    pub replication_info: HashMap<String, ReplicationInfoValue>, // Changed to use the enum
    pub slave_connections: Vec<Arc<Mutex<TcpStream>>>, // Changed to store multiple slave connections
}

impl RedisDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            replication_info: HashMap::new(),
            slave_connections: vec![],
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

    // Update the replication info with either a string or a number
    pub fn update_replication_info(&mut self, key: String, value: ReplicationInfoValue) {
        self.replication_info.insert(key, value);
    }

    // Get replication info as a string (for display or logging)
    pub fn get_replication_info(&self, key: &str) -> Option<&ReplicationInfoValue> {
        self.replication_info.get(key)
    }
}

#[derive(Debug)]
enum TtlState {
    Waiting(Duration),
    Expired,
}

#[derive(Debug)]
pub struct RedisValue {
    value: String,
    creation_time: Instant,
    ttl_state: Option<TtlState>,
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
