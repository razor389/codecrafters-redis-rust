use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use std::sync::Arc;
use std::fmt;

#[derive(Debug)]
pub struct WaitState {
    pub active: bool,
    pub num_slaves_to_wait_for: usize,
    pub responding_slaves: usize,
    pub start_time: Instant,
    pub timeout_duration: Duration,
}

impl WaitState {
    pub fn new(num_slaves: usize, timeout: u64) -> Self {
        Self {
            active: true,
            num_slaves_to_wait_for: num_slaves,
            responding_slaves: 0,
            start_time: Instant::now(),
            timeout_duration: Duration::from_millis(timeout),
        }
    }
}

// Define the enum to store either a String or a u32
#[derive(Debug)]
pub enum ReplicationInfoValue {
    StringValue(String),
    ByteValue(usize), // Number of bytes worth of commands processed
}

// Implement the Display trait for ReplicationInfoValue
impl fmt::Display for ReplicationInfoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicationInfoValue::StringValue(s) => write!(f, "{}", s),
            ReplicationInfoValue::ByteValue(bytes) => write!(f, "{} bytes", bytes),
        }
    }
}

pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
    pub replication_info: HashMap<String, ReplicationInfoValue>, // Changed to use the enum
    pub slave_connections: Vec<Arc<Mutex<TcpStream>>>, // Changed to store multiple slave connections
    pub wait_state: Option<WaitState>, // Add wait state to the database
}

impl RedisDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            replication_info: HashMap::new(),
            slave_connections: vec![],
            wait_state: None, // Initialize as None
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

    pub fn activate_wait_command(&mut self, num_slaves: usize, timeout: u64) {
        self.wait_state = Some(WaitState::new(num_slaves, timeout));
    }

    pub fn check_wait_timeout(&mut self) -> Option<usize> {
        if let Some(wait_state) = &self.wait_state {
            if wait_state.active && wait_state.start_time.elapsed() >= wait_state.timeout_duration {
                return Some(wait_state.responding_slaves);
            }
        }
        None
    }

    pub fn increment_responding_slaves(&mut self) {
        if let Some(wait_state) = &mut self.wait_state {
            wait_state.responding_slaves += 1;
        }
    }

    pub fn reset_wait_state(&mut self) {
        self.wait_state = None;
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
