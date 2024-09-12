use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, RwLock};

use std::fmt;

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
    pub slave_connections: RwLock<Vec<Arc<Mutex<OwnedWriteHalf>>>>, // Changed to store multiple slave connections
    pub ack_counter: Arc<Mutex<usize>>,
}

impl RedisDatabase {
    pub fn new() -> Self {
        // Create a broadcast channel with a capacity of 16 messages (adjust as needed)
        Self {
            data: HashMap::new(),
            replication_info: HashMap::new(),
            slave_connections: vec![].into(),
            ack_counter: Arc::new(Mutex::new(0))
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

// Define the StreamKey type
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StreamID(pub String);

// Define the value that Redis can hold
#[derive(Debug)]
pub enum RedisValueType {
    StringValue(String),
    StreamValue(HashMap<StreamID, HashMap<String, String>>), // Stream is a HashMap of HashMaps
}

#[derive(Debug)]
pub struct RedisValue {
    value: RedisValueType,
    creation_time: Instant,
    ttl_state: Option<TtlState>,
}

impl RedisValue {
    // New constructor that handles both String and Stream types
    pub fn new<V>(value: V, ttl_millis: Option<u64>) -> Self
    where
        V: Into<RedisValueType>,
    {
        let ttl_state = ttl_millis.map(|ttl| {
            if ttl == 0 {
                TtlState::Expired
            } else {
                TtlState::Waiting(Duration::from_millis(ttl))
            }
        });

        RedisValue {
            value: value.into(),
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

    pub fn get_value(&self) -> &RedisValueType  {
        &self.value
    }
}

// Implement the conversion from String to RedisValueType
impl From<String> for RedisValueType {
    fn from(s: String) -> Self {
        RedisValueType::StringValue(s)
    }
}

// Implement the conversion from HashMap<String, HashMap<String, String>> to RedisValueType
impl From<HashMap<String, HashMap<String, String>>> for RedisValueType {
    fn from(stream: HashMap<String, HashMap<String, String>>) -> Self {
        // Convert the HashMap<String, HashMap<String, String>> to HashMap<StreamID, HashMap<String, String>>
        let stream_converted: HashMap<StreamID, HashMap<String, String>> = stream
            .into_iter()
            .map(|(k, v)| (StreamID(k), v))
            .collect();
        RedisValueType::StreamValue(stream_converted)
    }
}

// Implement the conversion from HashMap<StreamID, HashMap<String, String>> to RedisValueType
impl From<HashMap<StreamID, HashMap<String, String>>> for RedisValueType {
    fn from(stream: HashMap<StreamID, HashMap<String, String>>) -> Self {
        RedisValueType::StreamValue(stream)
    }
}

impl RedisValueType {
    pub fn len(&self) -> usize {
        match self {
            RedisValueType::StringValue(s) => s.len(),
            RedisValueType::StreamValue(map) => map.len(),
        }
    }
}

impl fmt::Display for RedisValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisValueType::StringValue(s) => write!(f, "{}", s),
            RedisValueType::StreamValue(map) => {
                write!(f, "{{")?;
                for (key, inner_map) in map {
                    write!(f, "{}: {{", key.0)?; // Accessing the inner string of StreamID
                    for (k, v) in inner_map {
                        write!(f, "{}: {}, ", k, v)?;
                    }
                    write!(f, "}}, ")?;
                }
                write!(f, "}}")
            }
        }
    }
}