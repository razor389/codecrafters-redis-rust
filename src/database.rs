use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, RwLock};
use std::fmt;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

// Define the StreamID struct
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StreamID {
    pub milliseconds_time: u64,  // The first part of the ID
    pub sequence_number: u64,    // The second part of the ID
}

impl StreamID {
    // Parse a string like "12345-1" into a StreamID
    pub fn from_str(id_str: &str) -> Option<StreamID> {
        let parts: Vec<&str> = id_str.split('-').collect();
        if parts.len() != 2 {
            return None;
        }
        let milliseconds_time = parts[0].parse::<u64>().ok()?;
        let sequence_number = parts[1].parse::<u64>().ok()?;
        Some(StreamID {
            milliseconds_time,
            sequence_number,
        })
    }

    // Compare the new stream ID with the last stream ID for validation
    pub fn is_valid(&self, last_id: &StreamID) -> bool {
        if self.milliseconds_time > last_id.milliseconds_time {
            return true;
        } else if self.milliseconds_time == last_id.milliseconds_time {
            return self.sequence_number > last_id.sequence_number;
        }
        false
    }
}

// Implement Ord and PartialOrd to allow ordering in BTreeMap
impl Ord for StreamID {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.milliseconds_time.cmp(&other.milliseconds_time) {
            Ordering::Equal => self.sequence_number.cmp(&other.sequence_number),
            other => other,
        }
    }
}

impl PartialOrd for StreamID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.milliseconds_time, self.sequence_number)
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

// Define the value that Redis can hold
#[derive(Debug)]
pub enum RedisValueType {
    StringValue(String),
    StreamValue(BTreeMap<StreamID, HashMap<String, String>>), // Stream is now a BTreeMap for ordered entries
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

// Implement the conversion from HashMap<StreamID, HashMap<String, String>> to RedisValueType
impl From<BTreeMap<StreamID, HashMap<String, String>>> for RedisValueType {
    fn from(stream: BTreeMap<StreamID, HashMap<String, String>>) -> Self {
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
            RedisValueType::StringValue(s) => {
                write!(f, "{}", s)
            }
            RedisValueType::StreamValue(map) => {
                write!(f, "{{\n")?;
                for (key, inner_map) in map {
                    write!(f, "  {}: {{\n", key)?; // StreamID's Display will handle key
                    for (k, v) in inner_map {
                        write!(f, "    {}: {},\n", k, v)?; // Indent key-value pairs
                    }
                    write!(f, "  }},\n")?;
                }
                write!(f, "}}")
            }
        }
    }
}
