use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, RwLock};
use std::fmt::{self, Debug};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

// Define the StreamID struct
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct StreamID {
    pub milliseconds_time: u64,  // The first part of the ID
    pub sequence_number: u64,    // The second part of the ID
}

impl StreamID {
    // Generate a StreamID with the current Unix time and a sequence number
    pub fn generate(stream: &BTreeMap<StreamID, HashMap<String, String>>) -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let sequence_number = stream
            .iter()
            .rev()
            .find(|(id, _)| id.milliseconds_time == current_time)
            .map_or(0, |(id, _)| id.sequence_number + 1);

        StreamID {
            milliseconds_time: current_time,
            sequence_number,
        }
    }

    // Generate a StreamID with the provided milliseconds_time and the first available sequence number
    pub fn generate_with_time(
        milliseconds_time: u64,
        stream: &BTreeMap<StreamID, HashMap<String, String>>,
    ) -> Self {
        // For time 0, the first sequence number must be 1 (since 0-0 is not allowed)
        let sequence_number = if milliseconds_time == 0 {
            stream
                .iter()
                .rev()
                .find(|(id, _)| id.milliseconds_time == 0)
                .map_or(1, |(id, _)| id.sequence_number + 1)
        } else {
            stream
                .iter()
                .rev()
                .find(|(id, _)| id.milliseconds_time == milliseconds_time)
                .map_or(0, |(id, _)| id.sequence_number + 1)
        };

        StreamID {
            milliseconds_time,
            sequence_number,
        }
    }

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

    pub fn zero() -> Self {
        StreamID {
            milliseconds_time: 0,
            sequence_number: 0,
        }
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

    pub fn get_mut(&mut self, key: &str) -> Option<&mut RedisValue> {
        self.data.get_mut(key)
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
    IntegerValue(u64),
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

    pub fn get_mut_value(&mut self) -> &mut RedisValueType{
        &mut self.value
    }
}

// Implement the conversion from String to RedisValueType
impl From<String> for RedisValueType {
    fn from(s: String) -> Self {
        // Attempt to parse the string as a u64
        if let Ok(int_value) = s.parse::<u64>() {
            RedisValueType::IntegerValue(int_value)
        } else {
            RedisValueType::StringValue(s)
        }
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
            RedisValueType::IntegerValue(integer) => {let int_str = integer.to_string(); int_str.len()},
            RedisValueType::StringValue(s) => s.len(),
            RedisValueType::StreamValue(map) => map.len(),
        }
    }
}

impl fmt::Display for RedisValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisValueType::IntegerValue(integer) => {let int_str = integer.to_string(); write!(f, "{}", int_str)}
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
