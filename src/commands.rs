use crate::database::{RedisDatabase, RedisValue, RedisValueType, ReplicationInfoValue, StreamID};
use crate::parsing::parse_redis_message;
use std::collections::{BTreeMap, HashMap};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;

// Handle the SET command
pub fn handle_set(db: &mut RedisDatabase, args: &[String]) -> String {
    if args.len() == 2 {
        db.insert(args[0].clone(), RedisValue::new(args[1].clone(), None));
        "+OK\r\n".to_string()
    } else if args.len() == 4 && args[2].to_uppercase() == "PX" {
        let ttl = args[3].parse::<u64>().unwrap();
        db.insert(args[0].clone(), RedisValue::new(args[1].clone(), Some(ttl)));
        "+OK\r\n".to_string()
    } else {
        "-ERR wrong number of arguments for 'set' command\r\n".to_string()
    }
}

// Handle the GET command
pub fn handle_get(db: &mut RedisDatabase, args: &[String]) -> String {
    if let Some(redis_value) = db.get(&args[0]) {
        if redis_value.is_expired() {
            db.remove(&args[0]);
            "$-1\r\n".to_string()
        } else {
            format!("${}\r\n{}\r\n", redis_value.get_value().len(), redis_value.get_value())
        }
    } else {
        "$-1\r\n".to_string()
    }
}

// Handle the TYPE command
pub fn handle_type(db: &RedisDatabase, args: &[String]) -> String {
    if let Some(redis_value) = db.get(&args[0]) {
        match redis_value.get_value() {
            RedisValueType::StringValue(_) => "+string\r\n".to_string(),
            RedisValueType::StreamValue(_) => "+stream\r\n".to_string(),
        }
    } else {
        "+none\r\n".to_string()
    }
}

// Handle the XADD command
pub fn handle_xadd(db: &mut RedisDatabase, args: &[String]) -> String {
    if args.len() < 4 || args.len() % 2 != 0 {
        return "-ERR wrong number of arguments for 'xadd' command\r\n".to_string();
    }

    let stream_key = &args[0];
    let stream_id_str = &args[1];

    let stream_id = if stream_id_str == "*" {
        // Fully generate the stream ID using the current time
        if let Some(redis_value) = db.get(stream_key) {
            if let RedisValueType::StreamValue(stream) = redis_value.get_value() {
                StreamID::generate(stream)
            } else {
                return "-ERR wrong type for 'xadd' command\r\n".to_string();
            }
        } else {
            StreamID::generate(&BTreeMap::new()) // Generate if stream does not exist
        }
    } else if stream_id_str.contains('-') && stream_id_str.ends_with("-*") {
        // Partially generate stream ID, e.g., 1-*
        let time_part = stream_id_str.trim_end_matches("-*").parse::<u64>().unwrap();
        if let Some(redis_value) = db.get(stream_key) {
            if let RedisValueType::StreamValue(stream) = redis_value.get_value() {
                StreamID::generate_with_time(time_part, stream)
            } else {
                return "-ERR wrong type for 'xadd' command\r\n".to_string();
            }
        } else {
            StreamID {
                milliseconds_time: time_part,
                sequence_number: 0,
            } // No entries, so sequence number starts at 0
        }
    } else {
        // Parse the full stream ID
        match StreamID::from_str(stream_id_str) {
            Some(id) => id,
            None => return "-ERR invalid stream ID\r\n".to_string(),
        }
    };

    println!("stream id: {}", stream_id);

    // Check if the stream_id is greater than 0-0
    let zero_id = StreamID::zero();
    if !stream_id.is_valid(&zero_id) {
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n".to_string();
    }

    // Collect the key-value pairs for the stream entry
    let mut entry = HashMap::new();
    for i in (2..args.len()).step_by(2) {
        entry.insert(args[i].clone(), args[i + 1].clone());
    }

    // Check if the stream already exists in the database
    if let Some(redis_value) = db.get(stream_key) {
        if let RedisValueType::StreamValue(stream) = redis_value.get_value() {
            // Check if the stream has any entries
            if let Some(last_id) = stream.keys().max() {
                // Validate the new stream ID
                if !stream_id.is_valid(last_id) {
                    return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".to_string();
                }
            }
            let mut stream = stream.clone(); // Clone the stream to modify it
            stream.insert(stream_id.clone(), entry);
            db.insert(stream_key.clone(), RedisValue::new(stream, None)); // Update the stream in the database
        } else {
            return "-ERR wrong type for 'xadd' command\r\n".to_string();
        }
    } else {
        //create a new stream if it doesn't exist
        let mut stream = BTreeMap::new();
        stream.insert(stream_id.clone(), entry);
        db.insert(stream_key.clone(), RedisValue::new(stream, None));
    }

    // Return the stream_id as a RESP bulk string
    format!("${}\r\n{}\r\n", stream_id_str.len(), stream_id_str)
}


// Handle the KEYS command
pub fn handle_keys(db: &RedisDatabase) -> String {
    let keys: Vec<&String> = db.data.keys().collect();
    let mut response = format!("*{}\r\n", keys.len());
    for key in keys {
        response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
    }
    response
}

// Handle the CONFIG command
pub fn handle_config(config_map: &HashMap<String, String>, args: &[String]) -> String {
    if args.len() == 2 && args[0].to_uppercase() == "GET" {
        if let Some(value) = config_map.get(&args[1]) {
            format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", args[1].len(), args[1], value.len(), value)
        } else {
            "$-1\r\n".to_string()
        }
    } else {
        "-ERR syntax error\r\n".to_string()
    }
}

// Handle the ECHO command
pub fn handle_echo(args: &[String]) -> String {
    if args.len() == 1 {
        format!("${}\r\n{}\r\n", args[0].len(), args[0])
    } else {
        "-ERR wrong number of arguments for 'echo' command\r\n".to_string()
    }
}

// Handle the PING command
pub fn handle_ping(args: &[String]) -> String {
    if args.is_empty() {
        "+PONG\r\n".to_string()
    } else if args.len() == 1 {
        format!("${}\r\n{}\r\n", args[0].len(), args[0])
    } else {
        "-ERR wrong number of arguments for 'ping' command\r\n".to_string()
    }
}

// Handle the INFO REPLICATION command
pub fn handle_info(db: &RedisDatabase, args: &[String]) -> String {
    if args.len() == 1 && args[0].to_uppercase() == "REPLICATION" {
        let mut response = String::new();
        for (key, value) in &db.replication_info {
            response.push_str(&format!("{}:{}\r\n", key, value));
        }
        format!("${}\r\n{}\r\n", response.len(), response)
    } else {
        "-ERR unknown section for INFO\r\n".to_string()
    }
}

// Handle the REPLCONF command
pub fn handle_replconf(db: &RedisDatabase, args: &[String]) -> String {
    if args.len() == 2 && args[0].to_uppercase() == "GETACK" && args[1] == "*" {
        let bytes_processed = match db.get_replication_info("slave_repl_offset") {
            Some(ReplicationInfoValue::ByteValue(bytes)) => *bytes,  // Dereference to get the usize value
            _ => 0,  // Default to 0 if not found
        };

        format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", bytes_processed.to_string().len(), bytes_processed)
    } else {
        "+OK\r\n".to_string()
    }
}

// Handle the PSYNC command
pub fn handle_psync(db: &RedisDatabase, args: &[String]) -> String {
    if args.len() == 2 {
        if let Some(master_replid) = db.replication_info.get("master_replid") {
            if let Some(master_repl_offset) = db.replication_info.get("master_repl_offset") {
                return format!("+FULLRESYNC {} {}\r\n", master_replid, master_repl_offset);
            } else {
                return "-ERR master_repl_offset not found\r\n".to_string();
            }
        } else {
            return "-ERR master_replid not found\r\n".to_string();
        }
    } else {
        "-ERR wrong number of arguments for 'psync' command\r\n".to_string()
    }
}

// Asynchronously send the binary RDB file in RESP bulk string format
pub async fn send_rdb_file(stream: &mut OwnedWriteHalf) -> io::Result<()> {
    let hex_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let binary_data = hex_to_bytes(hex_rdb);

    // Prepare the length header in RESP format: $<length>\r\n
    let length_header = format!("${}\r\n", binary_data.len());

    // Send the length header and binary data asynchronously
    stream.write_all(length_header.as_bytes()).await?;
    stream.write_all(&binary_data).await?;
    stream.flush().await?;

    Ok(())
}

// Helper function to convert hex string to bytes
fn hex_to_bytes(hex: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chars = hex.chars();

    while let (Some(high), Some(low)) = (chars.next(), chars.next()) {
        let high_digit = high.to_digit(16).expect("Invalid hex character");
        let low_digit = low.to_digit(16).expect("Invalid hex character");
        bytes.push((high_digit * 16 + low_digit) as u8);
    }

    bytes
}

// Asynchronously process commands after receiving RDB file
pub async fn process_commands_after_rdb(
    partial_message: &mut String,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
    stream: &mut TcpStream,  // Added to send a response back to master
) -> io::Result<()> {
    

    let parsed_results = {
        let mut db_lock = db.lock().await;
        parse_redis_message(&partial_message, &mut db_lock, config_map)
    };

    for (command, args, response, _cursor, command_msg_length_bytes) in parsed_results {
        let partial_message_bytes = partial_message.as_bytes();

        if command_msg_length_bytes > partial_message_bytes.len() {
            eprintln!(
                "Error: consumed_length ({}) exceeds partial_message byte length ({}).",
                command_msg_length_bytes,
                partial_message_bytes.len()
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Consumed length exceeds the partial message byte length",
            ));
        }

        let remaining_bytes = &partial_message_bytes[command_msg_length_bytes..];
        *partial_message = String::from_utf8_lossy(remaining_bytes).to_string();

        if let Some(cmd) = command {
            match cmd.as_str() {
                "SET" => {
                    if args.len() >= 2 {
                        let key = args[0].clone();
                        let value = args[1].clone();
                        let mut db_lock = db.lock().await;
                        db_lock.insert(key.clone(), RedisValue::new(value.clone(), None));
                    }
                },
                "REPLCONF" => {
                    stream.write_all(response.as_bytes()).await?;
                },
                _ => println!("Unknown command: {}", cmd),
            }
        }
    }

    Ok(())
}
