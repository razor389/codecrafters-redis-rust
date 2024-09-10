use crate::database::{RedisDatabase, RedisValue, ReplicationInfoValue};
use crate::parsing::parse_redis_message;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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

// Handle the WAIT command with optional byte length matching and multi-threading
pub fn handle_wait(db: &mut RedisDatabase, args: &[String], check_byte_length: bool) -> String {
    if args.len() != 2 {
        return "-ERR wrong number of arguments for WAIT\r\n".to_string();
    }

    // Parse the number of slaves to wait for
    let num_slaves_to_wait_for: usize = match args[0].parse() {
        Ok(n) => n,
        Err(_) => return "-ERR invalid number of slaves\r\n".to_string(),
    };

    // Parse the timeout in milliseconds
    let timeout_ms: u64 = match args[1].parse() {
        Ok(t) => t,
        Err(_) => return "-ERR invalid timeout value\r\n".to_string(),
    };

    // Start the timer to keep track of the elapsed time
    let start_time = Instant::now();
    let timeout_duration = Duration::from_millis(timeout_ms);

    // Send REPLCONF GETACK * command to each slave
    let replconf_getack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    let replconf_byte_len = replconf_getack_message.as_bytes().len();

    // Spawn a thread for each slave connection to wait for their ACKs
    let mut handles = vec![];
    for slave in db.slave_connections.iter() {
        let slave_clone = Arc::clone(slave);

        // Spawn a thread for each slave connection
        let handle = thread::spawn(move || {
            let mut slave_stream = slave_clone.lock().unwrap();
            
            // Send the REPLCONF GETACK * command
            if slave_stream.write_all(replconf_getack_message.as_bytes()).is_err() {
                return 0; // Return 0 if writing fails
            }
            let _ = slave_stream.flush();

            // Calculate remaining time
            let elapsed_time = start_time.elapsed();
            if elapsed_time >= timeout_duration {
                return 0; // Timeout already exceeded
            }

            // Wait for the acknowledgment on the slave's stream with the remaining time
            let remaining_time = timeout_duration - elapsed_time;
            if wait_for_ack(&mut slave_stream, remaining_time, replconf_byte_len, check_byte_length).is_ok() {
                1 // Return 1 if the slave responds with an ACK
            } else {
                0 // Return 0 if no valid ACK is received
            }
        });

        // Store the handle to join the threads later
        handles.push(handle);
    }

    // Collect the results from all the threads
    let mut responding_slaves = 0;
    for handle in handles {
        responding_slaves += handle.join().unwrap_or(0); // Accumulate the number of successful ACKs
        if responding_slaves >= num_slaves_to_wait_for {
            break; // If we already have enough slaves, break early
        }

         // Check if the timeout has been exceeded
         if start_time.elapsed() >= timeout_duration {
            break; // Exit if timeout is exceeded
        }
    }

    // Update the replication_info without locking
    if let Some(ReplicationInfoValue::ByteValue(current_offset)) = db.replication_info.get("master_repl_offset") {
        // Increment the offset by the number of bytes sent
        let new_offset = current_offset + replconf_byte_len;

        // Update the replication_info with the new offset as a ByteValue
        db.replication_info.insert(
            "master_repl_offset".to_string(),
            ReplicationInfoValue::ByteValue(new_offset),
        );
    } else {
        // If the master_repl_offset does not exist, initialize it with the current bytes sent
        db.replication_info.insert(
            "master_repl_offset".to_string(),
            ReplicationInfoValue::ByteValue(replconf_byte_len),
        );
    }

    // Return the count of responding slaves
    format!(":{}\r\n", responding_slaves)
}

// A function that waits for an acknowledgment from a slave, optionally checking byte length
fn wait_for_ack(slave_stream: &mut TcpStream, remaining_time: Duration, replconf_byte_len: usize, check_byte_length: bool) -> Result<(), ()> {
    // Set the read timeout for the remaining time
    slave_stream.set_read_timeout(Some(remaining_time)).ok();

    // Buffer to store the incoming response
    let mut buffer = [0; 1024];
    
    // Attempt to read from the stream
    match slave_stream.read(&mut buffer) {
        Ok(bytes_read) if bytes_read > 0 => {
            println!("read bytes from slave");
            let response = String::from_utf8_lossy(&buffer[..bytes_read]);

            // Parse the response
            if response.contains("REPLCONF") && response.contains("ACK") {
                if check_byte_length {
                    // Extract the bytes processed from the response
                    if let Some(bytes_processed_str) = response.split("\r\n").last() {
                        if let Ok(bytes_processed) = bytes_processed_str.parse::<usize>() {
                            // If byte length matching is enabled, ensure the slave's replication offset is valid
                            if bytes_processed >= replconf_byte_len {
                                return Ok(()); // Slave has acknowledged
                            }
                        }
                    }
                    Err(()) // Byte length mismatch
                } else {
                    // No byte length checking, just return success if an ACK is found
                    return Ok(());
                }
            } else {
                Err(()) // No valid ACK found
            }
        }
        _ => Err(()), // Error or no data received
    }
}

// Handle the REPLCONF command
pub fn handle_replconf(db: &RedisDatabase, args: &[String]) -> String {
    if args.len() == 2 && args[0].to_uppercase() == "GETACK" && args[1] == "*" {
        // Retrieve the value of "bytes_processed" from the replication info
        let bytes_processed = match db.get_replication_info("slave_repl_offset") {
            Some(ReplicationInfoValue::ByteValue(bytes)) => *bytes,  // Dereference to get the usize value
            _ => 0,  // Default to 0 if not found
        };

        // Respond with "REPLCONF ACK <bytes_processed>"
        format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", bytes_processed.to_string().len(), bytes_processed)
    } else {
        // Otherwise, respond with +OK
        "+OK\r\n".to_string()
    }
}


// Handle the PSYNC command
pub fn handle_psync(db: &RedisDatabase, args: &[String]) -> String {
    if args.len() == 2 {
        if let Some(master_replid) = db.replication_info.get("master_replid") {
            if let Some(master_repl_offset) = db.replication_info.get("master_repl_offset") {
                // Prepare FULLRESYNC response
                let response = format!("+FULLRESYNC {} {}\r\n", master_replid, master_repl_offset);
                return response;
            } else {
                return "-ERR master_repl_offset not found\r\n".to_string();
            }
        } else {
            return "-ERR master_replid not found\r\n".to_string();
        }
    } else {
        return "-ERR wrong number of arguments for 'psync' command\r\n".to_string();
    }
}

// Function to send the binary RDB file in RESP bulk string format synchronously
pub fn send_rdb_file(stream: &mut TcpStream) -> io::Result<()> {
    // Hex representation of the empty RDB file
    let hex_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    // Convert hex string to binary
    let binary_data = hex_to_bytes(hex_rdb);

    // Prepare the length header in RESP format: $<length>\r\n
    let length_header = format!("${}\r\n", binary_data.len());
    
    // Send the length header first
    stream.write_all(length_header.as_bytes())?;

    // Send the raw binary data
    stream.write_all(&binary_data)?;

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

pub fn process_commands_after_rdb(
    partial_message: &mut String,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
    stream: &mut TcpStream,  // Added to send a response back to master
) -> io::Result<()> {
    let mut db_lock = db.lock().unwrap();
    //println!("processing message: {}", partial_message);

    // Parse the Redis message and handle the parsed commands
    let parsed_results = parse_redis_message(&partial_message, &mut db_lock, config_map);
    //println!("parsed results: {:?}", parsed_results);

    for (command, args, response, _cursor, command_msg_length_bytes) in parsed_results {
        // Ensure we are draining the string based on bytes
        let partial_message_bytes = partial_message.as_bytes();

        // Check if the consumed_length is within bounds before proceeding
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

        // Safely drain the bytes from the partial_message and convert back to a String
        let remaining_bytes = &partial_message_bytes[command_msg_length_bytes..];
        *partial_message = String::from_utf8_lossy(remaining_bytes).to_string();

        //println!("partial_message after drain: {}", partial_message);
        
        if let Some(cmd) = command {
            println!("command: {}", cmd);
            println!("command message length bytes: {}", command_msg_length_bytes);
            match cmd.as_str() {
                "SET" => {
                    if args.len() >= 2 {
                        let key = args[0].clone();
                        let value = args[1].clone();
                        db_lock.insert(key.clone(), RedisValue::new(value.clone(), None));
                        println!("Applied SET command: {} = {}", key, value);
                    }
                },
                "REPLCONF" => {
                    // Handle REPLCONF commands
                    println!("REPLCONF command received, responding with: {}", response);
                    stream.write_all(response.as_bytes())?;
                },
                _ => println!("Unknown command: {}", cmd),
            }
        }
    }

    Ok(())
}

