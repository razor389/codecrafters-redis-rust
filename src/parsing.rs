use crate::database::{RedisDatabase, ReplicationInfoValue};
use crate::commands::{handle_set, handle_get, handle_config, handle_keys, handle_echo, handle_ping, handle_info, handle_replconf, handle_psync, handle_wait};
use std::collections::HashMap;

pub fn parse_redis_message(
    message: &str,
    db: &mut RedisDatabase,
    config_map: &HashMap<String, String>,
) -> Vec<(Option<String>, Vec<String>, String, usize, usize)> {
    let mut results = Vec::new();
    let mut cursor = 0;
    let bytes = message.as_bytes();

    while cursor < bytes.len() {
        let initial_cursor = cursor; // Track where this message started

        // Check for argument count prefix *
        if bytes[cursor] == b'*' {
            cursor += 1; // Move past '*'

            // Parse the number of arguments
            let end = match find_crlf(&bytes[cursor..]) {
                Some(e) => e,
                None => {
                    results.push((None, vec![], "-ERR incomplete message\r\n".to_string(), cursor, 0));
                    break;
                }
            };

            let arg_count = match std::str::from_utf8(&bytes[cursor..cursor + end])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(count) => count,
                None => {
                    results.push((None, vec![], "-ERR invalid argument count\r\n".to_string(), cursor, 0));
                    break;
                }
            };
            cursor += end + 2; // Move past \r\n

            let mut command = None;
            let mut args = Vec::new();

            // Parse each bulk string
            for _ in 0..arg_count {
                // Expect $ for bulk string
                if bytes[cursor] == b'$' {
                    cursor += 1; // Move past '$'

                    // Get bulk string length
                    let end = match find_crlf(&bytes[cursor..]) {
                        Some(e) => e,
                        None => {
                            results.push((None, vec![], "-ERR incomplete message\r\n".to_string(), cursor, 0));
                            break;
                        }
                    };

                    let bulk_len = match std::str::from_utf8(&bytes[cursor..cursor + end])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(len) => len,
                        None => {
                            results.push((None, vec![], "-ERR invalid bulk string length\r\n".to_string(), cursor, 0));
                            break;
                        }
                    };
                    cursor += end + 2; // Move past \r\n

                    // Extract bulk string
                    if cursor + bulk_len + 2 <= bytes.len() {
                        let bulk_string = &message[cursor..cursor + bulk_len];
                        cursor += bulk_len + 2; // Move past the string and \r\n

                        if command.is_none() {
                            command = Some(bulk_string.to_uppercase());
                        } else {
                            args.push(bulk_string.to_string());
                        }
                    } else {
                        results.push((None, vec![], "-ERR incomplete bulk string\r\n".to_string(), cursor, 0));
                        break;
                    }
                } else {
                    results.push((None, vec![], "-ERR expected bulk string\r\n".to_string(), cursor, 0));
                    break;
                }
            }

            // Handle the command once all args are collected
            let response = match command.as_deref() {
                Some("SET") => handle_set(db, &args),
                Some("GET") => handle_get(db, &args),
                Some("CONFIG") => handle_config(config_map, &args),
                Some("KEYS") => handle_keys(db),
                Some("ECHO") => handle_echo(&args),
                Some("PING") => handle_ping(&args),
                Some("INFO") => handle_info(db, &args),
                Some("REPLCONF") => handle_replconf(db,&args),
                Some("PSYNC") => handle_psync(db, &args),
                Some("WAIT") => handle_wait(),
                _ => "-ERR unknown command\r\n".to_string(),
            };

            // Calculate the byte length of the entire command
            let byte_length = cursor - initial_cursor;
            // Incrementally update the bytes_processed
            if let Some(ReplicationInfoValue::StringValue(role)) = db.get_replication_info("role") {
                if role == "slave" {
                    println!("updating slave repl offset");
                    let cmd_bytes = match db.get_replication_info("slave_repl_offset") {
                        Some(ReplicationInfoValue::ByteValue(current_bytes)) => {
                            current_bytes + byte_length
                        }
                        _ => byte_length,  // Initialize if not present
                    };
            
                    // Update the replication info in the database
                    db.update_replication_info(
                        "slave_repl_offset".to_string(),
                        ReplicationInfoValue::ByteValue(cmd_bytes),
                    );
                }
            }
            
            //println!("Updated bytes_processed to: {}", cmd_bytes);

            // Push the result (command, args, response, cursor, byte_length)
            results.push((command, args, response, cursor, byte_length));
        } else {
            results.push((None, vec![], "-ERR invalid format\r\n".to_string(), cursor, 0));
            break;
        }
    }

    results
}

// Helper function to find CRLF (\r\n)
fn find_crlf(bytes: &[u8]) -> Option<usize> {
    for i in 0..bytes.len() - 1 {
        if bytes[i] == b'\r' && bytes[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}
