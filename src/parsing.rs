use tokio::sync::Mutex;
use crate::database::{RedisDatabase, ReplicationInfoValue};
use crate::commands::{handle_config, handle_echo, handle_get, handle_incr, handle_info, handle_keys, handle_ping, handle_psync, handle_replconf, handle_set, handle_type, handle_xadd, handle_xrange, handle_xread};
use crate::network::ClientState;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn parse_redis_message(
    message: &str,
    db: &Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
    client_state: &mut ClientState,
) -> Vec<(Option<String>, Vec<String>, String, usize)> {
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
                    results.push((None, vec![], "-ERR incomplete message\r\n".to_string(), 0));
                    break;
                }
            };

            let arg_count = match std::str::from_utf8(&bytes[cursor..cursor + end])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(count) => count,
                None => {
                    results.push((None, vec![], "-ERR invalid argument count\r\n".to_string(), 0));
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
                            results.push((None, vec![], "-ERR incomplete message\r\n".to_string(), 0));
                            break;
                        }
                    };

                    let bulk_len = match std::str::from_utf8(&bytes[cursor..cursor + end])
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(len) => len,
                        None => {
                            results.push((None, vec![], "-ERR invalid bulk string length\r\n".to_string(), 0));
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
                        results.push((None, vec![], "-ERR incomplete bulk string\r\n".to_string(), 0));
                        break;
                    }
                } else {
                    results.push((None, vec![], "-ERR expected bulk string\r\n".to_string(), 0));
                    break;
                }
            }

            let command = command.as_deref();
            match command {
                Some("MULTI") => {
                    if client_state.in_transaction() {
                        results.push((Some("MULTI".to_string()), vec![], "-ERR MULTI already started\r\n".to_string(), 0));
                    } else {
                        client_state.initialiaze_multiqueue();
                        results.push((Some("MULTI".to_string()), vec![], "+OK\r\n".to_string(),  0));
                    }
                }
                Some("EXEC") => {
                    if client_state.in_transaction() {
                        let response = execute_queued_commands(client_state.get_multi_queue_ref(), db, config_map).await;
                        client_state.deactivate_multiqueue();
                         // RESP array of responses from queued commands
                        results.push((
                            Some("EXEC".to_string()),
                            vec![],
                            response,
                            0,
                        ));
                    } else {
                        results.push((Some("EXEC".to_string()), vec![], "-ERR EXEC without MULTI\r\n".to_string(), 0));
                    }
                }
                Some("DISCARD") => {
                    if client_state.in_transaction() {
                        client_state.deactivate_multiqueue();
                        results.push((Some("DISCARD".to_string()), vec![], "+OK\r\n".to_string(), 0));
                    } else {
                        results.push((Some("DISCARD".to_string()), vec![], "-ERR DISCARD without MULTI\r\n".to_string(), 0));
                    }
                }
                _ => {
                    if client_state.in_transaction() {
                        // Add the command and its arguments to the multi_queue instead of executing
                        if let Some(queue) = client_state.get_mut_multi_queue_ref(){
                            queue.push((command.unwrap_or_default().to_string(), args.clone()));
                            results.push((command.map(|cmd| cmd.to_string()), args.clone(), "+QUEUED\r\n".to_string(), 0));
                        }
                    } else {
                        // Execute the command normally if not in transaction mode
                        // Handle the command once all args are collected
                        let response = match command {
                            Some("SET") => handle_set(db, &args).await,
                            Some("GET") => handle_get(db, &args).await,
                            Some("CONFIG") => handle_config(config_map, &args),
                            Some("KEYS") => handle_keys(db).await,
                            Some("ECHO") => handle_echo(&args),
                            Some("PING") => handle_ping(&args),
                            Some("INFO") => handle_info(db, &args).await,
                            Some("REPLCONF") => handle_replconf(db,&args).await,
                            Some("PSYNC") => handle_psync(db, &args).await,
                            Some("WAIT") => "".to_string(),
                            Some("TYPE") => handle_type(db, &args).await,
                            Some("XADD") => handle_xadd(db, &args).await,
                            Some("XRANGE") => handle_xrange(db, &args).await,
                            Some("XREAD") => handle_xread(db, &args).await,
                            Some("INCR") => handle_incr(db, &args).await,
                            _ => "-ERR unknown command\r\n".to_string(),
                        };

                        // Calculate the byte length of the entire command
                        let byte_length = cursor - initial_cursor;
                        // Incrementally update the bytes_processed
                        let mut db_lock = db.lock().await;
                        if let Some(ReplicationInfoValue::StringValue(role)) = db_lock.get_replication_info("role") {
                            if role == "slave" {
                                println!("updating slave repl offset");
                                let cmd_bytes = match db_lock.get_replication_info("slave_repl_offset") {
                                    Some(ReplicationInfoValue::ByteValue(current_bytes)) => {
                                        current_bytes + byte_length
                                    }
                                    _ => byte_length,  // Initialize if not present
                                };
                        
                                // Update the replication info in the database
                                db_lock.update_replication_info(
                                    "slave_repl_offset".to_string(),
                                    ReplicationInfoValue::ByteValue(cmd_bytes),
                                );
                            }
                        }
                        
                        //println!("Updated bytes_processed to: {}", cmd_bytes);

                        // Push the result (command, args, response, cursor, byte_length)
                        results.push((command.map(|cmd| cmd.to_string()), args, response, byte_length));
                    }
                }
            }
            
        } else {
            results.push((None, vec![], "-ERR invalid format\r\n".to_string(), 0));
            break;
        }
    }

    results
}

async fn execute_queued_commands(
    queue: &Option<Vec<(String, Vec<String>)>>,
    db: &Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> String {
    let mut responses = Vec::new();

    if let Some(commands) = queue {
        for (command, args) in commands {
            let response = match command.as_str() {
                "SET" => handle_set(db, &args).await,
                "GET" => handle_get(db, &args).await,
                "INCR" => handle_incr(db, &args).await,
                "CONFIG" => handle_config(config_map, &args),
                "KEYS" => handle_keys(db).await,
                "ECHO" => handle_echo(&args),
                "PING" => handle_ping(&args),
                "INFO" => handle_info(db, &args).await,
                "REPLCONF" => handle_replconf(db,&args).await,
                "PSYNC" => handle_psync(db, &args).await,
                "WAIT" => "".to_string(),
                "TYPE" => handle_type(db, &args).await,
                "XADD" => handle_xadd(db, &args).await,
                "XRANGE" => handle_xrange(db, &args).await,
                "XREAD"=> handle_xread(db, &args).await,
                // Add other supported commands here
                _ => "-ERR unknown command\r\n".to_string(),
            };

            responses.push(response);
        }
    }
    // Format responses as a RESP array
    let mut resp_array = format!("*{}\r\n", responses.len());
    for response in responses {
        resp_array.push_str(&format!("${}\r\n{}\r\n", response.len(), response));
    }
    resp_array
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
