// src/parsing.rs
use crate::database::RedisDatabase;
use crate::commands::{handle_set, handle_get, handle_config, handle_keys, handle_echo, handle_ping, handle_info, handle_replconf, handle_psync};
use std::collections::HashMap;

pub fn parse_redis_message(
    message: &str,
    db: &mut RedisDatabase,
    config_map: &HashMap<String, String>,
) -> Vec<(Option<String>, Vec<String>, String, usize)> {
    let mut lines = message.lines();
    let mut results = Vec::new();
    let mut partial_message = String::new();

    while let Some(line) = lines.next() {
        let mut command = None;
        let mut args = Vec::new();
        #[allow(unused_assignments)]
        let mut arg_count = 0;
        let mut consumed_length = 0;

        consumed_length += line.len() + 2;  // Include \r\n

        if line.starts_with('*') {
            // Parse the argument count
            if let Ok(count) = line[1..].parse::<usize>() {
                arg_count = count;
            } else {
                results.push((None, vec![], "-ERR invalid argument count\r\n".to_string(), consumed_length));
                continue;
            }

            // Parse the bulk strings (arguments) in RESP format
            while let Some(line) = lines.next() {
                consumed_length += line.len() + 2;  // Include \r\n

                if line.starts_with('$') {
                    // Expecting a bulk string length, skip over it to the next line
                    if let Some(arg) = lines.next() {
                        consumed_length += arg.len() + 2;  // Include \r\n

                        if command.is_none() {
                            // First argument is the command
                            command = Some(arg.to_uppercase());
                        } else {
                            // Subsequent arguments
                            args.push(arg.to_string());
                        }
                    } else {
                        // Incomplete message, return what we have so far
                        partial_message.push_str(&line);
                        break;
                    }
                }
            }

            // Ensure the number of arguments matches the declared count
            if args.len() + 1 != arg_count {
                results.push((command, args, "-ERR argument count mismatch\r\n".to_string(), consumed_length));
                continue;
            }

            // Debug output for parsed command and arguments
            println!("Parsed command: {:?}, Args: {:?}", command, args);

            // Match the command with appropriate handler
            let response = match command.as_deref() {
                Some("SET") => handle_set(db, &args),
                Some("GET") => handle_get(db, &args),
                Some("CONFIG") => handle_config(config_map, &args),
                Some("KEYS") => handle_keys(db),
                Some("ECHO") => handle_echo(&args),
                Some("PING") => handle_ping(&args),
                Some("INFO") => handle_info(db, &args),
                Some("REPLCONF") => handle_replconf(&args),
                Some("PSYNC") => handle_psync(db, &args),
                _ => "-ERR unknown command\r\n".to_string(),
            };

            results.push((command, args, response, consumed_length));  // Return parsed command, args, response, and consumed length
        } else {
            results.push((None, vec![], "-ERR invalid format\r\n".to_string(), consumed_length));
        }
    }

    results
}
