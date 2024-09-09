// src/parsing.rs
use crate::database::RedisDatabase;
use crate::commands::{handle_set, handle_get, handle_config, handle_keys, handle_echo, handle_ping, handle_info, handle_replconf, handle_psync};
use std::collections::HashMap;

pub fn parse_redis_message(
    message: &str,
    db: &mut RedisDatabase,
    config_map: &HashMap<String, String>,
) -> (Option<String>, Vec<String>, String) {
    let mut lines = message.lines();
    let mut command = None;
    let mut args = Vec::new();
    let mut arg_count = 0;

    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            // Parse the argument count
            if let Ok(count) = line[1..].parse::<usize>() {
                arg_count = count;
            } else {
                return (None, vec![], "-ERR invalid argument count\r\n".to_string());
            }

            // Parse the bulk strings (arguments) in RESP format
            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    // Expecting a bulk string length, skip over it to the next line
                    if let Some(arg) = lines.next() {
                        if command.is_none() {
                            // First argument is the command
                            command = Some(arg.to_uppercase());
                        } else {
                            // Subsequent arguments
                            args.push(arg.to_string());
                        }
                    }
                }
            }

            // Ensure the number of arguments matches the declared count
            if args.len() + 1 != arg_count {
                return (command, args, "-ERR argument count mismatch\r\n".to_string());
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

            return (command, args, response);  // Return parsed command, args, and response
        } else {
            return (None, vec![], "-ERR invalid format\r\n".to_string());
        }
    } else {
        return (None, vec![], "-ERR empty message\r\n".to_string());
    }
}
