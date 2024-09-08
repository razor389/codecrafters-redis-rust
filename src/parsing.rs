// src/parsing.rs
use crate::database::RedisDatabase;
use crate::commands::{handle_set, handle_get, handle_config, handle_keys, handle_echo, handle_ping, handle_info, handle_replconf, handle_psync};
use std::collections::HashMap;

pub fn parse_redis_message(
    message: &str,
    db: &mut RedisDatabase,
    config_map: &HashMap<String, String>,
) -> (Option<String>, String) {
    let mut lines = message.lines();

    if let Some(line) = lines.next() {
        // Handle simple "+OK" status reply
        if line == "+OK" {
            println!("Received +OK status reply.");
            return (Some("OK".to_string()), "+OK\r\n".to_string());
        }
        
        if line.starts_with('*') {
            let mut command = None;
            let mut args = Vec::new();
            let mut arg_count = 0;

            // Parse the number of arguments
            if let Ok(count) = line[1..].parse::<usize>() {
                arg_count = count;
            }

            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    // The next line contains the argument value
                    if let Some(arg) = lines.next() {
                        if command.is_none() {
                            command = Some(arg.to_uppercase()); // Command should be the first argument
                        } else {
                            args.push(arg.to_string()); // Add subsequent arguments to args
                        }
                    }
                }
            }

            // Debugging output for parsed command and arguments
            println!("Parsed command: {:?}, Args: {:?}", command, args);

            // Ensure the number of collected args matches the declared arg count
            if args.len() + 1 != arg_count {
                return (command, "-ERR argument count mismatch\r\n".to_string());
            }

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

            return (command, response);
        } else {
            return (None, "-ERR invalid format\r\n".to_string());
        }
    } else {
        return (None, "-ERR empty message\r\n".to_string());
    }
}
