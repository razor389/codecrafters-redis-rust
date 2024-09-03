// src/parsing.rs
use crate::database::RedisDatabase;
use crate::commands::{handle_set, handle_get, handle_config, handle_keys};
use std::collections::HashMap;

pub fn parse_redis_message(
    message: &str,
    db: &mut RedisDatabase,
    config_map: &HashMap<String, String>,
) -> String {
    let mut lines = message.lines();

    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            let mut command = None;
            let mut args = Vec::new();

            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    if let Some(arg) = lines.next() {
                        if command.is_none() {
                            command = Some(arg.to_uppercase());
                        } else {
                            args.push(arg.to_string());
                        }
                    }
                }
            }

            match command.as_deref() {
                Some("SET") => handle_set(db, &args),
                Some("GET") => handle_get(db, &args),
                Some("CONFIG") => handle_config(config_map, &args),
                Some("KEYS") => handle_keys(db),
                _ => "-ERR unknown command\r\n".to_string(),
            }
        } else {
            "-ERR invalid format\r\n".to_string()
        }
    } else {
        "-ERR empty message\r\n".to_string()
    }
}
