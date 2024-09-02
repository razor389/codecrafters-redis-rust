use std::collections::HashMap;
use std::time::{SystemTime, Duration};

#[derive(Debug)]
pub struct RedisValue {
    value: String,
    creation_time: SystemTime,
    ttl: Option<Duration>,
}

impl RedisValue {
    fn new(value: String, ttl: Option<u64>) -> Self {
        let ttl_duration = ttl.map(Duration::from_millis);
        RedisValue {
            value,
            creation_time: SystemTime::now(),
            ttl: ttl_duration,
        }
    }

    fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            self.creation_time.elapsed().unwrap_or(Duration::from_secs(0)) > ttl
        } else {
            false
        }
    }
}

pub fn parse_redis_message(
    message: &str,
    hashmap: &mut HashMap<String, RedisValue>,
    config_map: &HashMap<String, String>,
) -> String {
    let mut lines = message.lines();

    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            let mut command = None;
            let mut args = Vec::<String>::new();

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
                Some("PING") => "+PONG\r\n".to_string(),
                Some("ECHO") => {
                    if let Some(echo_message) = args.get(0) {
                        format!("${}\r\n{}\r\n", echo_message.len(), echo_message)
                    } else {
                        "-ERR missing argument\r\n".to_string()
                    }
                },
                Some("SET") => {
                    if args.len() == 2 {
                        hashmap.insert(args[0].clone(), RedisValue::new(args[1].clone(), None));
                        "+OK\r\n".to_string()
                    } else if args.len() == 4 {
                        if args[2].to_uppercase() == "PX" {
                            let ttl = args[3].parse::<u64>().unwrap();
                            hashmap.insert(args[0].clone(), RedisValue::new(args[1].clone(), Some(ttl)));
                            "+OK\r\n".to_string()
                        } else {
                            "-ERR syntax error\r\n".to_string()
                        }
                    } else {
                        "-ERR wrong number of arguments for 'set' command\r\n".to_string()
                    }
                },
                Some("GET") => {
                    if args.len() == 1 {
                        match hashmap.get(&args[0]) {
                            Some(redis_value) => {
                                if redis_value.is_expired() {
                                    hashmap.remove(&args[0]);
                                    "$-1\r\n".to_string()
                                } else {
                                    format!("${}\r\n{}\r\n", redis_value.value.len(), redis_value.value)
                                }
                            }
                            None => "$-1\r\n".to_string(),
                        }
                    } else {
                        "-ERR wrong number of arguments for 'get' command\r\n".to_string()
                    }
                },
                Some("CONFIG") => {
                    if args.len() == 2 && args[0].to_uppercase() == "GET" {
                        if let Some(value) = config_map.get(&args[1]) {
                            format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", args[1].len(), args[1], value.len(), value)
                        } else {
                            "$-1\r\n".to_string()
                        }
                    } else {
                        "-ERR syntax error\r\n".to_string()
                    }
                },
                _ => "-ERR unknown command\r\n".to_string(),
            }
        } else {
            "-ERR invalid format\r\n".to_string()
        }
    } else {
        "-ERR empty message\r\n".to_string()
    }
}
