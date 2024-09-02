use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::time::{SystemTime, Duration};
use std::path::Path;

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

fn parse_rdb_file(file_path: &str) -> io::Result<Vec<String>> {
    let mut file = fs::File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let mut keys = Vec::new();
    let mut cursor = 0;

    // Helper to read a single byte
    fn read_u8(buffer: &[u8], cursor: &mut usize) -> io::Result<u8> {
        if *cursor < buffer.len() {
            let byte = buffer[*cursor];
            *cursor += 1;
            Ok(byte)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reached end of buffer"))
        }
    }

    // Helper to read an 8-byte little-endian integer
    fn read_u64_le(buffer: &[u8], cursor: &mut usize) -> io::Result<u64> {
        if *cursor + 8 <= buffer.len() {
            let mut value = 0u64;
            for i in 0..8 {
                value |= (buffer[*cursor + i] as u64) << (i * 8);
            }
            *cursor += 8;
            Ok(value)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reached end of buffer"))
        }
    }

    // Helper to read a 4-byte little-endian integer
    fn read_u32_le(buffer: &[u8], cursor: &mut usize) -> io::Result<u32> {
        if *cursor + 4 <= buffer.len() {
            let mut value = 0u32;
            for i in 0..4 {
                value |= (buffer[*cursor + i] as u32) << (i * 8);
            }
            *cursor += 4;
            Ok(value)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reached end of buffer"))
        }
    }

    // Iterate over the data
    while let Ok(byte) = read_u8(&buffer, &mut cursor) {
        match byte {
            0xFE => {
                // Start of the database subsection, followed by the database index
                let _db_index = read_u8(&buffer, &mut cursor)?;
            }
            0xFB => {
                // Hash table size information
                let _key_table_size = read_u8(&buffer, &mut cursor)?;
                let _expire_table_size = read_u8(&buffer, &mut cursor)?;
            }
            0x00 => {
                // Value type and encoding (0 = string)
                let key_len = read_u8(&buffer, &mut cursor)? as usize;
                let key = &buffer[cursor..cursor + key_len];
                cursor += key_len;

                let value_len = read_u8(&buffer, &mut cursor)? as usize;
                let _value = &buffer[cursor..cursor + value_len];
                cursor += value_len;

                keys.push(String::from_utf8_lossy(key).into_owned());
            }
            0xFC => {
                // Key with expire in milliseconds
                let _expire_timestamp = read_u64_le(&buffer, &mut cursor)?;
                let _value_type = read_u8(&buffer, &mut cursor)?;

                let key_len = read_u8(&buffer, &mut cursor)? as usize;
                let key = &buffer[cursor..cursor + key_len];
                cursor += key_len;

                let value_len = read_u8(&buffer, &mut cursor)? as usize;
                let _value = &buffer[cursor..cursor + value_len];
                cursor += value_len;

                keys.push(String::from_utf8_lossy(key).into_owned());
            }
            0xFD => {
                // Key with expire in seconds
                let _expire_timestamp = read_u32_le(&buffer, &mut cursor)?;
                let _value_type = read_u8(&buffer, &mut cursor)?;

                let key_len = read_u8(&buffer, &mut cursor)? as usize;
                let key = &buffer[cursor..cursor + key_len];
                cursor += key_len;

                let value_len = read_u8(&buffer, &mut cursor)? as usize;
                let _value = &buffer[cursor..cursor + value_len];
                cursor += value_len;

                keys.push(String::from_utf8_lossy(key).into_owned());
            }
            _ => {
                // Unknown or unhandled byte
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown byte in RDB file"));
            }
        }
    }

    Ok(keys)
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
                Some("KEYS") => {
                    if args.len() == 1 {
                        let db_dir = config_map.get("dir").map(|s| s.as_str()).unwrap_or(".");
                        let db_filename = config_map.get("dbfilename").map(|s| s.as_str()).unwrap_or("dump.rdb");
                        let rdb_path = Path::new(db_dir).join(db_filename);
                
                        if rdb_path.exists() {
                            match parse_rdb_file(rdb_path.to_str().unwrap()) {
                                Ok(keys) => {
                                    let mut response = format!("*{}\r\n", keys.len());
                                    for key in keys {
                                        response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                    }
                                    response
                                }
                                Err(_) => "-ERR failed to parse .rdb file\r\n".to_string(),
                            }
                        } else {
                            "$-1\r\n".to_string()
                        }
                    } else {
                        "-ERR wrong number of arguments for 'keys' command\r\n".to_string()
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

