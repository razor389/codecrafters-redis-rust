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

    // Print buffer byte by byte in hexadecimal format
    for (i, byte) in buffer.iter().enumerate() {
        print!("{:02X} ", byte);
        if (i + 1) % 16 == 0 {
            println!(); // New line every 16 bytes for readability
        }
    }
    println!(); // Ensure the last line ends properly

    println!("RDB file read successfully. Size: {} bytes", buffer.len());
    
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

    // Helper to read an n-byte little-endian integer
    fn read_uint_le(buffer: &[u8], cursor: &mut usize, n: usize) -> io::Result<u64> {
        if *cursor + n <= buffer.len() {
            let mut value = 0u64;
            for i in 0..n {
                value |= (buffer[*cursor + i] as u64) << (i * 8);
            }
            *cursor += n;
            Ok(value)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reached end of buffer"))
        }
    }

    // Helper to decode the size-encoded values
    fn decode_size(buffer: &[u8], cursor: &mut usize) -> io::Result<u64> {
        let first_byte = read_u8(buffer, cursor)?;
        println!("Decoding size: first byte = {:02X}", first_byte);

        let size = match first_byte >> 6 {
            0b00 => {
                let size = u64::from(first_byte & 0x3F);
                println!("Size encoded with 6 bits: {}", size);
                size
            }
            0b01 => {
                let second_byte = read_u8(buffer, cursor)?;
                let size = u64::from(first_byte & 0x3F) << 8 | u64::from(second_byte);
                println!("Size encoded with 14 bits: {}", size);
                size
            }
            0b10 => {
                let size = read_uint_le(buffer, cursor, 4)?;
                println!("Size encoded with 32 bits: {}", size);
                size
            }
            0b11 => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected string encoding type")),
            _ => unreachable!(),
        };
        Ok(size)
    }

    // Helper to read a string-encoded value
    fn read_string(buffer: &[u8], cursor: &mut usize) -> io::Result<String> {
        let first_byte = read_u8(buffer, cursor)?;
    
        // Check if the size encoding indicates an integer or compressed data
        if (first_byte & 0xC0) == 0xC0 {
            match first_byte {
                0xC0 => {
                    // 8-bit integer
                    let value = read_u8(buffer, cursor)?;
                    println!("Read 8-bit integer: {}", value);
                    return Ok(value.to_string());
                }
                0xC1 => {
                    // 16-bit integer (little-endian)
                    let value = read_uint_le(buffer, cursor, 2)?;
                    println!("Read 16-bit integer: {}", value);
                    return Ok(value.to_string());
                }
                0xC2 => {
                    // 32-bit integer (little-endian)
                    let value = read_uint_le(buffer, cursor, 4)?;
                    println!("Read 32-bit integer: {}", value);
                    return Ok(value.to_string());
                }
                0xC3 => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "LZF compressed strings are not supported"));
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown string encoding type"));
                }
            }
        } else {
            // Handle regular size-encoded strings
            let size = match first_byte >> 6 {
                0b00 => u64::from(first_byte & 0x3F),
                0b01 => {
                    let second_byte = read_u8(buffer, cursor)?;
                    u64::from(first_byte & 0x3F) << 8 | u64::from(second_byte)
                }
                0b10 => read_uint_le(buffer, cursor, 4)?,
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected string encoding type")),
            };
    
            println!("String size: {}", size);
    
            let string_bytes = &buffer[*cursor..*cursor + size as usize];
            *cursor += size as usize;
            let result = String::from_utf8_lossy(string_bytes).into_owned();
            println!("Read string: {}", result);
            Ok(result)
        }
    }
    

    // Step 1: Read and validate the header
    let header = &buffer[0..9];
    let header_str = String::from_utf8_lossy(header);
    println!("Header: {}", header_str);

    if &header_str != "REDIS0003" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB file header"));
    }
    cursor += 9;

    // Step 2: Handle metadata sections
    while let Ok(byte) = read_u8(&buffer, &mut cursor) {
        if byte == 0xFA {
            // Start of metadata subsection
            let meta_key = read_string(&buffer, &mut cursor)?;
            let meta_value = read_string(&buffer, &mut cursor)?;
            println!("Metadata: {} -> {}", meta_key, meta_value);
        } else {
            // Stop reading metadata when a non-metadata byte is encountered
            cursor -= 1; // Move back one byte
            break;
        }
    }

    // Iterate over the data
    while let Ok(byte) = read_u8(&buffer, &mut cursor) {
        println!("Processing byte: {:02X} at cursor {}", byte, cursor - 1);
        match byte {
            0xFD => {
                // Expiration timestamp in seconds
                let expire_timestamp = read_uint_le(&buffer, &mut cursor, 4)?;
                println!("Expiration timestamp (seconds): {}", expire_timestamp);
            }
            0xFC => {
                // Expiration timestamp in milliseconds
                let expire_timestamp = read_uint_le(&buffer, &mut cursor, 8)?;
                println!("Expiration timestamp (milliseconds): {}", expire_timestamp);
            }
            0x00 | 0x01 | 0x02 | 0x03 => {
                // Value type (0 = string, other values may represent other types)
                println!("Value type: {:02X}", byte);
                let key = read_string(&buffer, &mut cursor)?;
                let value = read_string(&buffer, &mut cursor)?;
                println!("Parsed key-value pair: {} -> {}", key, value);
                keys.push(key);
            }
            0xFF => {
                // End of file section
                println!("End of file detected.");
                // Read and skip the 8-byte checksum
                cursor += 8;
                break;
            }
            _ => {
                println!("Unknown byte detected: {:02X}", byte);
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

