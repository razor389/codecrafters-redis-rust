use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::database::RedisDatabase;
use crate::parsing::parse_redis_message;

// Sends REPLCONF commands to the master after receiving the PING response
fn send_replconf(stream: &mut TcpStream, port: &str, db: Arc<Mutex<RedisDatabase>>, config_map: &HashMap<String, String>) -> io::Result<()> {
    // Send REPLCONF listening-port with the correct port
    let replconf_listening_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    );
    stream.write_all(replconf_listening_port.as_bytes())?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    // Send REPLCONF capa eof capa psync2
    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")?;
    println!("Sent REPLCONF capa eof capa psync2");

    // Wait for +OK response from the master
    let mut buffer = [0; 512];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);
    
    if response.contains("+OK") {
        println!("Received +OK from master, proceeding with PSYNC");

        // Send PSYNC command to the master after receiving +OK
        stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
        println!("Sent PSYNC command");

        // Keep listening for further commands from the master and pass the db for command execution
        listen_for_master_commands(stream, db, config_map)?;
    } else {
        println!("Unexpected response from master: {}", response);
    }

    Ok(())
}

// Listens for and processes commands sent by the master
// src/rdb_parser.rs
use std::fs;
use std::io::{self, Read};
use std::time::{Duration, SystemTime};
use crate::database::{RedisDatabase, RedisValue};

fn read_u8(buffer: &[u8], cursor: &mut usize) -> io::Result<u8> {
    if *cursor < buffer.len() {
        let byte = buffer[*cursor];
        *cursor += 1;
        Ok(byte)
    } else {
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reached end of buffer"))
    }
}

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

fn decode_size(buffer: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let first_byte = read_u8(buffer, cursor)?;
    let size = match first_byte >> 6 {
        0b00 => u64::from(first_byte & 0x3F),
        0b01 => {
            let second_byte = read_u8(buffer, cursor)?;
            u64::from(first_byte & 0x3F) << 8 | u64::from(second_byte)
        },
        0b10 => read_uint_le(buffer, cursor, 4)?,
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected string encoding type")),
    };
    Ok(size)
}

fn read_string(buffer: &[u8], cursor: &mut usize) -> io::Result<String> {
    let first_byte = read_u8(buffer, cursor)?;

    if (first_byte & 0xC0) == 0xC0 {
        match first_byte {
            0xC0 => {
                let value = read_u8(buffer, cursor)?;
                Ok(value.to_string())
            },
            0xC1 => {
                let value = read_uint_le(buffer, cursor, 2)?;
                Ok(value.to_string())
            },
            0xC2 => {
                let value = read_uint_le(buffer, cursor, 4)?;
                Ok(value.to_string())
            },
            0xC3 => Err(io::Error::new(io::ErrorKind::InvalidData, "LZF compressed strings are not supported")),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown string encoding type")),
        }
    } else {
        let size = match first_byte >> 6 {
            0b00 => u64::from(first_byte & 0x3F),
            0b01 => {
                let second_byte = read_u8(buffer, cursor)?;
                u64::from(first_byte & 0x3F) << 8 | u64::from(second_byte)
            },
            0b10 => read_uint_le(buffer, cursor, 4)?,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected string encoding type")),
        };

        let string_bytes = &buffer[*cursor..*cursor + size as usize];
        *cursor += size as usize;
        Ok(String::from_utf8_lossy(string_bytes).into_owned())
    }
}


pub fn parse_rdb_file(file_path: &str, db: &mut RedisDatabase) -> io::Result<()> {
    let mut file = fs::File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    println!("File content in bytes:");
    for (i, byte) in buffer.iter().enumerate() {
        // Print the byte as a hexadecimal value along with its index
        print!("{:02X} ", byte);
        if (i + 1) % 16 == 0 {
            println!(); // New line every 16 bytes for readability
        }
    }
    println!(); // Final newline after the last line


    let mut cursor = 0;
    let mut current_ttl: Option<u64> = None;

    // Validate header
    let header = &buffer[0..5]; // Only check the first 5 bytes ("REDIS")
    let header_str = String::from_utf8_lossy(header);
    if header_str != "REDIS" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB file header"));
    }
    cursor += 9;

    // Handle metadata sections
    while let Ok(byte) = read_u8(&buffer, &mut cursor) {
        if byte == 0xFA {
            let _meta_key = read_string(&buffer, &mut cursor)?;
            let _meta_value = read_string(&buffer, &mut cursor)?;
        } else {
            cursor -= 1;
            break;
        }
    }

    // Parse key-value pairs with TTL
    while let Ok(byte) = read_u8(&buffer, &mut cursor) {
        match byte {
            0xFD => { // Expiration timestamp in seconds
                println!("Debug: Found key with expiration timestamp in seconds.");
                let expire_seconds = read_uint_le(&buffer, &mut cursor, 4)?;
                let now_seconds = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs();
                println!("Debug: Current time in seconds: {}", now_seconds);
                println!("Debug: Expiration time in seconds: {}", expire_seconds);
                current_ttl = if expire_seconds > now_seconds {
                    Some((expire_seconds - now_seconds) * 1000) // Convert to milliseconds
                } else {
                    Some(0)  // Already expired
                };
            },
            0xFC => { // Expiration timestamp in milliseconds
                println!("Debug: Found key with expiration timestamp in milliseconds.");
                let expire_milliseconds = read_uint_le(&buffer, &mut cursor, 8)?;
                let now_millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64;
                println!("Debug: Current time in milliseconds: {}", now_millis);
                current_ttl = if expire_milliseconds > now_millis {
                    Some(expire_milliseconds - now_millis)
                } else {
                    Some(0)  // Already expired
                };
            },
            0xFE => { decode_size(&buffer, &mut cursor)?; }, // Start of database subsection
            0xFB => {
                decode_size(&buffer, &mut cursor)?; // Key hash table size
                decode_size(&buffer, &mut cursor)?; // Expire hash table size
            },
            0x00 | 0x01 | 0x02 | 0x03 => {
                let key = read_string(&buffer, &mut cursor)?;
                let value = read_string(&buffer, &mut cursor)?;
                println!("Debug: Inserting key-value pair. Key: {}, Value: {}, TTL: {:?}", key, value, current_ttl);
                db.insert(key, RedisValue::new(value, current_ttl)); // Insert with TTL in milliseconds
                current_ttl = None; // Reset TTL after insertion
            },
            0xFF => { break; }, // End of file section
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown byte in RDB file"));
            }
        }
    }
    
    Ok(())
}
// Initializes replication settings, determining whether this server is a master or slave
pub fn initialize_replication(config_map: &HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>, port: &str) {
    // Acquire the lock only to update the replication info, then release it
    if let Some(replicaof) = config_map.get("replicaof") {
        let replicaof_parts: Vec<&str> = replicaof.split(' ').collect();
        let ip = replicaof_parts[0];
        let replica_port = replicaof_parts[1];
        let address = format!("{}:{}", ip, replica_port);

        {
            // Lock the database to set replication info as "slave"
            let mut db_lock = db.lock().unwrap();
            db_lock.replication_info.insert("role".to_string(), "slave".to_string());
            println!("Replication info updated to 'slave'.");
        } // Lock is released here

        // Connect to the master and initiate the replication handshake
        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);

                // Send PING to the master
                stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap(); // Timeout for PING response

                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(_) => {
                        println!("Received PING response from master");
                        // Send REPLCONF commands and PSYNC, using the dynamically provided port
                        let _ = send_replconf(&mut stream, port, db.clone(), config_map);
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        // If no replicaof is present, the server acts as a master
        {
            let mut db_lock = db.lock().unwrap();
            db_lock.replication_info.insert("role".to_string(), "master".to_string());
            db_lock.replication_info.insert("master_replid".to_string(), "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
            db_lock.replication_info.insert("master_repl_offset".to_string(), "0".to_string());
            println!("Running as master.");
        } // Lock is released here
    }
}

