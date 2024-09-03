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

    let mut cursor = 0;
    let mut current_ttl: Option<u64> = None;

    // Validate header
    let header = &buffer[0..9];
    let header_str = String::from_utf8_lossy(header);
    if &header_str != "REDIS0003" {
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
                if expire_seconds > now_seconds {
                    let ttl_millis = (expire_seconds - now_seconds) * 1000; // Convert to milliseconds
                    println!("Debug: Calculated TTL in milliseconds: {}", ttl_millis);
                    current_ttl = Some(ttl_millis); // Store as u64
                } else {
                    println!("Debug: Key has already expired (seconds).");
                    current_ttl = None;  // Already expired, set to None
                }
            },
            0xFC => { // Expiration timestamp in milliseconds
                println!("Debug: Found key with expiration timestamp in milliseconds.");
                let expire_milliseconds = read_uint_le(&buffer, &mut cursor, 8)?;
                let now_millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64;
                if expire_milliseconds > now_millis {
                    let ttl_millis = expire_milliseconds - now_millis;
                    println!("Debug: Calculated TTL in milliseconds: {}", ttl_millis);
                    current_ttl = Some(ttl_millis); // Store as u64
                } else {
                    println!("Debug: Key has already expired (milliseconds).");
                    current_ttl = None;  // Already expired, set to None
                }
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