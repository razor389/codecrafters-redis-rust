use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::database::{RedisDatabase, RedisValue};
use crate::parsing::parse_redis_message;

// Sends REPLCONF commands to the master after receiving the PING response
pub fn send_replconf(
    stream: &mut TcpStream,
    port: &str,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    let replconf_listening_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    );
    stream.write_all(replconf_listening_port.as_bytes())?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")?;
    println!("Sent REPLCONF capa eof capa psync2");

    let mut buffer = vec![0; 512];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);

    if response.contains("+OK") {
        println!("Received +OK from master. Waiting for more commands...");
        listen_for_master_commands(stream, db, config_map)?;
    } else {
        println!("Unexpected response from master: {}", response);
    }

    Ok(())
}

// Listens for further commands from the master after REPLCONF
pub fn listen_for_master_commands(
    stream: &mut TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    let mut buffer = vec![0; 512];
    let mut partial_message = String::new();
    let mut received_rdb = false;

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            println!("Connection closed by master.");
            break;
        }

        let message = String::from_utf8_lossy(&buffer[..bytes_read]);
        partial_message.push_str(&message);

        // Handle "+OK\r\n" to send the PSYNC command
        if partial_message == "+OK\r\n" {
            println!("Received +OK from master. Sending PSYNC command...");
            stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
            partial_message.clear(); // Clear after handling the +OK
            continue;
        }

        // Handle FULLRESYNC
        if partial_message.starts_with("+FULLRESYNC") {
            if let Some((replid, offset)) = parse_fullresync(&partial_message) {
                let mut db_lock = db.lock().unwrap();
                db_lock.replication_info.insert("master_replid".to_string(), replid.clone());
                db_lock.replication_info.insert("master_repl_offset".to_string(), offset.clone());
                println!("Handled FULLRESYNC: replid = {}, offset = {}", replid, offset);
                partial_message.clear();  // Clear just the FULLRESYNC part
            }
        }

        // Handle RDB file parsing
        if partial_message.starts_with('$') && !received_rdb {
            println!("Received RDB File: {}", partial_message);
            if let Some(length) = parse_bulk_length(&partial_message) {
                received_rdb = true;
                partial_message.clear();  // Clear the bulk length header but keep the rest
            }
        }

        // Process Redis commands after RDB has been received
        if received_rdb {
            process_commands_after_rdb(&mut partial_message, db.clone(), config_map, stream)?;
        }
    }
    Ok(())
}

// Processes Redis commands after the RDB file has been received
fn process_commands_after_rdb(
    partial_message: &mut String,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
    stream: &mut TcpStream,  // Added to send a response back to master
) -> io::Result<()> {
    let mut db_lock = db.lock().unwrap();

    // Parse the Redis message and handle the parsed commands
    let parsed_results = parse_redis_message(&partial_message, &mut db_lock, config_map);

    for (command, args, response, consumed_length) in parsed_results {
        // Remove the processed part from the partial message
        partial_message.drain(..consumed_length);
        println!("partial_message after drain: {}", partial_message);

        if let Some(cmd) = command {
            match cmd.as_str() {
                "SET" => {
                    if args.len() >= 2 {
                        let key = args[0].clone();
                        let value = args[1].clone();
                        db_lock.insert(key.clone(), RedisValue::new(value.clone(), None));
                        println!("Applied SET command: {} = {}", key, value);
                    }
                },
                "REPLCONF" => {
                    // Handle REPLCONF commands
                    println!("REPLCONF command received, responding with: {}", response);
                    stream.write_all(response.as_bytes())?;
                },
                // Handle other commands as needed
                _ => println!("Unknown command: {}", cmd),
            }
        }
    }

    Ok(())
}


// Helper function to parse the FULLRESYNC command and extract replid and offset
fn parse_fullresync(message: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = message.split_whitespace().collect();
    if parts.len() >= 3 {
        let replid = parts[1].to_string();
        let offset = parts[2].to_string();
        Some((replid, offset))
    } else {
        None
    }
}

// Helper function to parse bulk length from the Redis message
fn parse_bulk_length(message: &str) -> Option<usize> {
    println!("message: {:?}", message);
    if message.starts_with('$') {
        let parts: Vec<&str> = message.split("\r\n").collect();
        if parts.len() > 1 {
            if let Ok(length) = parts[0][1..].parse::<usize>() {
                return Some(length);
            }
        }
    }
    None
}

// Initializes replication settings, determining whether this server is a master or slave
pub fn initialize_replication(
    config_map: &HashMap<String, String>,
    db: Arc<Mutex<RedisDatabase>>,
    port: &str,
) {
    if let Some(replicaof) = config_map.get("replicaof") {
        let replicaof_parts: Vec<&str> = replicaof.split(' ').collect();
        let ip = replicaof_parts[0];
        let replica_port = replicaof_parts[1];
        let address = format!("{}:{}", ip, replica_port);

        {
            let mut db_lock = db.lock().unwrap();
            db_lock.replication_info.insert("role".to_string(), "slave".to_string());
            println!("Replication info updated to 'slave'.");
        }

        match TcpStream::connect(address.clone()) {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);
                stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
                stream.set_nodelay(true).unwrap();

                let mut buffer = vec![0; 512];
                match stream.read(&mut buffer) {
                    Ok(_) => {
                        println!("Received PING response from master");
                        let _ = send_replconf(&mut stream, port, db.clone(), config_map);
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        let mut db_lock = db.lock().unwrap();
        db_lock.replication_info.insert("role".to_string(), "master".to_string());
        db_lock.replication_info.insert("master_replid".to_string(), "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
        db_lock.replication_info.insert("master_repl_offset".to_string(), "0".to_string());
        println!("Running as master.");
    }
}
