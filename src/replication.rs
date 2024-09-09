use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::database::{RedisDatabase, RedisValue};
use crate::parsing::parse_redis_message;

// Sends REPLCONF commands to the master after receiving the PING response
pub async fn send_replconf(
    stream: &mut TcpStream,
    port: &str,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    // Send REPLCONF listening-port with the correct port
    let replconf_listening_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    );
    stream.write_all(replconf_listening_port.as_bytes()).await?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    // Send REPLCONF capa eof capa psync2
    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    println!("Sent REPLCONF capa eof capa psync2");

    // Wait for +OK response from the master
    let mut buffer = vec![0; 512];
    let bytes_read = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);

    if response.contains("+OK") {
        println!("Received +OK from master. Waiting for more commands...");
        // Keep listening for further commands from the master
        listen_for_master_commands(stream, db, config_map).await?;
    } else {
        println!("Unexpected response from master: {}", response);
    }

    Ok(())
}

pub async fn listen_for_master_commands(
    stream: &mut TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    let mut buffer = vec![0; 512];
    let mut partial_message = String::new();
    let mut received_rdb = false;
    let mut rdb_bulk_len = 0;

    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            println!("Connection closed by master.");
            break;
        }

        let message = String::from_utf8_lossy(&buffer[..bytes_read]);
        partial_message.push_str(&message);

        // Handle "+OK\r\n" to send the PSYNC command
        if partial_message == "+OK\r\n" {
            println!("Received +OK from master. Sending PSYNC command...");
            stream
                .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                .await?;
            partial_message.clear(); // Clear after handling the +OK
            continue;
        }

        // Handle FULLRESYNC
        if partial_message.starts_with("+FULLRESYNC") {
            if let Some((replid, offset)) = parse_fullresync(&partial_message) {
                let mut db_lock = db.lock().await;
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
                rdb_bulk_len = length;
                received_rdb = true;
                partial_message.clear();  // Clear the bulk length header but keep the rest
            }
        }

        if received_rdb {
            // Process commands after RDB file reception
            println!("Partial message: {}", partial_message);

            // Process each RESP message in the buffer
            while let Some((command, args, response)) = process_complete_redis_message(&mut partial_message, &db, config_map).await {
                if let Some(cmd) = command {
                    if cmd == "SET" && args.len() >= 2 {
                        let key = args[0].clone();
                        let value = args[1].clone();
                        db.lock().await.insert(key.clone(), RedisValue::new(value.clone(), None));  // Insert cloned key and value
                        println!("Applied SET command: {} = {}", key, value);
                    }
                }

                // If there's a response that needs to be sent, write it to the master
                if !response.is_empty() {
                    stream.write_all(response.as_bytes()).await?;
                }
            }
        }
    }
    Ok(())
}

// Helper function to process complete RESP messages
async fn process_complete_redis_message(
    partial_message: &mut String,
    db: &Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> Option<(Option<String>, Vec<String>, String)> {
    let mut db_lock = db.lock().await;

    // Check if there is a complete Redis message to process
    if let (command, args, response) = parse_redis_message(partial_message, &mut db_lock, config_map) {
        // If command is None, there wasn't a complete message
        if command.is_some() {
            // Drain the processed part of the partial message
            *partial_message = partial_message[response.len()..].to_string();
            return Some((command, args, response));
        }
    }
    
    None
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
    // We expect the bulk string to start with '$' followed by the length
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
pub async fn initialize_replication(
    config_map: &HashMap<String, String>,
    db: Arc<Mutex<RedisDatabase>>,
    port: &str,
) {
    // Acquire the lock only to update the replication info, then release it
    if let Some(replicaof) = config_map.get("replicaof") {
        let replicaof_parts: Vec<&str> = replicaof.split(' ').collect();
        let ip = replicaof_parts[0];
        let replica_port = replicaof_parts[1];
        let address = format!("{}:{}", ip, replica_port);

        {
            // Lock the database to set replication info as "slave"
            let mut db_lock = db.lock().await;
            db_lock.replication_info.insert("role".to_string(), "slave".to_string());
            println!("Replication info updated to 'slave'.");
        } // Lock is released here

        // Connect to the master and initiate the replication handshake
        match TcpStream::connect(address.clone()).await {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);

                // Send PING to the master
                stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                stream.set_nodelay(true).unwrap();

                let mut buffer = vec![0; 512];
                match stream.read(&mut buffer).await {
                    Ok(_) => {
                        println!("Received PING response from master");
                        // Send REPLCONF commands, using the dynamically provided port
                        let _ = send_replconf(&mut stream, port, db.clone(), config_map).await;
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        // If no replicaof is present, the server acts as a master
        {
            let mut db_lock = db.lock().await;
            db_lock.replication_info.insert("role".to_string(), "master".to_string());
            db_lock.replication_info.insert("master_replid".to_string(), "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
            db_lock.replication_info.insert("master_repl_offset".to_string(), "0".to_string());
            println!("Running as master.");
        } // Lock is released here
    }
}
