use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use crate::database::RedisDatabase;
use crate::commands::process_commands_after_rdb;
use crate::database::ReplicationInfoValue;

// Sends REPLCONF commands to the master after receiving the PING response
pub async fn send_replconf(
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
    stream.write_all(replconf_listening_port.as_bytes()).await?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    println!("Sent REPLCONF capa eof capa psync2");

    let mut buffer = vec![0; 512];
    let bytes_read = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);

    if response.contains("+OK") {
        println!("Received +OK from master. Waiting for more commands...");
        listen_for_master_commands(stream, db, config_map).await?;
    } else {
        println!("Unexpected response from master: {}", response);
    }

    Ok(())
}

// Listens for further commands from the master after REPLCONF
pub async fn listen_for_master_commands(
    stream: &mut TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    let mut buffer = vec![0; 512];
    let mut partial_message = Vec::new();
    let mut received_rdb = false;
    #[allow(unused_assignments)]
    let mut remaining_bulk_bytes = 0;

    while let Ok(bytes_read) = stream.read(&mut buffer).await {
        if bytes_read == 0 && received_rdb {
            println!("Connection closed by master.");
            break;
        }

        partial_message.extend_from_slice(&buffer[..bytes_read]);

        // Handle "+OK\r\n" as text
        if let Ok(message_str) = std::str::from_utf8(&partial_message) {
            if message_str == "+OK\r\n" {
                println!("Received +OK from master. Sending PSYNC command...");
                stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
                partial_message.clear();
                continue;
            }
        }

        // Handle FULLRESYNC
        if let Some(fullresync_pos) = partial_message.windows(11).position(|w| w == b"+FULLRESYNC") {
            let fullresync_end = partial_message.windows(2).position(|w| w == b"\r\n").unwrap_or(partial_message.len());
            let fullresync_message = &partial_message[fullresync_pos..fullresync_end + 2];

            if let Ok(fullresync_str) = std::str::from_utf8(fullresync_message) {
                if let Some((replid, offset)) = parse_fullresync(fullresync_str) {
                    let mut db_lock = db.lock().await;
                    db_lock.replication_info.insert("master_replid".to_string(), ReplicationInfoValue::StringValue(replid.clone()));
                    db_lock.replication_info.insert("master_repl_offset".to_string(), ReplicationInfoValue::StringValue(offset.clone()));
                    partial_message.drain(..fullresync_end + 2);
                }
            }
        }

        // Handle RDB file parsing (bulk string)
        if !received_rdb && partial_message.starts_with(b"$") {
            if let Some(bulk_length) = parse_bulk_length(&partial_message) {
                let header_size = partial_message.windows(2).position(|w| w == b"\r\n").unwrap() + 2;

                // Drain the header bytes
                partial_message.drain(..header_size);
                remaining_bulk_bytes = bulk_length;

                // Read the entire bulk string (RDB file)
                while partial_message.len() < remaining_bulk_bytes {
                    let bytes_read = stream.read(&mut buffer).await?;
                    if bytes_read == 0 {
                        println!("No bytes read from master when waiting on RDB file. Breaking.");
                        return Ok(());
                    }
                    partial_message.extend_from_slice(&buffer[..bytes_read]);
                }

                partial_message.drain(..remaining_bulk_bytes);
                received_rdb = true;
                println!("RDB file fully received and processed.");
            }
        }

        // Process Redis commands after RDB has been received
        if received_rdb {
            if let Ok(partial_str) = std::str::from_utf8(&partial_message) {
                if !partial_str.is_empty() {
                    println!("Processing command in replication: {}", partial_str);
                    process_commands_after_rdb(&mut partial_str.to_string(), db.clone(), config_map, stream).await?;

                    // Clear the processed part of the message
                    partial_message.clear();
                }
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
fn parse_bulk_length(message: &[u8]) -> Option<usize> {
    if message.starts_with(b"$") {
        let newline_pos = message.windows(2).position(|w| w == b"\r\n")?;
        let bulk_length_str = std::str::from_utf8(&message[1..newline_pos]).ok()?;
        bulk_length_str.trim().parse::<usize>().ok()
    } else {
        None
    }
}

// Initializes replication settings, determining whether this server is a master or slave
pub async fn initialize_replication(
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
            let mut db_lock = db.lock().await;
            db_lock.replication_info.insert("role".to_string(), ReplicationInfoValue::StringValue("slave".to_string()));
            println!("Replication info updated to 'slave'.");
        }

        match TcpStream::connect(address.clone()).await {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);
                let _ = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await;
                let _ = stream.set_nodelay(true);

                let mut buffer = vec![0; 512];
                match stream.read(&mut buffer).await {
                    Ok(_) => {
                        println!("Received PING response from master");
                        let _ = send_replconf(&mut stream, port, db.clone(), config_map).await;
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        let mut db_lock = db.lock().await;
        db_lock.replication_info.insert("role".to_string(), ReplicationInfoValue::StringValue("master".to_string()));
        db_lock.replication_info.insert("master_replid".to_string(), ReplicationInfoValue::StringValue("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()));
        if !db_lock.replication_info.contains_key("master_repl_offset") {
            db_lock.replication_info.insert("master_repl_offset".to_string(), ReplicationInfoValue::ByteValue(0));
        }
        println!("Running as master.");
    }
}
