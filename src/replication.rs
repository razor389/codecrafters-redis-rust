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
    let mut buffer = vec![0; 512];  // Buffer to store incoming data
    let mut partial_message = String::new();
    let mut expecting_rdb = false;  // Track whether we are expecting an RDB file
    let mut rdb_bulk_len = 0;       // Length of the RDB file to receive

    loop {
        // Read from the master
        let bytes_read = stream.read(&mut buffer).await?;

        if bytes_read == 0 {
            // Master has closed the connection
            println!("Connection closed by master.");
            break;
        }

        // Convert the response into a string to handle the command
        let message = String::from_utf8_lossy(&buffer[..bytes_read]);
        partial_message.push_str(&message);

        // Handle FULLRESYNC and RDB file
        if !expecting_rdb && partial_message.starts_with("+FULLRESYNC") {
            if let Some(length) = parse_bulk_length(&partial_message) {
                rdb_bulk_len = length;
                expecting_rdb = true;
                println!("Expecting RDB file of length: {}", rdb_bulk_len);
            }
            partial_message.clear();  // Clear message after detecting FULLRESYNC
        }

        // Handle RDB bulk string (starts with '$')
        if expecting_rdb {
            if let Some(rdb_data) = receive_bulk_string(stream, rdb_bulk_len).await? {
                println!("Received RDB file, length: {}", rdb_data.len());
                expecting_rdb = false;  // Finished receiving the RDB file
                partial_message.clear();  // Clear the buffer
                continue;  // Go to the next iteration to handle commands
            }
        }

        // Handle "+OK\r\n" to send the PSYNC command
        else if partial_message == "+OK\r\n" {
            println!("Received +OK from master. Sending PSYNC command...");
            stream
                .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                .await?;
            partial_message.clear(); // Clear after handling the +OK
        }

        // Handle and process individual Redis commands
        while let Some(position) = partial_message.find("\r\n") {
            // Extract one complete command from partial_message
            let command = partial_message[..position + 2].to_string(); // Create a copy of the command
            partial_message = partial_message[position + 2..].to_string(); // Reassign the remaining part

            // If we have a complete Redis command, parse it
            if let Ok(mut db_lock) = db.try_lock() {
                let (command, args, _) = parse_redis_message(&command, &mut db_lock, config_map);

                // Log the command and response for debugging purposes
                println!("Parsed command: {:?}, Args: {:?}", command, args);

                // Apply SET command to the slave's local database
                if let Some(cmd) = command {
                    if cmd == "SET" && args.len() == 2 {
                        let key = args[0].clone();  // Access key
                        let value = args[1].clone();  // Access value
                        db_lock.insert(key, RedisValue::new(value, None));  // Insert into database
                        println!("Applied SET command from master: {} = {}", args[0], args[1]);
                    }
                }
            } else {
                eprintln!("Failed to acquire lock for command processing.");
            }
        }
    }

    Ok(())
}

// Helper function to parse bulk string length from the RESP message
fn parse_bulk_length(message: &str) -> Option<usize> {
    if message.starts_with('$') {
        if let Some(newline_idx) = message.find("\r\n") {
            if let Ok(len) = message[1..newline_idx].parse::<usize>() {
                return Some(len);
            }
        }
    }
    None
}

// Helper function to receive bulk string data (such as the RDB file)
async fn receive_bulk_string(
    stream: &mut TcpStream,
    length: usize
) -> io::Result<Option<String>> {
    let mut data = vec![0; length];
    stream.read_exact(&mut data).await?;
    Ok(Some(String::from_utf8_lossy(&data).to_string()))
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
