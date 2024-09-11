use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};
use crate::commands::send_rdb_file;
use crate::database::{RedisDatabase, ReplicationInfoValue};
use crate::parsing::parse_redis_message;

pub async fn start_server(config_map: HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let default_port = "6379".to_string();
    let port = config_map.get("port").unwrap_or(&default_port).to_string();
    let address = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(&address).await?;
    println!("Server listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New client connection from {}", addr);

                let db = Arc::clone(&db);
                let config_map = config_map.clone();

                // Spawn a new async task to handle the client connection
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, db, &config_map).await {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> std::io::Result<()> {
    let (mut reader, writer) = stream.into_split(); // Split stream into reader and writer
    let writer = Arc::new(Mutex::new(writer)); // Wrap the TcpStream in an Arc<Mutex>
    let mut buffer = vec![0; 4096];
    let mut partial_message = String::new();

    // Set the connection timeout duration (for example, 30 seconds)
    let connection_timeout = Duration::from_secs(30);

    while let Ok(bytes_read) = timeout(connection_timeout, async {
        reader.read(&mut buffer).await
    })
    .await {
        match bytes_read {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    let db_lock = db.lock().await;
                    if let Some(ReplicationInfoValue::StringValue(value)) = db_lock.get_replication_info("role") {
                        println!("Closing connection for role: {}", value);
                    }
                    println!("Connection closed by client.");
                    return Ok(());
                } else {
                    // Append the newly read data to the partial message buffer
                    partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

                    // Process all complete Redis messages
                    while let Some(message_end) = get_end_of_redis_message(&partial_message) {
                        let current_message = partial_message[..message_end].to_string();
                        println!("Received Redis message in handle client: {}", current_message);

                        let parsed_results = {
                            let mut db_lock = db.lock().await;
                            parse_redis_message(&current_message, &mut db_lock, config_map)
                        };

                        for (command, args, response, _, _) in parsed_results {
                            let replconf_getack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                            let replconf_getack_byte_len = replconf_getack_message.as_bytes().len();
                            
                            if command == Some("WAIT".to_string()) {
                                println!("Got WAIT command");
                                if args.len() != 2 {
                                    let error_response = "-ERR wrong number of arguments for WAIT\r\n";
                                    let mut stream_lock = writer.lock().await;
                                    stream_lock.write_all(error_response.as_bytes()).await?;
                                    stream_lock.flush().await?;
                                } else {
                                    let num_slaves = args[0].parse::<usize>().unwrap_or(0);
                                    println!("num slaves to wait for: {}", num_slaves);
                                    let timeout_ms = args[1].parse::<u64>().unwrap_or(0);
                                    println!("timeout ms: {}", timeout_ms);
                                    {   
                                        println!("waiting to lock db to write getack to slave streams");
                                        let mut db_lock = db.lock().await;
                                        println!("locked db to write getack to slave streams");
                                        // Send REPLCONF GETACK * to all connected slaves
                                        let slaves = {
                                            db_lock.slave_connections.read().await.clone() // Clone slave connections list
                                        };

                                        
                                        if slaves.len() > 0{
                                            if let Some(ReplicationInfoValue::ByteValue(current_offset)) = db_lock.replication_info.get("master_repl_offset") {
                                                let new_offset = current_offset + replconf_getack_byte_len;
                                                db_lock.replication_info.insert(
                                                    "master_repl_offset".to_string(),
                                                    ReplicationInfoValue::ByteValue(new_offset),
                                                );
                                            } else {
                                                db_lock.replication_info.insert(
                                                    "master_repl_offset".to_string(),
                                                    ReplicationInfoValue::ByteValue(replconf_getack_byte_len),
                                                );
                                            }
                                        }
                                        for slave_stream in &slaves {
                                            let mut slave_stream = slave_stream.lock().await;
                                            slave_stream.write_all(replconf_getack_message.as_bytes()).await?;
                                            slave_stream.flush().await?;
                                        }
                                        println!("releasing db lock after getack write");
                                    }
                                    // Wait for REPLCONF ACKs or timeout
                                    let timeout_duration = tokio::time::Duration::from_millis(timeout_ms);
                                    let result = tokio::time::timeout(timeout_duration, async {
                                        loop {
                                            //println!("locking ack counter");
                                            let ack_counter_value = {
                                                // Only lock the ack_counter, not the whole db
                                                let db_lock = db.lock().await;
                                                let ack_counter = db_lock.ack_counter.lock().await;
                                                *ack_counter
                                            };
                                    
                                            //println!("current ack counter: {}", ack_counter_value);
                                            
                                            if ack_counter_value >= num_slaves {
                                                break; // All required slaves have sent REPLCONF ACKs
                                            }
                                    
                                            // Sleep briefly before checking again
                                            tokio::time::sleep(Duration::from_millis(1)).await;
                                        }
                                        println!("finished waiting for ACKs");
                                        Ok::<(), ()>(())
                                    }).await;
                                
                                    let response = {
                                        let db_lock = db.lock().await;
                                        let final_ack_count = *db_lock.ack_counter.lock().await;
                                        println!("final ack count: {}", final_ack_count);
                                        match result {
                                            Ok(_) => format!(":{}\r\n", final_ack_count),
                                            Err(_) => format!(":0\r\n"), // Timeout
                                        }
                                    };

                                    writer.lock().await.write_all(response.as_bytes()).await?;
                                    writer.lock().await.flush().await?;
                                    let db_lock = db.lock().await;
                                    let mut ack_counter_lock = db_lock.ack_counter.lock().await;
                                    *ack_counter_lock = 0;
                                    println!("reset Ack counter to {}", *ack_counter_lock);
                                }
                            } // Detect and handle "REPLCONF ACK" message
                            else if command == Some("REPLCONF".to_string())&& args[0].to_string() == "ACK" {
                                // Increment the ack_counter inside the db
                                println!("got replconf ack, waiting for db lock");
                                let db_lock = db.lock().await;
                                println!("got db lock, waiting for arc counter lock");
                                let mut ack_counter_lock = db_lock.ack_counter.lock().await;
                                *ack_counter_lock += 1;
                                println!("Received REPLCONF ACK. Ack counter incremented to {}", *ack_counter_lock);
                            } else if response.starts_with("+FULLRESYNC") {
                                // Send the FULLRESYNC response
                                {
                                    let mut stream_lock = writer.lock().await;
                                    stream_lock.write_all(response.as_bytes()).await?;
                                    stream_lock.flush().await?;
                                }
                                // Send the RDB file to the client (slave)
                                {
                                    let mut stream_lock = writer.lock().await;
                                    send_rdb_file(&mut *stream_lock).await?;
                                    println!("Sent RDB file after FULLRESYNC");
                                }

                                // Add the slave to the slave connections list
                                {
                                    let db_lock = db.lock().await;
                                    db_lock.slave_connections.write().await.push(Arc::clone(&writer));
                                }
                                println!("Added new slave after FULLRESYNC");

                            } else {
                                // Write the response to the client
                                println!("Sending response: {}", response);
                                {
                                    let mut stream_lock = writer.lock().await;
                                    stream_lock.write_all(response.as_bytes()).await?;
                                    stream_lock.flush().await?;
                                }

                                // Forward the command to all connected slaves if applicable
                                if let Some(cmd) = command {
                                    if should_forward_to_slaves(&cmd) {
                                        println!("Forwarding to slaves: {}", cmd);
                                        let mut db_lock = db.lock().await;
                                        println!("got db lock to forward to slaves");
                                        // Forward the message to all connected slaves
                                        let slaves = {
                                            
                                            db_lock.slave_connections.read().await.clone()
                                        };

                                        for slave_stream in slaves {
                                            let mut slave_stream = slave_stream.lock().await;
                                            println!("got slave stream lock");
                                            if let Err(e) = slave_stream.write_all(current_message.as_bytes()).await {
                                                eprintln!("Error sending message to slave: {:?}", e);
                                            }
                                        }

                                        // Increment the master_repl_offset only once for the total bytes sent
                                        let bytes_sent = current_message.as_bytes().len();
                                        if let Some(ReplicationInfoValue::ByteValue(current_offset)) = db_lock.replication_info.get("master_repl_offset") {
                                            let new_offset = current_offset + bytes_sent;
                                            db_lock.replication_info.insert(
                                                "master_repl_offset".to_string(),
                                                ReplicationInfoValue::ByteValue(new_offset),
                                            );
                                        } else {
                                            db_lock.replication_info.insert(
                                                "master_repl_offset".to_string(),
                                                ReplicationInfoValue::ByteValue(bytes_sent),
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // Remove the processed message from the partial buffer
                        partial_message.drain(..message_end);
                    }
                }
            }
            Err(e) => {
                // Handle errors from the read operation, if any
                println!("Error reading from stream: {:?}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Read operation timed out"));
            }
        }
    }

    println!("Connection timeout reached. Closing connection");
    Ok(())
}

// Function to determine if the end of the Redis message is reached
fn get_end_of_redis_message(message: &str) -> Option<usize> {
    let mut lines = message.lines();
    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            if let Ok(arg_count) = line[1..].parse::<usize>() {
                let mut total_len = line.len() + 2; // Include \r\n
                for _ in 0..arg_count {
                    if let Some(length_line) = lines.next() {
                        if length_line.starts_with('$') {
                            if let Ok(_bulk_length) = length_line[1..].parse::<usize>() {
                                total_len += length_line.len() + 2; // $<len>\r\n
                                if let Some(arg) = lines.next() {
                                    total_len += arg.len() + 2; // Argument and \r\n
                                }
                            }
                        }
                    }
                }
                return Some(total_len);
            }
        }
    }
    None
}

// Determines whether a command should be forwarded to slaves
fn should_forward_to_slaves(command: &str) -> bool {
    match command {
        "SET" | "GET" | "DEL" | "INCR" | "DECR" | "MSET" | "MGET" => true,
        _ => false, // Do not forward protocol-related commands like PING, REPLCONF, PSYNC, etc.
    }
}

