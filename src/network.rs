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
    let stream = Arc::new(Mutex::new(stream));

    let mut buffer = vec![0; 4096];
    let mut partial_message = String::new();

    // Set the connection timeout duration (for example, 30 seconds)
    let connection_timeout = Duration::from_secs(30);

    while let Ok(bytes_read) = timeout(connection_timeout, async {
        let mut stream_lock = stream.lock().await;
        stream_lock.read(&mut buffer).await
    })
    .await{
        match bytes_read {
            Ok(bytes_read) => {
                println!("Waiting for data...");

                if bytes_read == 0 {
                    let db_lock = db.lock().await;
                    if let Some(ReplicationInfoValue::StringValue(value)) = db_lock.get_replication_info("role"){
                        println!("closing connection for role: {}" ,value);
                    }
                
                    println!("Connection closed by client.");
                    return Ok(());
                }else{

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
                            let mut sent_replconf_getack = false;
                            let replconf_getack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                            let replconf_getack_byte_len = replconf_getack_message.as_bytes().len();

                            if command == Some("WAIT".to_string()) {
                                println!("got wait command");
                                if args.len() != 2 {
                                    let error_response = "-ERR wrong number of arguments for WAIT\r\n";
                                    let mut stream_lock = stream.lock().await;
                                    stream_lock.write_all(error_response.as_bytes()).await?;
                                    stream_lock.flush().await?;
                                } else {
                                    let num_slaves = args[0].parse::<usize>().unwrap_or(0);
                                    let timeout_ms = args[1].parse::<u64>().unwrap_or(0);
                                    let mut responding_slaves = 0;
                            
                                    // Send REPLCONF GETACK * to all slaves
                                    let slaves = {
                                        let db_lock = db.lock().await;
                                        db_lock.slave_connections.clone()
                                    };
                            
                                    for slave_connection in slaves.iter() {
                                        let mut slave_stream = slave_connection.lock().await;
                                        if slave_stream.write_all(replconf_getack_message.as_bytes()).await.is_err() {
                                            println!("Failed to send REPLCONF GETACK to slave.");
                                            continue;
                                        }
                                        sent_replconf_getack = true;
                                        slave_stream.flush().await?;
                                    }
                            
                                    // Start the timeout for the WAIT command
                                    let timeout_duration = tokio::time::Duration::from_millis(timeout_ms);
                            
                                    // Listen for REPLCONF ACK responses within the timeout period
                                    let wait_result = tokio::time::timeout(timeout_duration, async {
                                        println!("got here in waiting loop");
                                        loop {
                                            let slaves = {
                                                let db_lock = db.lock().await;
                                                db_lock.slave_connections.clone()
                                            };
                            
                                            for slave_connection in slaves.iter() {
                                                let mut slave_stream = slave_connection.lock().await;
                                                let mut buffer = vec![0; 512]; // Adjust the buffer size as needed
                            
                                                match slave_stream.read(&mut buffer).await {
                                                    Ok(bytes_read) => {
                                                        if bytes_read == 0 {
                                                            continue; // No response from this slave
                                                        }
                            
                                                        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
                                                        if response.contains("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK") {
                                                            responding_slaves += 1;
                                                            if responding_slaves >= num_slaves {
                                                                return Ok::<usize, ()>(responding_slaves); // All slaves have responded
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        println!("Error reading from slave: {:?}", e);
                                                        continue;
                                                    }
                                                }
                                            }
                            
                                            // Sleep briefly to avoid busy-looping
                                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                        }
                                    }).await;
                            
                                    // Check the result of the wait
                                    let wait_response = match wait_result {
                                        Ok(Ok(responding_slaves)) => format!(":{}\r\n", responding_slaves),
                                        Ok(Err(_)) | Err(_) => format!(":0\r\n"), // Either timeout or an internal error
                                    };

                            
                                    let mut stream_lock = stream.lock().await;
                                    stream_lock.write_all(wait_response.as_bytes()).await?;
                                    stream_lock.flush().await?;
                                }
                            
                            } else {
                                if response.starts_with("+FULLRESYNC") {
                                    // Send the FULLRESYNC response
                                    {
                                        let mut stream_lock = stream.lock().await;
                                        stream_lock.write_all(response.as_bytes()).await?;
                                        stream_lock.flush().await?;
                                
                                        // Send the RDB file to the client (slave)
                                        send_rdb_file(&mut *stream_lock).await?;
                                        println!("Sent RDB file after FULLRESYNC");
                                    }
                                    {
                                        // Add the slave connection to the list of slaves
                                        let mut db_lock = db.lock().await;
                                        db_lock.slave_connections.push(Arc::clone(&stream));
                                    }
                                    println!("Added new slave after FULLRESYNC");
                                } else {
                                    // Write the response to the client
                                    println!("Sending response: {}", response);
                                    {
                                        let mut stream_lock = stream.lock().await;
                                        stream_lock.write_all(response.as_bytes()).await?;
                                        stream_lock.flush().await?;
                                    }
                                    // Forward the command to all connected slaves if applicable
                                    if let Some(cmd) = command {
                                        if should_forward_to_slaves(&cmd) {
                                            println!("forwarding to slaves: {}", cmd);
                                            // Calculate the length of the current message in bytes
                                            let bytes_sent = current_message.as_bytes().len();

                                            // Lock the database and clone the slave connections
                                            let slaves = {
                                                let db_lock = db.lock().await;
                                                db_lock.slave_connections.clone()
                                            };
                                            // Forward the message to each slave
                                            for slave_connection in slaves.iter() {
                                                let mut slave_stream = slave_connection.lock().await;
                                                println!("Forwarding message to slave: {}", current_message);

                                                // Write the original command to the slave's stream
                                                slave_stream.write_all(current_message.as_bytes()).await?;
                                                slave_stream.flush().await?;
                                            }

                                            // Increment the master_repl_offset only once for the total bytes sent
                                            let mut db_lock = db.lock().await;
                                            if let Some(ReplicationInfoValue::ByteValue(current_offset)) =
                                                db_lock.replication_info.get("master_repl_offset")
                                            {
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

                            if sent_replconf_getack {
                                let mut db_lock = db.lock().await;
                                let new_offset = db_lock
                                    .replication_info
                                    .get("master_repl_offset")
                                    .map(|offset| match offset {
                                        ReplicationInfoValue::ByteValue(current_offset) => current_offset + replconf_getack_byte_len,
                                        _ => replconf_getack_byte_len,
                                    })
                                    .unwrap_or(replconf_getack_byte_len);
                                db_lock.replication_info.insert(
                                    "master_repl_offset".to_string(),
                                    ReplicationInfoValue::ByteValue(new_offset),
                                );
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

    println!("connection timeout reached. closing connection");
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

