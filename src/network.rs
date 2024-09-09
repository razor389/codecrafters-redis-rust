use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::commands::send_rdb_file;
use crate::database::RedisDatabase;
use crate::parsing::parse_redis_message;

// Starts the server and handles incoming client connections
pub async fn start_server(config_map: HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let default_port = "6379".to_string();
    let port = config_map.get("port").unwrap_or(&default_port).to_string(); // Capture the port dynamically
    let address = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(&address).await?;
    println!("Server listening on {}", address);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New client connection: {}", addr);
                let db = Arc::clone(&db);
                let config_map = config_map.clone();
                let stream = Arc::new(Mutex::new(stream));  // Wrap the stream in an Arc<Mutex<>> for shared access

                // Spawn a new asynchronous task to handle the client
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

// Handles a single client connection asynchronously
async fn handle_client(
    stream: Arc<Mutex<TcpStream>>,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> std::io::Result<()> {
    let mut buffer = vec![0; 4096]; // Buffer size to read from the stream
    let mut partial_message = String::new();

    loop {
        println!("Waiting for data...");
        let mut locked_stream = stream.lock().await;  // Lock the stream for access
        let bytes_read = locked_stream.read(&mut buffer).await?;

        if bytes_read == 0 {
            println!("Connection closed by client.");
            break; // Connection closed by client
        }

        // Append the newly read data to the partial message buffer
        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        // Process the accumulated message while there is a complete RESP message
        while let Some(message_end) = get_end_of_redis_message(&partial_message) {
            let current_message = partial_message[..message_end].to_string(); // Extract the complete message
            println!("Received complete Redis message: {}", current_message);

            let (command, args, response, _) = {
                // Acquire lock briefly for database operations
                let mut db_lock = db.lock().await;
                parse_redis_message(&current_message, &mut db_lock, config_map)
            };

            if response.starts_with("+FULLRESYNC") {
                {
                    let mut locked_stream = stream.lock().await;
                    // Send the FULLRESYNC response first
                    locked_stream.write_all(response.as_bytes()).await?;
                    locked_stream.flush().await?;
                }

                // Send the RDB file to the client (slave)
                {
                    let mut locked_stream = stream.lock().await;
                    send_rdb_file(&mut *locked_stream).await?;
                }

                println!("Sent RDB file after FULLRESYNC");

                // Add the slave connection to the list of slaves
                {
                    let mut db_lock = db.lock().await;
                    db_lock.slave_connections.push(Arc::clone(&stream));  // Reuse the same stream using Arc
                }
                println!("Added new slave after FULLRESYNC");

            } else {
                // Write the response to the client
                {
                    println!("Sending response: {}", response);
                    let mut locked_stream = stream.lock().await;
                    locked_stream.write_all(response.as_bytes()).await?;
                    locked_stream.flush().await?;
                }

                // Forward the command to all connected slaves if applicable
                if let Some(cmd) = command {
                    if should_forward_to_slaves(&cmd) {
                        let slaves = {
                            // Acquire lock briefly to get slave connections
                            let db_lock = db.lock().await;
                            db_lock.slave_connections.clone()
                        };
                        for slave_connection in slaves {
                            let mut slave_stream = slave_connection.lock().await;
                            println!("Forwarding message to slave: {}", current_message);
                            slave_stream.write_all(current_message.as_bytes()).await?;
                            slave_stream.flush().await?;
                        }
                    }
                }
            }

            // Remove the processed message from the partial buffer
            partial_message.drain(..message_end);
        }
    }

    Ok(())
}

fn get_end_of_redis_message(message: &str) -> Option<usize> {
    let mut lines = message.lines();
    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            if let Ok(arg_count) = line[1..].parse::<usize>() {
                let mut total_len = line.len() + 2; // Including \r\n
                for _ in 0..arg_count {
                    if let Some(length_line) = lines.next() {
                        if length_line.starts_with('$') {
                            if let Ok(bulk_length) = length_line[1..].parse::<usize>() {
                                total_len += length_line.len() + 2; // $<len>\r\n
                                if let Some(arg) = lines.next() {
                                    total_len += arg.len() + 2; // The argument with \r\n
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


fn should_forward_to_slaves(command: &str) -> bool {
    match command {
        "SET" | "GET" | "DEL" | "INCR" | "DECR" | "MSET" | "MGET" => true,
        _ => false, // Do not forward protocol-related commands like PING, REPLCONF, PSYNC, etc.
    }
}
