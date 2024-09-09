use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;
use crate::commands::send_rdb_file;
use crate::database::RedisDatabase;
use crate::parsing::parse_redis_message;

pub fn start_server(config_map: HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let default_port = "6379".to_string();
    let port = config_map.get("port").unwrap_or(&default_port).to_string();
    let address = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(&address)?;
    println!("Server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New client connection from {}", stream.peer_addr()?);

                let db = Arc::clone(&db);
                let config_map = config_map.clone();

                // Spawn a new thread to handle the client connection
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, db, &config_map) {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> std::io::Result<()> {
    let mut buffer = vec![0; 4096];
    let mut partial_message = String::new();

    loop {
        println!("Waiting for data...");

        // Read data from the stream
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            println!("Connection closed by client.");
            break;
        }

        // Append the newly read data to the partial message buffer
        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        // Process all complete Redis messages
        while let Some(message_end) = get_end_of_redis_message(&partial_message) {
            let current_message = partial_message[..message_end].to_string();
            println!("Received complete Redis message: {}", current_message);

            let parsed_results = {
                let mut db_lock = db.lock().unwrap();
                parse_redis_message(&current_message, &mut db_lock, config_map)
            };

            for (command, _args, response, _) in parsed_results {
                if response.starts_with("+FULLRESYNC") {
                    // Send the FULLRESYNC response
                    stream.write_all(response.as_bytes())?;
                    stream.flush()?;

                    // Send the RDB file to the client (slave)
                    send_rdb_file(&mut stream)?;
                    println!("Sent RDB file after FULLRESYNC");

                    // Add the slave connection to the list of slaves
                    let mut db_lock = db.lock().unwrap();
                    db_lock.slave_connections.push(Arc::new(Mutex::new(stream.try_clone()?)));
                    println!("Added new slave after FULLRESYNC");

                } else {
                    // Write the response to the client
                    println!("Sending response: {}", response);
                    stream.write_all(response.as_bytes())?;
                    stream.flush()?;

                    // Forward the command to all connected slaves if applicable
                    if let Some(cmd) = command {
                        if should_forward_to_slaves(&cmd) {
                            let slaves = {
                                let db_lock = db.lock().unwrap();
                                db_lock.slave_connections.clone()
                            };
                            for slave_connection in slaves {
                                let mut slave_stream = slave_connection.lock().unwrap();
                                println!("Forwarding message to slave: {}", current_message);
                                slave_stream.write_all(current_message.as_bytes())?;
                                slave_stream.flush()?;
                            }
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
