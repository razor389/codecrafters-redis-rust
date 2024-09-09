use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::commands::send_rdb_file;
use crate::database::RedisDatabase;
use crate::parsing::parse_redis_message;

// Starts the server and handles incoming client connections
pub fn start_server(config_map: HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>) -> io::Result<()> {
    let default_port = "6379".to_string();
    let port = config_map.get("port").unwrap_or(&default_port).to_string(); // Capture the port dynamically
    let address = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(&address)?;
    println!("Server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("New client connection: {}", stream.peer_addr().unwrap());
                let db = Arc::clone(&db);
                let config_map = config_map.clone();
                thread::spawn(move || {
                    let _ = handle_client(&mut stream, db, &config_map);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}

// Handles a single client connection
fn handle_client(
    stream: &mut TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> io::Result<()> {
    let mut buffer = [0; 1024]; // Buffer size
    let mut partial_message = String::new();

    loop {
        println!("Waiting for data...");
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            println!("Connection closed by client.");
            break; // Connection closed by client
        }

        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        if partial_message.ends_with("\r\n") {
            println!("Received message: {}", partial_message);

            let (command, response) = {
                // Acquire lock briefly for database operations
                let mut db_lock = db.lock().unwrap();
                parse_redis_message(&partial_message, &mut db_lock, config_map)
            };

            // Handle FULLRESYNC
            if response.starts_with("+FULLRESYNC") {
                // Send the FULLRESYNC response first
                stream.write_all(response.as_bytes())?;
                stream.flush()?;

                // Send the RDB file to the client (slave)
                send_rdb_file(stream)?;
                println!("Sent RDB file after FULLRESYNC");

                // Add the slave connection to the list of slaves
                let cloned_stream = Arc::new(Mutex::new(stream.try_clone()?));
                {
                    let mut db_lock = db.lock().unwrap();
                    db_lock.slave_connections.push(cloned_stream);
                }
                println!("Added new slave after FULLRESYNC");

            } else {
                // Write the response to the client
                stream.write_all(response.as_bytes())?;
                stream.flush()?;

                // Forward the command to all connected slaves if applicable
                if let Some(cmd) = command {
                    if should_forward_to_slaves(&cmd) {
                        let slaves = {
                            // Acquire lock briefly to get slave connections
                            let db_lock = db.lock().unwrap();
                            db_lock.slave_connections.clone()
                        };
                        for slave_connection in slaves {
                            let mut slave_stream = slave_connection.lock().unwrap();
                            println!("Forwarding message to slave: {}", partial_message);
                            slave_stream.write_all(partial_message.as_bytes())?;
                            slave_stream.flush()?;
                        }
                    }
                }
            }

            partial_message.clear(); // Reset message buffer for the next command
        }
    }

    Ok(())
}

fn should_forward_to_slaves(command: &str) -> bool {
    match command {
        "SET" | "GET" | "DEL" | "INCR" | "DECR" | "MSET" | "MGET" => true,
        _ => false, // Do not forward protocol-related commands like PING, REPLCONF, PSYNC, etc.
    }
}
