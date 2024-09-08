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
fn handle_client(stream: &mut TcpStream, db: Arc<Mutex<RedisDatabase>>, config_map: &HashMap<String, String>) -> io::Result<()> {
    let mut buffer = [0; 512];
    let mut partial_message = String::new();

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }

        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        if partial_message.ends_with("\r\n") {
            println!("Received message: {}", partial_message);
            let mut db_lock = db.lock().unwrap();
            let response = parse_redis_message(&partial_message, &mut db_lock, config_map);

            // Check if the response is a FULLRESYNC message
            if response.starts_with("+FULLRESYNC") {
                // Write the FULLRESYNC response first
                stream.write_all(response.as_bytes())?;
                stream.flush()?;

                // Send the RDB file in RESP bulk string format
                send_rdb_file(stream)?;
                println!("Sent RDB file to client");
            } else {
                // Send the normal response
                stream.write_all(response.as_bytes())?;
                // Forward the command to all connected slaves
                for slave_connection in &db_lock.slave_connections {
                    let mut slave_stream = slave_connection.lock().unwrap();
                    println!("Forwarding message to slave: {}", partial_message);
                    slave_stream.write_all(partial_message.as_bytes())?;
                    slave_stream.flush()?;
                }
            }

            
            stream.flush()?;
            partial_message.clear();
        }
    }

    Ok(())
}
