use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::database::RedisDatabase;
use crate::parsing::parse_redis_message;

// Sends REPLCONF commands to the master after receiving the PING response
fn send_replconf(stream: &mut TcpStream, port: &str, db: Arc<Mutex<RedisDatabase>>, config_map: &HashMap<String, String>) -> io::Result<()> {
    // Send REPLCONF listening-port with the correct port
    let replconf_listening_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    );
    stream.write_all(replconf_listening_port.as_bytes())?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    // Send REPLCONF capa eof capa psync2
    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")?;
    println!("Sent REPLCONF capa eof capa psync2");

    // Wait for +OK response from the master
    let mut buffer = [0; 512];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);
    
    if response.contains("+OK") {
        println!("Received +OK from master, proceeding with PSYNC");

        // Send PSYNC command to the master after receiving +OK
        stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
        println!("Sent PSYNC command");

        // Keep listening for further commands from the master and pass the db for command execution
        listen_for_master_commands(stream, db, config_map)?;
    } else {
        println!("Unexpected response from master: {}", response);
    }

    Ok(())
}

// Listens for and processes commands sent by the master
fn listen_for_master_commands(stream: &mut TcpStream, db: Arc<Mutex<RedisDatabase>>, config_map: &HashMap<String, String>) -> io::Result<()> {
    let mut buffer = [0; 512];  // Buffer to store incoming data

    loop {
        // Read from the master
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            // Master has closed the connection
            println!("Connection closed by master.");
            break;
        }

        // Convert the response into a string to handle the command
        let message = String::from_utf8_lossy(&buffer[..bytes_read]);
        println!("Received message from master: {}", message);

        // Check if we received a FULLRESYNC message
        if message.starts_with("+FULLRESYNC") {
            // Parse the FULLRESYNC response, which contains the master replication ID and offset
            let parts: Vec<&str> = message.split_whitespace().collect();
            if parts.len() >= 3 {
                let master_replid = parts[1];
                let master_offset = parts[2];

                println!("Received FULLRESYNC with replid: {} and offset: {}", master_replid, master_offset);

                // Store master_replid and master_offset in your RedisDatabase replication state (lock briefly)
                {
                    let mut db_lock = db.lock().unwrap();
                    db_lock.replication_info.insert("master_replid".to_string(), master_replid.to_string());
                    db_lock.replication_info.insert("master_repl_offset".to_string(), master_offset.to_string());
                }

                // After FULLRESYNC, we expect an RDB file as a bulk string
                println!("Waiting for the RDB file...");
            }
        }
        // Handle the bulk string for the RDB file transfer
        else if message.starts_with("$") {
            // Parse the bulk string length (the RDB file length)
            if let Some((length_str, _)) = message[1..].split_once("\r\n") {
                let length: usize = length_str.parse().unwrap();
                println!("Expecting RDB file of length: {}", length);

                // Allocate a buffer for the RDB file
                let mut rdb_data = vec![0u8; length];

                // Read the complete RDB file, retrying if needed until all bytes are received
                let mut bytes_read = 0;
                while bytes_read < length {
                    let read = stream.read(&mut rdb_data[bytes_read..])?;
                    if read == 0 {
                        println!("Connection closed before receiving the full RDB file.");
                        break;
                    }
                    bytes_read += read;
                    println!("Read {}/{} bytes of RDB file.", bytes_read, length);
                }

                // Ensure we received the complete RDB file
                if bytes_read == length {
                    println!("Successfully received the full RDB file.");

                    // Lock database and store/process the RDB data
                    {
                        let mut db_lock = db.lock().unwrap();
                        db_lock.load_rdb_data(rdb_data);
                    }

                    println!("RDB file received and processed.");
                } else {
                    println!("Failed to receive the full RDB file. Expected {} bytes, but got {} bytes.", length, bytes_read);
                }
            }
        } else {
            println!("Received command from master: {}", message);
            if message == "+OK\r\n" {
                continue; // Ignore PING responses
            }
            // Parse the Redis message and get the response for other commands
            let mut db_lock = db.lock().unwrap();
            let (command, response) = parse_redis_message(&message, &mut db_lock, config_map);

            // Log the command and response for debugging purposes
            println!("Parsed command: {:?}, Response: {}", command, response);

            // Only send responses back for certain commands (e.g., PSYNC), no "OK" for most
            if let Some(cmd) = command {
                if cmd == "PSYNC" {
                    stream.write_all(response.as_bytes())?;
                }
            }
        }
    }

    Ok(())
}

// Initializes replication settings, determining whether this server is a master or slave
pub fn initialize_replication(config_map: &HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>, port: &str) {
    let mut db_lock = db.lock().unwrap();

    if let Some(replicaof) = config_map.get("replicaof") {
        let replicaof_parts: Vec<&str> = replicaof.split(' ').collect();
        let ip = replicaof_parts[0];
        let replica_port = replicaof_parts[1];
        let address = format!("{}:{}", ip, replica_port);

        db_lock.replication_info.insert("role".to_string(), "slave".to_string());

        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);

                // Send PING to the master
                stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap(); // Timeout for PING response

                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(_) => {
                        println!("Received PING response from master");
                        // Send REPLCONF commands and PSYNC, using the dynamically provided port
                        let _ = send_replconf(&mut stream, port, db.clone(), config_map);
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        db_lock.replication_info.insert("role".to_string(), "master".to_string());
        db_lock.replication_info.insert("master_replid".to_string(), "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
        db_lock.replication_info.insert("master_repl_offset".to_string(), "0".to_string());
        println!("Running as master.");
    }
}
