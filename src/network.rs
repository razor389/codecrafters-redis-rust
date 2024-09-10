use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};
use crate::commands::send_rdb_file;
use crate::database::{RedisDatabase, ReplicationInfoValue};
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
    println!("started handle client");
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
            println!("Received complete Redis message in network: {}", current_message);

            let parsed_results = {
                let mut db_lock = db.lock().unwrap();
                parse_redis_message(&current_message, &mut db_lock, config_map)
            };

            for (command, args, response,_,_) in parsed_results {
                let mut sent_replconf_getack = false;
                let replconf_getack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                let replconf_getack_byte_len = replconf_getack_message.as_bytes().len();
                if command == Some("WAIT".to_string()) {
                    println!("got wait command");
                    if args.len() != 2 {
                        let error_response = "-ERR wrong number of arguments for WAIT\r\n";
                        stream.write_all(error_response.as_bytes())?;
                        stream.flush()?;
                    } else {
                        let num_slaves = args[0].parse::<usize>().unwrap_or(0);
                        let timeout_ms = args[1].parse::<u64>().unwrap_or(0);
                
                        // Activate the WAIT command in the database
                        {
                            println!("activating wait command in db");
                            println!("wait stream: {:?}", stream);
                            let mut db_lock = db.lock().unwrap();
                            db_lock.activate_wait_command(num_slaves, timeout_ms); 
                        }
                        // Send REPLCONF GETACK * to all slaves
                        let slaves = {
                            let db_lock = db.lock().unwrap();
                            db_lock.slave_connections.clone()
                        };
                
                        for slave_connection in slaves.iter() {
                            let mut slave_stream = slave_connection.lock().unwrap();
                            if slave_stream.write_all(replconf_getack_message.as_bytes()).is_err() {
                                println!("Failed to send REPLCONF GETACK to slave.");
                                continue;
                            }
                            sent_replconf_getack = true;
                            slave_stream.flush()?;
                        }

                        // Spin until the WAIT condition is satisfied or timeout occurs
                        spin_until_wait_resolved(db.clone(), &mut stream, timeout_ms)?;
                    }
                } else if command == Some("REPLCONF".to_string()) && args[0].to_uppercase()=="ACK" {
                    let mut db_lock = db.lock().unwrap();
                    db_lock.increment_responding_slaves();
                } else {
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
                                // Calculate the length of the current message in bytes
                                let bytes_sent = current_message.as_bytes().len();
                        
                                // Lock the database and clone the slave connections
                                let slaves = {
                                    let db_lock = db.lock().unwrap();
                                    db_lock.slave_connections.clone()
                                };
                        
                                // Forward the message to each slave
                                for slave_connection in slaves.iter() {
                                    let mut slave_stream = slave_connection.lock().unwrap();
                                    println!("Forwarding message to slave: {}", current_message);
                                    
                                    // Write the original command to the slave's stream
                                    slave_stream.write_all(current_message.as_bytes())?;
                                    slave_stream.flush()?;
                                }
                        
                                // Increment the master_repl_offset only once for the total bytes sent
                                let mut db_lock = db.lock().unwrap();
                                // Increment the master_repl_offset in replication_info
                                if let Some(ReplicationInfoValue::ByteValue(current_offset)) = db_lock.replication_info.get("master_repl_offset") {
                                    // Increment the offset by the number of bytes sent
                                    let new_offset = current_offset + bytes_sent;
                        
                                    // Update the replication_info with the new offset as a ByteValue
                                    db_lock.replication_info.insert(
                                        "master_repl_offset".to_string(),
                                        ReplicationInfoValue::ByteValue(new_offset)
                                    );
                                } else {
                                    // If the master_repl_offset does not exist, initialize it with the current bytes sent
                                    db_lock.replication_info.insert(
                                        "master_repl_offset".to_string(),
                                        ReplicationInfoValue::ByteValue(bytes_sent)
                                    );
                                }
                            }
                        }
                    }
                }
                if sent_replconf_getack{
                    let mut db_lock = db.lock().unwrap();
                    // Increment the master_repl_offset in replication_info
                    if let Some(ReplicationInfoValue::ByteValue(current_offset)) = db_lock.replication_info.get("master_repl_offset") {
                        // Increment the offset by the number of bytes sent
                        let new_offset = current_offset + replconf_getack_byte_len;
            
                        // Update the replication_info with the new offset as a ByteValue
                        db_lock.replication_info.insert(
                            "master_repl_offset".to_string(),
                            ReplicationInfoValue::ByteValue(new_offset)
                        );
                    } else {
                        // If the master_repl_offset does not exist, initialize it with the current bytes sent
                        db_lock.replication_info.insert(
                            "master_repl_offset".to_string(),
                            ReplicationInfoValue::ByteValue(replconf_getack_byte_len)
                        );
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

fn spin_until_wait_resolved(
    db: Arc<Mutex<RedisDatabase>>,
    stream: &mut TcpStream,
    timeout_ms: u64,
) -> std::io::Result<()> {
    let start_time = Instant::now();
    
    loop {
        // Check if we've hit the timeout
        if start_time.elapsed() >= Duration::from_millis(timeout_ms) {
            let mut db_lock = db.lock().unwrap();
            if let Some(responding_slaves) = db_lock.check_wait_timeout() {
                println!("WAIT command timed out.");
                let wait_response = format!(":{}\r\n", responding_slaves);
                stream.write_all(wait_response.as_bytes())?;
                stream.flush()?;
                // Reset the WAIT state in the database
                db_lock.reset_wait_state();
            }
            break;
        }

        // Check if enough slaves have responded
        {
            let mut db_lock = db.lock().unwrap();
            let wait_state = db_lock.wait_state.as_mut();
            if let Some(wait_state) = wait_state {
                //println!(
                //    "Checking WAIT state: num_slaves_to_wait_for = {}, responding_slaves = {}",
                //    wait_state.num_slaves_to_wait_for, wait_state.responding_slaves
                //);

                if wait_state.responding_slaves >= wait_state.num_slaves_to_wait_for {
                    // If we have enough slave responses, send the response to the client
                    let wait_response = format!(":{}\r\n", wait_state.responding_slaves);
                    stream.write_all(wait_response.as_bytes())?;
                    stream.flush()?;

                    // Reset the WAIT state in the database
                    db_lock.reset_wait_state();
                    break;
                }
            }
        }

        // Sleep for a short period before the next check to avoid busy-waiting
        sleep(Duration::from_millis(200));
    }

    Ok(())
}