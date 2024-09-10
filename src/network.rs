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
                let db = Arc::clone(&db);
                let config_map = config_map.clone();

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
    stream: TcpStream,
    db: Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> std::io::Result<()> {
    let stream = Arc::new(Mutex::new(stream));
    let mut buffer = vec![0; 4096];
    let mut partial_message = String::new();

    loop {
        check_wait_timeout(&db)?;
        let bytes_read = read_from_stream(&stream, &mut buffer)?;
        if bytes_read == 0 {
            println!("Connection closed by client.");
            break;
        }
        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        process_messages(&mut partial_message, &stream, &db, config_map)?;
    }
    Ok(())
}

fn check_wait_timeout(db: &Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let mut db_lock = db.lock().unwrap();
    if let Some(responding_slaves) = db_lock.check_wait_timeout() {
        let wait_response = format!(":{}\r\n", responding_slaves);
        {
            let wait_stream = &mut db_lock.wait_state.as_mut().unwrap().wait_stream;
            let mut stream_lock = wait_stream.lock().unwrap();
            stream_lock.write_all(wait_response.as_bytes())?;
            stream_lock.flush()?;
        }
        db_lock.reset_wait_state();
    }
    Ok(())
}

fn read_from_stream(stream: &Arc<Mutex<TcpStream>>, buffer: &mut Vec<u8>) -> std::io::Result<usize> {
    let mut stream_lock = stream.lock().unwrap();
    stream_lock.read(buffer)
}

fn process_messages(
    partial_message: &mut String,
    stream: &Arc<Mutex<TcpStream>>,
    db: &Arc<Mutex<RedisDatabase>>,
    config_map: &HashMap<String, String>,
) -> std::io::Result<()> {
    while let Some(message_end) = get_end_of_redis_message(partial_message) {
        let current_message = partial_message[..message_end].to_string();
        let parsed_results = {
            let mut db_lock = db.lock().unwrap();
            parse_redis_message(&current_message, &mut db_lock, config_map)
        };

        for (command, args, response, _, _) in parsed_results {
            handle_command(command, args, response, stream, db)?;
        }

        partial_message.drain(..message_end);
    }
    Ok(())
}

// Restored get_end_of_redis_message function
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

fn handle_command(
    command: Option<String>,
    args: Vec<String>,
    response: String,
    stream: &Arc<Mutex<TcpStream>>,
    db: &Arc<Mutex<RedisDatabase>>,
) -> std::io::Result<()> {
    if let Some(ref cmd) = command {
        match (cmd.as_str(), args.get(0).map(|arg| arg.to_uppercase())) {
            // Match REPLCONF with ACK
            ("REPLCONF", Some(ref arg)) if arg == "ACK" => handle_replconf_ack(db)?,
            
            // Match WAIT command (no need to check the argument here)
            ("WAIT", _) => handle_wait_command(&args, stream, db)?,
            
            // Default handling for other commands
            _ => handle_default_command(cmd, response, stream, db)?,
        }
    }
    Ok(())
}


fn handle_replconf_ack(db: &Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let mut db_lock = db.lock().unwrap();
    db_lock.increment_responding_slaves();

    if let Some(wait_state) = db_lock.wait_state.as_mut() {
        if wait_state.responding_slaves >= wait_state.num_slaves_to_wait_for {
            let wait_response = format!(":{}\r\n", wait_state.responding_slaves);
            {
                let wait_stream = &wait_state.wait_stream;
                let mut stream_lock = wait_stream.lock().unwrap();
                stream_lock.write_all(wait_response.as_bytes())?;
                stream_lock.flush()?;
            }
            db_lock.reset_wait_state();
        }
    }

    Ok(())
}


fn handle_wait_command(args: &[String], stream: &Arc<Mutex<TcpStream>>, db: &Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    if args.len() != 2 {
        send_error_response("-ERR wrong number of arguments for WAIT\r\n", stream)?;
    } else {
        let num_slaves = args[0].parse::<usize>().unwrap_or(0);
        let timeout_ms = args[1].parse::<u64>().unwrap_or(0);
        {
            let mut db_lock = db.lock().unwrap();
            db_lock.activate_wait_command(num_slaves, timeout_ms, stream.clone());
        }
        send_replconf_getack_to_slaves(db)?;
    }
    Ok(())
}

fn handle_default_command(
    command: &str,
    response: String,
    stream: &Arc<Mutex<TcpStream>>,
    db: &Arc<Mutex<RedisDatabase>>,
) -> std::io::Result<()> {
    if response.starts_with("+FULLRESYNC") {
        send_full_resync_response(response, stream, db)?;
    } else {
        send_response(&response, stream)?;
        if should_forward_to_slaves(command) {
            forward_command_to_slaves(command, db)?;
        }
    }
    Ok(())
}

fn send_full_resync_response(
    response: String,
    stream: &Arc<Mutex<TcpStream>>,
    db: &Arc<Mutex<RedisDatabase>>,
) -> std::io::Result<()> {
    let mut stream_lock = stream.lock().unwrap();
    stream_lock.write_all(response.as_bytes())?;
    stream_lock.flush()?;
    send_rdb_file(&mut stream_lock)?;
    let stream_clone = stream_lock.try_clone()?;
    let mut db_lock = db.lock().unwrap();
    db_lock.slave_connections.push(Arc::new(Mutex::new(stream_clone)));
    Ok(())
}

fn send_response(response: &str, stream: &Arc<Mutex<TcpStream>>) -> std::io::Result<()> {
    let mut stream_lock = stream.lock().unwrap();
    stream_lock.write_all(response.as_bytes())?;
    stream_lock.flush()?;
    Ok(())
}

fn send_error_response(error: &str, stream: &Arc<Mutex<TcpStream>>) -> std::io::Result<()> {
    let mut stream_lock = stream.lock().unwrap();
    stream_lock.write_all(error.as_bytes())?;
    stream_lock.flush()?;
    Ok(())
}

fn send_replconf_getack_to_slaves(db: &Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let replconf_getack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    let slaves = {
        let db_lock = db.lock().unwrap();
        db_lock.slave_connections.clone()
    };
    for slave_connection in slaves.iter() {
        let mut slave_stream = slave_connection.lock().unwrap();
        slave_stream.write_all(replconf_getack_message.as_bytes())?;
        slave_stream.flush()?;
    }
    Ok(())
}

fn forward_command_to_slaves(command: &str, db: &Arc<Mutex<RedisDatabase>>) -> std::io::Result<()> {
    let current_message = format!("*1\r\n${}\r\n{}\r\n", command.len(), command);
    let slaves = {
        let db_lock = db.lock().unwrap();
        db_lock.slave_connections.clone()
    };
    for slave_connection in slaves.iter() {
        let mut slave_stream = slave_connection.lock().unwrap();
        slave_stream.write_all(current_message.as_bytes())?;
        slave_stream.flush()?;
    }
    Ok(())
}

fn should_forward_to_slaves(command: &str) -> bool {
    match command {
        "SET" | "GET" | "DEL" | "INCR" | "DECR" | "MSET" | "MGET" => true,
        _ => false, // Do not forward protocol-related commands like PING, REPLCONF, PSYNC, etc.
    }
}