// src/main.rs
use std::{env, io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::parsing::parse_redis_message;
use crate::database::RedisDatabase;
use crate::rdb_parser::parse_rdb_file;
use std::path::Path;

mod database;
mod commands;
mod parsing;
mod rdb_parser;

pub fn handle_client(stream: &mut TcpStream, db: Arc<Mutex<RedisDatabase>>, config_map: &HashMap<String, String>) -> io::Result<()> {
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
            let mut db_lock = db.lock().unwrap();  // Locking the database only when needed
            let response = parse_redis_message(&partial_message, &mut db_lock, config_map);
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            partial_message.clear();
        }
    }

    Ok(())
}


fn initialize_database(config_map: &HashMap<String, String>) -> RedisDatabase {
    let mut db = RedisDatabase::new();

    if let Some(dir) = config_map.get("dir") {
        if let Some(dbfilename) = config_map.get("dbfilename") {
            let rdb_path = Path::new(dir).join(dbfilename);
            if let Err(e) = parse_rdb_file(rdb_path.to_str().unwrap(), &mut db) {
                println!("Failed to parse RDB file: {}. Starting with an empty database.", e);
            }
        }
    }
    println!("Database data: {:?}", db.data);
    db
}


pub fn start_server(config_map: HashMap<String, String>) -> io::Result<()> {
    let default_port = "6379".to_string(); // Bind the default value to a variable
    let port = config_map.get("port").unwrap_or(&default_port); // Borrow either the value from the map or the default value
    let address = format!("127.0.0.1:{}", port);

    // Start the TCP listener on the chosen address
    let listener = TcpListener::bind(&address)?;

    println!("Server listening on {}", address);
    let db = Arc::new(Mutex::new(initialize_database(&config_map)));

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
                println!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let mut config_map = HashMap::new();

    let mut i = 1;
    while i < args.len() {
        let key = &args[i];

        if key.starts_with("--") {
            if i + 1 < args.len() {
                let value = args[i + 1].clone();
                let key = key[2..].to_string();
                config_map.insert(key, value);
                i += 2;
            } else {
                eprintln!("Missing value for key: {}", key);
                break;
            }
        } else {
            eprintln!("Invalid argument format: {}", key);
            break;
        }
    }

    println!("Starting server with config: {:?}", config_map);
    start_server(config_map).unwrap();
}
