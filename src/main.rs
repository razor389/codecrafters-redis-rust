// src/main.rs
use std::{env, io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::parsing::parse_redis_message;
use crate::database::RedisDatabase;
use crate::rdb_parser::parse_rdb_file;

mod database;
mod commands;
mod parsing;
mod rdb_parser;

pub fn handle_client(stream: &mut TcpStream, db: &mut RedisDatabase, config_map: &HashMap<String, String>) -> io::Result<()> {
    let mut buffer = [0; 512];
    let mut partial_message = String::new();

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }

        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        if partial_message.ends_with("\r\n") {
            let response = parse_redis_message(&partial_message, db, config_map);
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            partial_message.clear();
        }
    }

    Ok(())
}

fn initialize_database(config_map: &HashMap<String, String>) -> RedisDatabase {
    let mut db = RedisDatabase::new();
    if let Some(rdb_path) = config_map.get("dbfilename") {
        if let Err(e) = parse_rdb_file(rdb_path, &mut db) {
            println!("Failed to parse RDB file: {}. Starting with an empty database.", e);
        }
    }
    db
}

pub fn start_server(config_map: HashMap<String, String>) -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    let db = Arc::new(Mutex::new(initialize_database(&config_map)));

    for mut stream in listener.incoming() {
        let config_map = config_map.clone();
        let db = Arc::clone(&db);
        thread::spawn(move || {
            match stream {
                Ok(ref mut stream) => {
                    println!("accepted new connection");
                    let mut db = db.lock().unwrap();
                    let _ = handle_client(stream, &mut db, &config_map);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
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
