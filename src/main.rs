use std::{env, io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};
use std::collections::HashMap;
use redis_parser::parse_redis_message;

mod redis_parser;

pub fn handle_client(stream: &mut TcpStream, config_map: &HashMap<String, String>) -> io::Result<()> {
    let mut buffer = [0; 512];
    let mut partial_message = String::new();
    let mut hashmap = HashMap::new();

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }

        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        if partial_message.ends_with("\r\n") {
            let response = parse_redis_message(&partial_message, &mut hashmap, config_map);
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            partial_message.clear();
        }
    }

    Ok(())
}

pub fn start_server(config_map: HashMap<String, String>) -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for mut stream in listener.incoming() {
        let config_map = config_map.clone();
        thread::spawn(move || {
            match stream {
                Ok(ref mut stream) => {
                    println!("accepted new connection");
                    let _ = handle_client(stream, &config_map);
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

    let mut i = 1; // Start at 1 to skip the program name
    while i < args.len() {
        if i + 1 < args.len() {
            let key = args[i].clone();
            let value = args[i + 1].clone();
            config_map.insert(key, value);
            i += 2;
        } else {
            eprintln!("Missing value for key: {}", args[i]);
            break;
        }
    }

    println!("Starting server with config: {:?}", config_map);
    start_server(config_map).unwrap();
}
