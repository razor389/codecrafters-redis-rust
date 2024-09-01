#![allow(unused_imports)]
use std::{io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};

use redis_parser::parse_redis_message;

mod redis_parser;

fn handle_client(stream: &mut TcpStream) -> io::Result<()> {
    let mut buffer = [0; 512];
    let mut partial_message = String::new();

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break; // Connection was closed
        }

        let received_data = String::from_utf8_lossy(&buffer[..bytes_read]);
        partial_message.push_str(&received_data);

        if let Some(last_newline_idx) = partial_message.rfind('\n') {
            let complete_messages = &partial_message[..last_newline_idx + 1];
            let remaining_part = &partial_message[last_newline_idx + 1..];

            for message in complete_messages.split('\n') {
                if !message.trim().is_empty() {
                    let response = parse_redis_message(message);
                    stream.write_all(response.as_bytes())?;
                    stream.flush()?;
                }
            }

            partial_message = remaining_part.to_string();
        }
    }

    Ok(())
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for mut stream in listener.incoming() {
        thread::spawn(move || {
            match stream {
                Ok(ref mut stream) => {
                    println!("accepted new connection");
                    let _ = handle_client(stream);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}
