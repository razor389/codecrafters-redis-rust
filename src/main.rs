#![allow(unused_imports)]
use std::{io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};

use redis_parser::parse_redis_message;

mod redis_parser;

pub fn handle_client(stream: &mut TcpStream) -> io::Result<()> {
    let mut buffer = [0; 512];
    let mut partial_message = String::new();

    loop {
        // Read data from the stream into the buffer
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break; // Connection was closed
        }

        // Append the received data to the partial message
        partial_message.push_str(&String::from_utf8_lossy(&buffer[..bytes_read]));

        // Log the partial message for debugging
        println!("Partial message received: {:?}", partial_message);

        // Check if the partial message contains a full RESP command
        if partial_message.ends_with("\r\n") {
            // Now parse the full message
            let response = parse_redis_message(&partial_message);
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            partial_message.clear(); // Clear the message buffer after processing
        }
    }

    Ok(())
}

pub fn start_server() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

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
    Ok(())
}

fn main() {
    println!("Logs from your program will appear here!");
    start_server().unwrap();
}