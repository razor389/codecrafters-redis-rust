#![allow(unused_imports)]
use std::{io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread};



fn handle_client(stream: &mut std::net::TcpStream)->io::Result<()> {
    // handle client here
    let mut buffer = [0; 512];
    let mut partial_message = String::new();
    Ok(loop {
        // Read data from the stream into the buffer
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            break; // Connection was closed
        }

        // Convert the buffer to a string (assuming it's UTF-8 encoded)
        let received_data = String::from_utf8_lossy(&buffer[..bytes_read]);
        partial_message.push_str(&received_data);

        // Process complete messages
        if let Some(last_newline_idx) = partial_message.rfind('\n') {
            let complete_messages = &partial_message[..last_newline_idx + 1];
            let remaining_part = &partial_message[last_newline_idx + 1..];

            for message in complete_messages.split('\n') {
                if message.trim() == "PING" {
                    // Respond with "+PONG\r\n"
                    stream.write_all(b"+PONG\r\n")?;
                    stream.flush()?;
                }
            }

            // Keep the remaining part for the next read
            partial_message = remaining_part.to_string();
        }
    })
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
