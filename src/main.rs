#![allow(unused_imports)]
use std::{io::Write, net::{TcpListener, TcpStream}};

fn handle_client(stream: &mut std::net::TcpStream) {
    // handle client here
    stream.write("+PONG\r\n".as_bytes()).unwrap();
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for mut stream in listener.incoming() {
        match stream {
            Ok(ref mut stream) => {
                println!("accepted new connection");
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
