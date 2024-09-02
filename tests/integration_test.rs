// tests/integration_test.rs

use std::net::TcpStream;
use std::io::{Read, Write};
use std::thread;
use std::time::Duration;

#[test]
fn test_ping() {
    thread::spawn(|| {
        // Start the server in a separate thread
        redis_starter_rust::start_server().unwrap(); // Replace with the correct crate name
    });

    // Give the server some time to start
    thread::sleep(Duration::from_secs(1));

    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();

    // Send PING command
    stream.write_all(b"PING\r\n").unwrap();

    // Read the response
    let mut buffer = [0; 512];
    let n = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[..n]);

    assert_eq!(response, "+PONG\r\n");
}
