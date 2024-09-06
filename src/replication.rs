use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::database::RedisDatabase;


// Sends REPLCONF commands to the master after receiving the PING response
fn send_replconf(stream: &mut TcpStream, port: &str) -> io::Result<()> {
    // Send REPLCONF listening-port with the correct port
    let replconf_listening_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    );
    stream.write_all(replconf_listening_port.as_bytes())?;
    println!("Sent REPLCONF listening-port with port: {}", port);

    // Send REPLCONF capa eof capa psync2
    stream.write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")?;
    println!("Sent REPLCONF capa eof capa psync2");

    // Send PSYNC command to the master
    stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
    println!("Sent PSYNC command");

    // Keep listening for further commands from the master
    listen_for_master_commands(stream)?;

    Ok(())
}

fn listen_for_master_commands(stream: &mut TcpStream) -> io::Result<()> {
    let mut buffer = [0; 512];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        
        if bytes_read == 0 {
            // Master has closed the connection
            println!("Connection closed by master.");
            break;
        }

        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        println!("Received from master: {}", response);

        // Example: if the master sends a PSYNC command, you can handle it here.
        // You could match against specific commands and respond accordingly.
        // if response.contains("PSYNC") {
        //     println!("Master requested PSYNC");

        //     // Send back a PSYNC response (this is an example; you'll need to implement real logic here)
        //     stream.write_all(b"+FULLRESYNC\r\n")?;
        // }
    }

    Ok(())
}

// Initializes replication settings, determining whether this server is a master or slave
pub fn initialize_replication(config_map: &HashMap<String, String>, db: Arc<Mutex<RedisDatabase>>, port: &str) {
    let mut db_lock = db.lock().unwrap();

    if let Some(replicaof) = config_map.get("replicaof") {
        let replicaof_parts: Vec<&str> = replicaof.split(' ').collect();
        let ip = replicaof_parts[0];
        let replica_port = replicaof_parts[1];
        let address = format!("{}:{}", ip, replica_port);

        db_lock.replication_info.insert("role".to_string(), "slave".to_string());

        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                println!("Connected to master at {}", address);

                // Send PING to the master
                stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap(); // Timeout for PING response

                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(_) => {
                        println!("Received PING response from master");
                        // Send REPLCONF commands, using the dynamically provided port
                        let _ = send_replconf(&mut stream, port);
                    }
                    Err(e) => eprintln!("Failed to receive PING response: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to connect to master at {}: {}", address, e),
        }
    } else {
        db_lock.replication_info.insert("role".to_string(), "master".to_string());
        db_lock.replication_info.insert("master_replid".to_string(), "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
        db_lock.replication_info.insert("master_repl_offset".to_string(), "0".to_string());
        println!("Running as master.");
    }
}
