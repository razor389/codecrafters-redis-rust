use std::collections::HashMap;

pub fn parse_redis_message(message: &str, hashmap: &mut HashMap<String, String>) -> String {
    let mut lines = message.lines();

    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            let mut command = None;
            let mut args = Vec::<String>::new();

            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    // Skip the length line and go to the actual data
                    if let Some(arg) = lines.next() {
                        if command.is_none() {
                            command = Some(arg.to_uppercase());  // Set the command
                        } else {
                            args.push(arg.to_string());  // Set the argument
                        }
                    }
                }
            }

            match command.as_deref() {
                Some("PING") => "+PONG\r\n".to_string(),
                Some("ECHO") => {
                    if let Some(echo_message) = args.get(0) {
                        // Return the argument as a bulk string
                        format!("${}\r\n{}\r\n", echo_message.len(), echo_message)
                    } else {
                        "-ERR missing argument\r\n".to_string()
                    }
                },
                Some("SET") => {
                    if args.len() == 2 {
                        hashmap.insert(args[0].clone(), args[1].clone());
                        "+OK\r\n".to_string()
                    } else {
                        "-ERR wrong number of arguments for 'set' command\r\n".to_string()
                    }
                },
                Some("GET") => {
                    if args.len() == 1 {
                        match hashmap.get(&args[0]) {
                            Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
                            None => "$-1\r\n".to_string()
                        }
                    } else {
                        "-ERR wrong number of arguments for 'get' command\r\n".to_string()
                    }
                },
                _ => "-ERR unknown command\r\n".to_string(),
            }
        } else {
            "-ERR invalid format\r\n".to_string()
        }
    } else {
        "-ERR empty message\r\n".to_string()
    }
}
