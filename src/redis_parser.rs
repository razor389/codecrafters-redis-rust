pub fn parse_redis_message(message: &str) -> String {
    let mut lines = message.lines();
    
    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            // Parse the number of elements in the array (though we don't need to use this)
            let _num_elements = line[1..].parse::<usize>().unwrap_or(0);
            let mut command = None;
            let mut args = Vec::new();

            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    // Skip the line indicating the length of the next bulk string
                    if let Some(arg) = lines.next() {
                        if command.is_none() {
                            // Set the command if not already set
                            command = Some(arg.to_uppercase());
                        } else {
                            // Add the argument to the list
                            args.push(arg.to_string());
                        }
                    }
                }
            }

            // Debugging output
            println!("Command parsed: {:?}", command);
            println!("Arguments parsed: {:?}", args);

            match command.as_deref() {
                Some("PING") => "+PONG\r\n".to_string(),
                Some("ECHO") => {
                    if let Some(echo_message) = args.get(0) {
                        format!("{}\r\n", echo_message)
                    } else {
                        "-ERR missing argument\r\n".to_string()
                    }
                }
                _ => {
                    println!("Unknown command: {:?}", command);
                    "-ERR unknown command\r\n".to_string()
                },
            }
        } else {
            "-ERR invalid format\r\n".to_string()
        }
    } else {
        "-ERR empty message\r\n".to_string()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        // RESP format for PING command
        assert_eq!(parse_redis_message("*1\r\n$4\r\nPING\r\n"), "+PONG\r\n".to_string());
    }

    #[test]
    fn test_echo() {
        // RESP format for ECHO command
        assert_eq!(
            parse_redis_message("*2\r\n$4\r\nECHO\r\n$13\r\nHello, World!\r\n"),
            "$13\r\nHello, World!\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("*2\r\n$4\r\nECHO\r\n$14\r\nthis is a test\r\n"),
            "$14\r\nthis is a test\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("*2\r\n$4\r\nECHO\r\n$20\r\n  multiple    spaces\r\n"),
            "$20\r\n  multiple    spaces\r\n".to_string()
        );
        assert_eq!(parse_redis_message("*2\r\n$4\r\nECHO\r\n$0\r\n\r\n"), "$0\r\n\r\n".to_string());
        assert_eq!(parse_redis_message("*2\r\n$4\r\nECHO\r\n$3\r\n   \r\n"), "$3\r\n   \r\n".to_string()); 
    }

    #[test]
    fn test_unknown_command() {
        // RESP format for unknown command
        assert_eq!(
            parse_redis_message("*1\r\n$7\r\nUNKNOWN\r\n"),
            "-ERR unknown command\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("*1\r\n$3\r\nGET\r\n"),
            "-ERR unknown command\r\n".to_string()
        );
    }
}
