pub fn parse_redis_message(message: &str) -> String {
    let mut lines = message.lines();
    
    if let Some(line) = lines.next() {
        if line.starts_with('*') {
            // Extract the number of elements (not needed for this case but we should handle it)
            let _num_elements = line[1..].parse::<usize>().unwrap_or(0);
            let mut command = String::new();
            let mut args = Vec::new();

            while let Some(line) = lines.next() {
                if line.starts_with('$') {
                    // Skip the line indicating the length of the next bulk string
                    if let Some(arg) = lines.next() {
                        if command.is_empty() {
                            command = arg.to_uppercase();
                        } else {
                            args.push(arg.to_string());
                        }
                    }
                }
            }

            match command.as_str() {
                "PING" => "+PONG\r\n".to_string(),
                "ECHO" => {
                    if let Some(echo_message) = args.get(0) {
                        format!("${}\r\n{}\r\n", echo_message.len(), echo_message)
                    } else {
                        "-ERR missing argument\r\n".to_string()
                    }
                }
                _ => "-ERR unknown command\r\n".to_string(),
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
