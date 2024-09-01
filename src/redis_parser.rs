pub fn parse_redis_message(message: &str) -> String {
    // Remove leading whitespace to correctly identify the command
    let message = message.trim_start();

    // Split the message into two parts: the command and the rest
    let mut parts = message.splitn(2, ' ');
    let command = parts.next().unwrap_or("").to_uppercase();

    match command.as_str() {
        "PING" => "+PONG\r\n".to_string(),
        "ECHO" => {
            // Preserve the entire remaining part of the message after "ECHO"
            let echo_message = parts.next().unwrap_or("");
            // Return as a bulk string
            format!("${}\r\n{}\r\n", echo_message.len(), echo_message)
        }
        _ => "-ERR unknown command\r\n".to_string(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        assert_eq!(parse_redis_message("PING"), "+PONG\r\n".to_string());
        assert_eq!(parse_redis_message("ping"), "+PONG\r\n".to_string());
        assert_eq!(parse_redis_message("   PING   "), "+PONG\r\n".to_string());
    }

    #[test]
    fn test_echo() {
        assert_eq!(
            parse_redis_message("ECHO Hello, World!"),
            "+Hello, World!\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("echo this is a test"),
            "+this is a test\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("   ECHO   multiple    spaces"),
            "+  multiple    spaces\r\n".to_string()
        );
        assert_eq!(parse_redis_message("ECHO"), "+\r\n".to_string());
        assert_eq!(parse_redis_message("ECHO    "), "+   \r\n".to_string()); // This test should pass now
    }

    #[test]
    fn test_unknown_command() {
        assert_eq!(
            parse_redis_message("UNKNOWN"),
            "-ERR unknown command\r\n".to_string()
        );
        assert_eq!(
            parse_redis_message("GET"),
            "-ERR unknown command\r\n".to_string()
        );
    }
}
