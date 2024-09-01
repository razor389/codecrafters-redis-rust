// redis_parser.rs

pub fn parse_redis_message(message: &str) -> Option<String> {
    // Trim leading whitespace
    let trimmed = message.trim_start();
    
    // Split the message into command and the rest (up to 2 parts)
    let mut parts = trimmed.splitn(2, ' ');
    let command = parts.next()?.to_uppercase();

    match command.as_str() {
        "PING" => Some("+PONG\r\n".to_string()),
        "ECHO" => {
            // Get the rest of the message after "ECHO"
            let echo_message = parts.next().unwrap_or("");
            // Preserve the original spacing in the echo message
            Some(format!("+{}\r\n", echo_message))
        }
        _ => None, // Handle other commands or return None if the command is unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        assert_eq!(parse_redis_message("PING"), Some("+PONG\r\n".to_string()));
        assert_eq!(parse_redis_message("ping"), Some("+PONG\r\n".to_string()));
        assert_eq!(parse_redis_message("   PING   "), Some("+PONG\r\n".to_string()));
    }

    #[test]
    fn test_echo() {
        assert_eq!(
            parse_redis_message("ECHO Hello, World!"),
            Some("+Hello, World!\r\n".to_string())
        );
        assert_eq!(
            parse_redis_message("echo this is a test"),
            Some("+this is a test\r\n".to_string())
        );
        assert_eq!(
            parse_redis_message("   ECHO   multiple    spaces"),
            Some("+  multiple    spaces\r\n".to_string())
        );
        assert_eq!(parse_redis_message("ECHO"), Some("+\r\n".to_string()));
        assert_eq!(parse_redis_message("ECHO    "), Some("+   \r\n".to_string()));
    }

    #[test]
    fn test_unknown_command() {
        assert_eq!(parse_redis_message("UNKNOWN"), None);
        assert_eq!(parse_redis_message("GET"), None);
    }
}
