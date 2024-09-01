pub fn parse_redis_message(message: &str) -> Option<String> {
    // We want our commands to be case-insensitive
    // So we convert the message to uppercase
    // But not the whole message, only the command part
    let mut parts = message.split_whitespace();
    let command = parts.next()?.to_uppercase();

    match command.as_str() {
        "PING" => Some("+PONG\r\n".to_string()),
        "ECHO" => {
            // Reconstruct the remaining part of the message after "ECHO"
            let echo_message: String = parts.collect::<Vec<&str>>().join(" ");
            Some(format!("+{}\r\n", echo_message))
        }
        _ => None, // Handle other commands or return None if the command is unknown
    }
}
