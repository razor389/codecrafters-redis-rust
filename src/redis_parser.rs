pub fn parse_redis_message(message: &str) -> Option<String> {
    //we want our commands to be case-insensitive
    //so we convert the message to uppercase
    match message.trim() {
        "PING" => Some("+PONG\r\n".to_string()),
        //for ECHO command we need to return the message itself
        //we can use the format! macro to create a new string
        //with the message
        message if message.starts_with("ECHO") => {
            let echo_message = message.trim_start_matches("ECHO ");
            Some(format!("+{}\r\n", echo_message))
        }
        _ => None, // Handle other commands or return None if the command is unknown
    }
}
