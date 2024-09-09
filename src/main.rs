use std::{env, collections::HashMap, path::Path};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task;

mod replication;
mod network;
mod database;
mod commands;
mod parsing;
mod rdb_parser;

use replication::initialize_replication;
use network::start_server;
use database::RedisDatabase;
use rdb_parser::parse_rdb_file;

// Function to initialize the Redis database, including loading from RDB file if available
fn initialize_database(config_map: &HashMap<String, String>) -> RedisDatabase {
    let mut db = RedisDatabase::new();

    // Check for RDB file loading
    if let Some(dir) = config_map.get("dir") {
        if let Some(dbfilename) = config_map.get("dbfilename") {
            let rdb_path = Path::new(dir).join(dbfilename);
            if let Err(e) = parse_rdb_file(rdb_path.to_str().unwrap(), &mut db) {
                println!("Failed to parse RDB file: {}. Starting with an empty database.", e);
            }
        }
    }

    println!("Database data: {:?}", db.data);
    db
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let mut config_map = HashMap::new();

    // Parse command-line arguments into config_map
    let mut i = 1;
    while i < args.len() {
        let key = &args[i];

        if key.starts_with("--") {
            if i + 1 < args.len() {
                let value = args[i + 1].clone();
                let key = key[2..].to_string();
                config_map.insert(key, value);
                i += 2;
            } else {
                eprintln!("Missing value for key: {}", key);
                break;
            }
        } else {
            eprintln!("Invalid argument format: {}", key);
            break;
        }
    }

    let default_port = "6379".to_string();
    let port = config_map.get("port").unwrap_or(&default_port).to_string(); // Capture port

    println!("Starting server with config: {:?}", config_map);

    // Initialize the database
    let db = Arc::new(AsyncMutex::new(initialize_database(&config_map)));

    // Start the server and replication concurrently
    let server_db = Arc::clone(&db);
    let server_config = config_map.clone();
    let server_task = task::spawn(async move {
        if let Err(e) = start_server(server_config, server_db).await {
            eprintln!("Server failed: {}", e);
        }
    });

    let replication_db = Arc::clone(&db);
    let replication_config = config_map.clone();
    let replication_task = task::spawn(async move {
        initialize_replication(&replication_config, replication_db, &port).await;
    });

    // Wait for both the server and replication to finish
    let _ = tokio::try_join!(server_task, replication_task);
}
