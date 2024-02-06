use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use url::Url;
use reqwest::Client;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::{f32::consts::E, sync::Arc};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    vec,
};

mod storage;
use storage::{compress_and_write, read_and_decompress};

static DB_PATH: &str = "../feeder_db";
static FEEDER_GATEWAY_URL: &str = "https://alpha-mainnet.starknet.io";
static MAX_BLOCK_TO_SYNC: u64 = 50;

#[tokio::main]
async fn main() {
    {
        let path = PathBuf::from(DB_PATH);
        if path.is_file() {
            eprintln!("❌ {} is a file", &path.display());
            return;
        } else if path.exists() == false {
            match fs::create_dir(&path) {
                Ok(_) => {
                    println!("✅ Created directory {}", &path.display());
                }
                Err(e) => {
                    eprintln!("❌ Error creating directory {}: {}", &path.display(), e);
                    return;
                }
            }
        }
    }

    let running = Arc::new(AtomicBool::new(true));
    let clone_running = running.clone();
    // Handle SIGINT and change running to false when received
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for SIGINT");
        clone_running.store(false, Ordering::SeqCst);
    });

    let mut set = tokio::task::JoinSet::new();

    let clone_running = running.clone();
    set.spawn(sync_block(0, MAX_BLOCK_TO_SYNC, clone_running));

    let clone_running = running.clone();
    set.spawn(sync_state_update(0, MAX_BLOCK_TO_SYNC, clone_running));

    let clone_running = running.clone();
    set.spawn(sync_class(0, MAX_BLOCK_TO_SYNC, clone_running));

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(handle_request)) });

    let addr = ([127, 0, 0, 1], 3000).into();

    let clone_running = running.clone();

    let server = Server::bind(&addr)
        .serve(make_svc)
        .with_graceful_shutdown(async move {
            while clone_running.load(Ordering::SeqCst) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

    set.spawn(async move {
        match server.await {
            Ok(_) => format!("Server stopped"),
            Err(e) => format!("server: {}", e),
        }
    });

    println!("🟢 Server running on http://{}", addr);

    while let Some(result) = set.join_next().await {
        match result {
            Ok(ret) => {
                println!("🔴 Task stopped: {}", ret);
            }
            Err(e) => {
                eprintln!("❌ Error: {}", e);
            }
        }
    }
}

async fn fetch_data(client: &Client, url: &str) -> anyhow::Result<String> {
    loop {
        let response = client.get(url).send().await?;
        match response.status() {
            StatusCode::OK => match response.text().await {
                Ok(content) => return Ok(content),
                Err(e) => e,
            },
            StatusCode::TOO_MANY_REQUESTS => {
                println!("📈 Too many requests, waiting 5 seconds 💤");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
            e => return Err(anyhow::anyhow!("{}", e)),
        };
    }
}

async fn sync_block(start: u64, end: u64, running: Arc<AtomicBool>) -> String {
    let client = Client::new();

    for block_number in start..=end {
        // Check if a graceful shutdown was requested
        if running.load(Ordering::SeqCst) == false {
            return format!("Synched block {} to {}", start, block_number - 1);
        }
        let path_file = PathBuf::from(format!(
            "{}/{}.gz",
            DB_PATH,
            RequestType::Block(block_number).path()
        ));
        if path_file.exists() {
            continue;
        }

        let url = format!(
            "{}/feeder_gateway/get_block?blockNumber={}",
            FEEDER_GATEWAY_URL, block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => match compress_and_write(&path_file, &content) {
                Ok(_) => {
                    println!("📦 Fetched block {}", block_number);
                }
                Err(e) => {
                    eprintln!("❌ Error writing to file {}: {}", &path_file.display(), e);
                    continue;
                }
            },
            Err(e) => {
                eprintln!("❌ Error fetching block {}: {}", block_number, e);
            }
        }
    }
    format!("Synched block {} to {}", start, end)
}

async fn sync_state_update(start: u64, end: u64, running: Arc<AtomicBool>) -> String {
    let client = Client::new();

    for block_number in start..=end {
        // Check if a graceful shutdown was requested
        if running.load(Ordering::SeqCst) == false {
            return format!(
                "Synched state update from block {} to {}",
                start,
                block_number - 1
            );
        }
        let path_file = PathBuf::from(format!(
            "{}/{}.gz",
            DB_PATH,
            RequestType::StateUpdate(block_number).path()
        ));
        if path_file.exists() {
            continue;
        }

        let url = format!(
            "{}/feeder_gateway/get_state_update?blockNumber={}",
            FEEDER_GATEWAY_URL, block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => match compress_and_write(&path_file, &content) {
                Ok(_) => {
                    println!("🗳️  Fetched state update block {}", block_number);
                }
                Err(e) => {
                    eprintln!("❌ Error writing to file {}: {}", &path_file.display(), e);
                    continue;
                }
            },
            Err(e) => {
                eprintln!(
                    "❌ Error fetching state update block {}: {}",
                    block_number, e
                );
            }
        }
    }
    format!("Synched state update {} to {}", start, end)
}

#[derive(Deserialize)]
struct StateUpdate {
    state_diff: StateDiff,
}

#[derive(Deserialize)]
struct StateDiff {
    deployed_contracts: Vec<Contract>,
    declared_classes: Vec<Class>,
}

#[derive(Deserialize)]
struct Contract {
    class_hash: String,
}

#[derive(Deserialize)]
struct Class {
    class_hash: String,
}

async fn sync_class(start: u64, end: u64, running: Arc<AtomicBool>) -> String {
    let client = Client::new();

    for block_number in start..=end {
        // Check if a graceful shutdown was requested
        if !running.load(Ordering::SeqCst) {
            return format!("Synched class from block {} to {}", start, block_number - 1);
        }

        let path_state_update = PathBuf::from(format!(
            "{}/{}.gz",
            DB_PATH,
            RequestType::StateUpdate(block_number).path()
        ));

        // Check if state update was fetched, if not, skip
        if path_state_update.exists() == false {
            continue;
        }

        let state_update = match read_and_decompress(&path_state_update) {
            Ok(state_update) => state_update,
            Err(e) => {
                eprintln!(
                    "❌ Error reading file {}: {}",
                    &path_state_update.display(),
                    e
                );
                continue;
            }
        };

        let json_state_update: StateUpdate = match serde_json::from_str(&state_update) {
            Ok(json_state_update) => json_state_update,
            Err(e) => {
                eprintln!("❌ Error parsing JSON: {}", e);
                continue;
            }
        };

        let class_hashes = extract_class_hash(&json_state_update.state_diff);

        for hash in class_hashes {
            let path_file = PathBuf::from(format!("{}/class_{}.gz", DB_PATH, hash));
            if path_file.exists() {
                continue;
            }
            let url = format!(
                "{}/feeder_gateway/get_class_by_hash?classHash={}",
                FEEDER_GATEWAY_URL, hash
            );
            match fetch_data(&client, &url).await {
                Ok(content) => match compress_and_write(&path_file, &content) {
                    Ok(_) => {
                        println!("📦 Fetched class {}", hash);
                    }
                    Err(e) => {
                        eprintln!("❌ Error writing to file {}: {}", &path_file.display(), e);
                    }
                },
                Err(e) => {
                    eprintln!("❌ Error fetching class {}: {}", hash, e);
                }
            }
        }
    }

    format!("Synched class from block {} to {}", start, end)
}

fn extract_class_hash(state_diff: &StateDiff) -> Vec<&String> {
    let mut class_hashes = vec![];

    state_diff.deployed_contracts.iter().for_each(|contract| {
        class_hashes.push(&contract.class_hash);
    });
    state_diff.declared_classes.iter().for_each(|class| {
        class_hashes.push(&class.class_hash);
    });

    class_hashes
}
enum RequestType {
    Block(u64),
    StateUpdate(u64),
    Class(String),
    Other(String),
}

impl RequestType {
    fn path(&self) -> String {
        match self {
            RequestType::Block(id) => format!("block_{}", id),
            RequestType::StateUpdate(id) => format!("state_update_{}", id),
            RequestType::Class(hash) => format!("class_{}", hash),
            RequestType::Other(uri) => uri.to_string(),
        }
    }

    fn uri(&self) -> String {
        match self {
            RequestType::Block(id) => format!("get_block?blockNumber={}", id),
            RequestType::StateUpdate(id) => format!("get_state_update?blockNumber={}", id),
            RequestType::Class(hash) => format!("get_class_by_hash?classHash={}", hash),
            RequestType::Other(uri) => uri.to_string(),
        }
    }

    fn from_uri(uri: &str) -> Result<RequestType, &'static str> {
        match uri {
            uri if uri.starts_with("/feeder_gateway/get_block?blockNumber=") => {
                let block_number = block_number_from_path(&uri)?;
                match block_number {
                    block_number if block_number <= MAX_BLOCK_TO_SYNC => {
                        Ok(RequestType::Block(block_number))
                    }
                    _ => Ok(RequestType::Other(uri.to_string())),
                }
            }
            uri if uri.starts_with("/feeder_gateway/get_state_update?blockNumber=") => {
                let block_number = block_number_from_path(&uri)?;
                match block_number {
                    block_number if block_number <= MAX_BLOCK_TO_SYNC => {
                        Ok(RequestType::StateUpdate(block_number))
                    }
                    _ => Ok(RequestType::Other(uri.to_string())),
                }
            }
            uri if uri.starts_with("/feeder_gateway/get_class_by_hash?classHash=") => {
                let class_hash = class_hash_from_path(&uri)?;
                Ok(RequestType::Class(class_hash))
            }
            _ => Ok(RequestType::Other(uri.to_string())),
        }
    }
}

impl std::fmt::Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestType::Block(id) => write!(f, "block {}", id),
            RequestType::StateUpdate(id) => write!(f, "state update {}", id),
            RequestType::Class(hash) => write!(f, "class {}", hash),
            RequestType::Other(uri) => write!(f, "other: {}", uri),
        }
    }
}

async fn handle_request(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
    let begin_time = std::time::Instant::now();
    let uri = req.uri().to_string();

    // Check if URI is valid
    let request_type = match RequestType::from_uri(&uri) {
        Ok(request_type) => request_type,
        Err(e) => {
            eprintln!("❌ Error parsing URI: {}", e);
            return Ok(Response::new(Body::from("Error parsing URI")));
        }
    };

    match request_type {
        RequestType::Block(_) | RequestType::StateUpdate(_) | RequestType::Class(_) => {
            let cache_path = PathBuf::from(format!("{}/{}.gz", DB_PATH, request_type.path()));

            // Check if response is in cache
            if cache_path.exists() {
                // Serve from cache
                match read_and_decompress(&cache_path) {
                    Ok(content) => {
                        let size = content.len();
                        let ret = Response::new(Body::from(content));
                        let elapsed_time = begin_time.elapsed();
                        println!(
                            "📤 Serving from cache, {} ({} Ko, {} µs)",
                            request_type,
                            size / 1024,
                            elapsed_time.as_micros()
                        );
                        return Ok(ret);
                    }
                    Err(e) => {
                        eprintln!("❌ Error reading file {}: {}", &cache_path.display(), e);
                        match fs::remove_file(&cache_path) {
                            Ok(_) => {
                                println!("🗑️ Removed file {}", &cache_path.display());
                            }
                            Err(e) => {
                                eprintln!(
                                    "❌ Error removing file {}: {}",
                                    &cache_path.display(),
                                    e
                                );
                            }
                        }
                        return Ok(Response::new(Body::from("Error from cache")));
                    }
                }
            } else {
                // Fetch from external API and store in cache
                let client = reqwest::Client::new();
                let external_url = format!(
                    "{}/feeder_gateway/{}",
                    FEEDER_GATEWAY_URL,
                    request_type.uri()
                );
                match fetch_data(&client, &external_url).await {
                    Ok(content) => {
                        let size = content.len();
                        let ret = Ok(Response::new(Body::from(content.clone())));
                        let elapsed_time = begin_time.elapsed();
                        println!(
                            "📤 Serving from external API, {} ({} Ko, {} µs)",
                            request_type,
                            size / 1024,
                            elapsed_time.as_micros()
                        );
                        match compress_and_write(&cache_path, &content) {
                            Ok(_) => {
                                println!("📦 Fetched {} and stored in cache", request_type);
                                return ret;
                            }
                            Err(e) => {
                                eprintln!(
                                    "❌ Error writing to file {}: {}",
                                    &cache_path.display(),
                                    e
                                );
                                return ret;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Error fetching {}: {}", request_type.path(), e);
                        return Ok(Response::new(Body::from(
                            "Error fetching from external API",
                        )));
                    }
                }
            }
        }
        // unmatched uri
        RequestType::Other(uri) => {
            println!("❓ Unmatched URI: {}", uri);
            let client = reqwest::Client::new();
            let external_url = format!("{}{}", FEEDER_GATEWAY_URL, uri);
            match fetch_data(&client, &external_url).await {
                Ok(content) => {
                    let size = content.len();
                    let ret = Response::new(Body::from(content));
                    let elapsed_time = begin_time.elapsed();
                    println!(
                        "📤 Serving from external API, {} ({} Ko, {} µs)",
                        uri,
                        size / 1024,
                        elapsed_time.as_micros()
                    );
                    return Ok(ret);
                }
                Err(e) => {
                    eprintln!("❌ Error fetching {}: {}", uri, e);
                    return Ok(Response::new(Body::from(
                        "Error fetching from external API",
                    )));
                }
            }
        }
    }
}

fn block_number_from_path(path: &str) -> Result<u64, &'static str> {
    match Url::parse(format!("http://localhost:3000/{}" ,path).as_str()) {
        Ok(url) => for (key, value) in url.query_pairs() {
            if key == "blockNumber" {
                match value.parse() {
                    Ok(block_number) => return Ok(block_number),
                    Err(_) => return Err("Invalid block number"),
                }
            }
        }
        Err(_) => return Err("Invalid URL"),
    };
    Err("Invalid block number")
}

fn class_hash_from_path(path: &str) -> Result<String, &'static str> {
    match Url::parse(format!("http://localhost:3000/{}" ,path).as_str()) {
        Ok(url) => for (key, value) in url.query_pairs() {
            if key == "classHash" {
                return Ok(value.to_string());
            }
        }
        Err(_) => return Err("Invalid URL"),
    };
    Err("Invalid class hash")
}
