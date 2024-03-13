use actix_web::middleware::Logger;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};

mod class_extract;
mod config;
mod primitives;
mod storage;

use crate::primitives::{Block, Class, State};
use class_extract::extract_class_hash;
use storage::{is_key_present, read_data, write_data, Storage};

#[actix_web::main]
async fn main() {
    env_logger::init();
    let config = config::Config::new();

    let storage = match Storage::new(&PathBuf::from(config.db_path)) {
        Ok(storage) => Arc::new(storage),
        Err(e) => {
            log::error!("❌ Error initializing storage: {}", e);
            return;
        }
    };

    log::info!("💾 Storage initialized");
    if let Some(max_block_sync) = storage.max_block_sync() {
        log::info!("📦 Max block to sync: {}", max_block_sync);
    }
    if let Some(max_state_sync) = storage.max_state_sync() {
        log::info!("📦 Max state to sync: {}", max_state_sync);
    }
    log::info!("🔗 Feeder gateway URL: {}", config.feeder_gateway_url);

    let run = Arc::new(AtomicBool::new(true));
    let run_clone = run.clone();

    // Handle SIGINT and change run to false when received
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for SIGINT");
        run_clone.store(false, Ordering::SeqCst);
    });

    let mut set = tokio::task::JoinSet::new();

    let run_clone = run.clone();
    let storage_clone = storage.clone();
    set.spawn(sync_block(
        config.max_block_to_sync,
        run_clone,
        storage_clone,
        config.feeder_gateway_url.clone(),
    ));

    let run_clone = run.clone();
    let storage_clone = storage.clone();
    set.spawn(sync_state_update(
        config.max_block_to_sync,
        run_clone,
        storage_clone,
        config.feeder_gateway_url.clone(),
    ));

    let run_clone = run.clone();
    let storage_clone = storage.clone();
    set.spawn(sync_class(
        0,
        config.max_block_to_sync,
        run_clone,
        storage_clone,
        config.feeder_gateway_url.clone(),
    ));

    let storage_clone = storage.clone();
    let data = web::Data::new(storage_clone);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::clone(&data))
            .route("/feeder_gateway/get_block", web::get().to(get_block))
            .route(
                "/feeder_gateway/get_state_update",
                web::get().to(get_state_update),
            )
            .route(
                "/feeder_gateway/get_class_by_hash",
                web::get().to(get_class_by_hash),
            )
            .wrap(Logger::default())
            .route("/", web::get().to(index))
    })
    .bind(&config.server_addr)
    .expect("Failed to bind server to address")
    .run();

    let server_handle = server.handle();

    actix_web::rt::spawn(server);

    log::info!("🟢 Server running on http://{}", &config.server_addr);

    let run_clone = run.clone();
    set.spawn(async move {
        while run_clone.load(Ordering::SeqCst) {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        server_handle.stop(false).await;
        "server stop".to_string()
    });

    while let Some(result) = set.join_next().await {
        match result {
            Ok(ret) => {
                log::info!("🔴 Task stopped: {}", ret);
            }
            Err(e) => {
                log::error!("❌ Error: {}", e);
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
                log::info!("📈 Too many requests, waiting 5 seconds 💤");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
            e => return Err(anyhow::anyhow!("{}", e)),
        };
    }
}

async fn sync_block(
    end: u64,
    running: Arc<AtomicBool>,
    storage: Arc<Storage>,
    feeder: String,
) -> String {
    let client = Client::new();

    let start = match storage.max_block_sync() {
        Some(block) => block.next(),
        None => Block(0),
    };

    if start.0 > end {
        return "No block to sync".to_string();
    }

    let mut block = start;
    loop {
        // Check if a graceful shutdown was requested or sync is finished
        if !running.load(Ordering::SeqCst) || block.0 > end {
            break;
        }

        let url = format!(
            "{}/feeder_gateway/get_block?blockNumber={}",
            feeder, block.0
        );
        match fetch_data(&client, &url).await {
            Ok(content) => match write_data(storage.db(), &block.key(), &content) {
                Ok(_) => {
                    log::info!("📦 Fetched block {}", block.0);
                    storage.set_max_block_sync(block);
                    block = block.next();
                }
                Err(e) => {
                    return format!("❌ Error writing to DB {}: {}", &block.key(), e);
                }
            },
            Err(e) => {
                log::error!("❌ Error fetching block {}: {}", block.0, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await
            }
        }
    }

    format!("Synched block {} to {}", start.0, block.0)
}

async fn sync_state_update(
    end: u64,
    running: Arc<AtomicBool>,
    storage: Arc<Storage>,
    feeder: String,
) -> String {
    let client = Client::new();

    let start = match storage.max_state_sync() {
        Some(state) => state.next(),
        None => State(0),
    };

    if start.0 > end {
        return "No state update to sync".to_string();
    }

    let mut state = start;
    loop {
        // Check if a graceful shutdown was requested or sync is finished
        if !running.load(Ordering::SeqCst) || state.0 > end {
            break;
        }

        let url = format!(
            "{}/feeder_gateway/get_state_update?blockNumber={}",
            feeder, state.0
        );
        match fetch_data(&client, &url).await {
            Ok(content) => match write_data(storage.db(), &state.key(), &content) {
                Ok(_) => {
                    log::info!("📦 Fetched state update {}", state.0);
                    storage.set_max_state_sync(state);
                    state = state.next();
                }
                Err(e) => {
                    return format!("❌ Error writing to DB {}: {}", &state.key(), e);
                }
            },
            Err(e) => {
                log::error!("❌ Error fetching state update {}: {}", state.0, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await
            }
        }
    }

    format!("Synched state update {} to {}", start.0, state.0)
}

async fn sync_class(
    start: u64,
    end: u64,
    running: Arc<AtomicBool>,
    storage: Arc<Storage>,
    feeder: String,
) -> String {
    let client = Client::new();

    let mut state = State(start);
    loop {
        // Check if a graceful shutdown was requested
        if !running.load(Ordering::SeqCst) || state.0 > end {
            break;
        }

        let state_update = match read_data(storage.db(), &state.key()) {
            Ok(state_update) => match state_update {
                Some(state_update) => state_update,
                None => {
                    log::info!("💾 State update {} not found, 💤 waiting 5 sec", state);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
            },
            Err(e) => {
                log::error!("❌ Error reading state update {}: {}", state, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let class_hashes = match extract_class_hash(&state_update) {
            Ok(class_hashes) => class_hashes,
            Err(e) => {
                log::error!(
                    "❌ Error extracting class hashes from state update {}: {}",
                    state,
                    e
                );
                continue;
            }
        };

        state = state.next();

        for hash in class_hashes {
            let class = Class(hash.to_string());
            if is_key_present(storage.db(), &class.key()) {
                continue;
            }
            let url = format!(
                "{}/feeder_gateway/get_class_by_hash?classHash={}",
                feeder, hash
            );
            match fetch_data(&client, &url).await {
                Ok(content) => match write_data(storage.db(), &class.key(), &content) {
                    Ok(_) => {
                        log::info!("📦 Fetched class {}", hash);
                    }
                    Err(e) => {
                        log::error!("❌ Error writing to DB {}: {}", &class.key(), e);
                    }
                },
                Err(e) => {
                    log::error!("❌ Error fetching class {}: {}", hash, e);
                }
            }
        }
    }

    format!("Synched class from block {} to {}", start, end)
}

async fn index(storage: web::Data<Arc<Storage>>) -> impl Responder {
    log::info!("🔗 Request received");
    let max_block_sync = storage.max_block_sync().unwrap_or(Block(0));
    let max_state_sync = storage.max_state_sync().unwrap_or(State(0));

    format!(
        "Max block to sync: {}, max state to sync: {}\n",
        max_block_sync, max_state_sync
    )
}

#[derive(Deserialize)]
struct BlockNumber {
    block_number: u64,
}

async fn get_block(
    storage: web::Data<Arc<Storage>>,
    web::Query(block_number): web::Query<BlockNumber>,
) -> impl Responder {
    let block = Block(block_number.block_number);
    match read_data(storage.db(), &block.key()) {
        Ok(block) => match block {
            Some(block) => HttpResponse::Ok().body(block),
            None => HttpResponse::NotFound().body("Block not found"),
        },
        Err(e) => {
            log::error!("❌ Error reading block {}: {}", block, e);
            HttpResponse::InternalServerError().body("Error reading block")
        }
    }
}

#[derive(Deserialize)]
struct StateUpdate {
    block_number: u64,
}

async fn get_state_update(
    storage: web::Data<Arc<Storage>>,
    web::Query(state_update): web::Query<StateUpdate>,
) -> impl Responder {
    let state = State(state_update.block_number);
    match read_data(storage.db(), &state.key()) {
        Ok(state) => match state {
            Some(state) => HttpResponse::Ok().body(state),
            None => HttpResponse::NotFound().body("State update not found"),
        },
        Err(e) => {
            log::error!("❌ Error reading state update {}: {}", state, e);
            HttpResponse::InternalServerError().body("Error reading state update")
        }
    }
}

#[derive(Deserialize)]
struct ClassHash {
    class_hash: String,
}

async fn get_class_by_hash(
    storage: web::Data<Arc<Storage>>,
    web::Query(class_hash): web::Query<ClassHash>,
) -> impl Responder {
    let class = Class(class_hash.class_hash);
    match read_data(storage.db(), &class.key()) {
        Ok(class) => match class {
            Some(class) => HttpResponse::Ok().body(class),
            None => HttpResponse::NotFound().body("Class not found"),
        },
        Err(e) => {
            log::error!("❌ Error reading class {}: {}", class, e);
            HttpResponse::InternalServerError().body("Error reading class")
        }
    }
}
