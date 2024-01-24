use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use reqwest::Client;
use std::sync::Arc;
use std::{
    clone,
    sync::atomic::{AtomicBool, Ordering},
};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
use tokio::{fs, io};

static DB_PATH: &str = "../feeder_db";
static FEEDER_GATEWAY_URL: &str = "https://alpha-mainnet.starknet.io/feeder_gateway";
static MAX_BLOCK_TO_SYNC: u64 = 200_000;

#[tokio::main]
async fn main() {
    {
        let path = PathBuf::from(DB_PATH);
        if path.is_file() {
            eprintln!("❌ {} is a file", &path.display());
        } else if path.exists() == false {
            match fs::create_dir(&path).await {
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
            Ok(_) => {
                println!("🔴 Server stopped");
            }
            Err(e) => {
                eprintln!("❌ Error running server: {}", e);
            }
        }
    });

    println!("🟢 Server running on http://{}", addr);

    while let Some(result) = set.join_next().await {
        match result {
            Ok(_) => {
                println!("🔴 Task stopped");
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
                println!("📈 Too many requests, waiting 1 second 💤");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            e => return Err(anyhow::anyhow!("{}", e)),
        };
    }
}

async fn compress_and_write(file_path: &PathBuf, data: &str) -> io::Result<()> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes())?;
    let compressed_data = encoder.finish()?;
    write_atomically(file_path, &compressed_data).await?;
    Ok(())
}

async fn read_and_decompress(file_path: &PathBuf) -> io::Result<String> {
    let compressed_data = fs::read(&file_path).await?;
    let mut decoder = GzDecoder::new(&compressed_data[..]);
    let mut decompressed_data = String::new();
    decoder.read_to_string(&mut decompressed_data)?;
    Ok(decompressed_data)
}

async fn sync_block(start: u64, end: u64, running: Arc<AtomicBool>) {
    let client = Client::new();

    for block_number in start..=end {
        // Check if a graceful shutdown was requested
        if !running.load(Ordering::SeqCst) {
            println!("block {} to {} done", start, block_number - 1);
            return;
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
            "{}/get_block?blockNumber={}",
            FEEDER_GATEWAY_URL, block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => {
                match compress_and_write(&path_file, &content).await {
                    Ok(_) => {
                        println!("📦 Fetched block {}", block_number);
                    }
                    Err(e) => {
                        eprintln!("❌ Error writing to file {}: {}", &path_file.display(), e);
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Error fetching block {}: {}", block_number, e);
            }
        }
    }
    println!("block {} to {} done", start, end);
}

async fn sync_state_update(start: u64, end: u64, running: Arc<AtomicBool>) {
    let client = Client::new();

    for block_number in start..=end {
        // Check if a graceful shutdown was requested
        if !running.load(Ordering::SeqCst) {
            println!("state update {} to {} done", start, block_number - 1);
            return;
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
            "{}/get_state_update?blockNumber={}",
            FEEDER_GATEWAY_URL, block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => {
                match compress_and_write(&path_file, &content).await {
                    Ok(_) => {
                        println!("🗳️  Fetched state update block {}", block_number);
                    }
                    Err(e) => {
                        eprintln!("❌ Error writing to file {}: {}", &path_file.display(), e);
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Error fetching state update block {}: {}", block_number, e);
            }
        }
    }
    println!("state update {} to {} done", start, end);
}

enum RequestType {
    Block(u64),
    StateUpdate(u64),
    Other,
}

impl RequestType {
    fn path(&self) -> String {
        match self {
            RequestType::Block(id) => format!("block_{}", id),
            RequestType::StateUpdate(id) => format!("state_update_{}", id),
            RequestType::Other => String::from(""),
        }
    }
    fn uri(&self) -> String {
        match self {
            RequestType::Block(id) => format!("get_block?blockNumber={}", id),
            RequestType::StateUpdate(id) => format!("get_state_update?blockNumber={}", id),
            RequestType::Other => String::from(""),
        }
    }
}

impl std::fmt::Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestType::Block(id) => write!(f, "block {}", id),
            RequestType::StateUpdate(id) => write!(f, "state update {}", id),
            RequestType::Other => write!(f, "other"),
        }
    }
}

async fn handle_request(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let uri = req.uri().to_string();

    // Check if URI is valid
    let request_type = match uri {
        uri if uri.starts_with("/feeder_gateway/get_block") => {
            RequestType::Block(match block_number_from_path(&uri) {
                Ok(block_number) => block_number,
                Err(e) => return Ok(Response::new(Body::from(e))),
            })
        }
        uri if uri.starts_with("/feeder_gateway/get_state_update") => {
            RequestType::StateUpdate(match block_number_from_path(&uri) {
                Ok(block_number) => block_number,
                Err(e) => return Ok(Response::new(Body::from(e))),
            })
        }
        _ => RequestType::Other,
    };

    match request_type {
        RequestType::Block(_) | RequestType::StateUpdate(_) => {
            let cache_path = PathBuf::from(format!("{}/{}.gz", DB_PATH, request_type.path()));

            // Check if response is in cache
            if cache_path.exists() {
                // Serve from cache
                match read_and_decompress(&cache_path).await {
                    Ok(content) => {
                        println!("📤 Serving from cache, {}", request_type.path());
                        return Ok(Response::new(Body::from(content)));
                    }
                    Err(e) => {
                        eprintln!("❌ Error reading file {}: {}", &cache_path.display(), e);
                        return Ok(Response::new(Body::from("Error from cache")));
                    }
                }
            } else {
                // Fetch from external API and store in cache
                let client = reqwest::Client::new();
                let external_url = format!("{}/{}", FEEDER_GATEWAY_URL, request_type.uri());
                match fetch_data(&client, &external_url).await {
                    Ok(content) => {
                        let ret = Ok(Response::new(Body::from(content.clone())));
                        match compress_and_write(&cache_path, &content).await {
                            Ok(_) => {
                                println!("📦 Fetched {} and stored in cache", request_type);
                                return ret;
                            }
                            Err(e) => {
                                eprintln!("❌ Error writing to file {}: {}", &cache_path.display(), e);
                                return ret;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Error fetching {}: {}", request_type.path(), e);
                        return Ok(Response::new(Body::from("Error fetching from external API")));
                    }
                }
            }
        }
        RequestType::Other => {
            println!("🚫 Invalid request type");
            Ok(Response::new(Body::from("Invalid request type")))
        }
    }
}

fn compress(data: &str) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes())?;
    Ok(encoder.finish()?)
}

fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

fn block_number_from_path(path: &str) -> Result<u64, &'static str> {
    match path.split("=").last() {
        Some(block_number) => match block_number.parse() {
            Ok(block_number) => Ok(block_number),
            Err(_) => return Err("Invalid block number"),
        },
        None => return Err("Invalid block number"),
    }
}

async fn write_atomically(file_path: &PathBuf, data: &[u8]) -> io::Result<()> {
    let temp_file_path = file_path.with_extension("tmp");

    fs::write(&temp_file_path, data).await?;
    fs::rename(&temp_file_path, file_path).await?;
    Ok(())
}
