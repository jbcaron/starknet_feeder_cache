use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use reqwest::Client;
use std::{path::PathBuf, io::{Write, Read}, u64::MAX};
use tokio::{fs, io};

static DB_PATH: &str = "../feeder_db";
static FEEDER_GATEWAY_URL: &str = "https://alpha-mainnet.starknet.io/feeder_gateway";
static MAX_BLOCK_TO_SYNC: u64 = 100_000;

#[tokio::main]
async fn main() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(sync_block(0, MAX_BLOCK_TO_SYNC / 2));
    set.spawn(sync_block(MAX_BLOCK_TO_SYNC / 2 + 1, MAX_BLOCK_TO_SYNC));
    set.spawn(sync_state_update(0, MAX_BLOCK_TO_SYNC));

    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(handle_request)) });

    let addr = ([127, 0, 0, 1], 3000).into();
    set.spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(make_svc).await {
            eprintln!("server error: {}", e);
        }
    });

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        println!("Running");
    }


    while let resut = set.join_next().await {
        println!("Task completed with result {:?}", resut);
    }
    println!("Shutting down server");
}

async fn fetch_data(client: &Client, url: &str) -> Result<String, String> {
    let response = client.get(url).send().await;
    if response.is_err(){
        return Err(format!("Error fetching data: {}", response.err().unwrap()));
    }
    loop{
        match client.get(url).send().await {
            Ok(response) => {
                match response.status() {
                    StatusCode::OK => match response.text().await{
                        Ok(content) => return Ok(content),
                        Err(e) => Err::<String, String>(format!("Error reading response: {}", e)),
                    },
                    StatusCode::TOO_MANY_REQUESTS => {
                        println!("Too many requests, waiting 1 seconds");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    e => Err::<String, String>(format!("Error status from external API: {}", e)),
                };
            },
            Err(e) => return Err(format!("Error fetching data: {}", e)),
        }
    }
}

async fn sync_block(start: u64, end: u64) {
    let client = Client::new();

    for block_number in start..=end {
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
            FEEDER_GATEWAY_URL,
            block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => {
                let compressed_data = compress(&content);
                write_atomically(&path_file, &compressed_data)
                    .await
                    .err();
                println!("Fetched block {}", block_number);
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }

    }
    println!("block {} to {} done", start, end);
}

async fn sync_state_update(start: u64, end: u64) {
    let client = Client::new();

    for block_number in start..=end {
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
            FEEDER_GATEWAY_URL,
            block_number
        );
        match fetch_data(&client, &url).await {
            Ok(content) => {
                let compressed_data = compress(&content);
                write_atomically(&path_file, &compressed_data)
                    .await
                    .err();
                println!("Fetched state update block {}", block_number);
            }
            Err(e) => {
                println!("Error: {}", e);
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

async fn handle_request(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
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
        RequestType::Block(block_number) | RequestType::StateUpdate(block_number) => {
            let cache_path = PathBuf::from(format!("{}/{}.gz", DB_PATH, request_type.path()));

            // Check if response is in cache
            if cache_path.exists() {
                let cached_response =
                    decompress(&fs::read(&cache_path).await.expect("Unable to read file"));
                println!("Serving from cache, {}", request_type.path());
                return Ok(Response::new(Body::from(cached_response)));
            } else {
                // Fetch from external API and store in cache
                let client = reqwest::Client::new();
                let external_url = format!(
                    "{}/{}",
                    FEEDER_GATEWAY_URL,
                    request_type.uri()
                );
                let external_resp = client
                    .get(&external_url)
                    .send()
                    .await
                    .expect("Unable to fetch");

                let status = external_resp.status();

                let external_resp_text =
                    external_resp.text().await.expect("Unable to read response");

                if status != 200 {
                    println!("Error status from external API: {}", status);
                    return Ok(Response::new(Body::from(external_resp_text)));
                }

                // Write to cache
                write_atomically(&cache_path, external_resp_text.as_bytes())
                    .await
                    .err();

                println!("Fetched block {} and stored in cache", block_number);
                Ok(Response::new(Body::from(external_resp_text)))
            }
        }
        RequestType::Other => {
            println!("Invalid request type");
            Ok(Response::new(Body::from("Invalid request type")))
        }
    }
}

fn compress(data: &str) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

fn decompress(data: &[u8]) -> Vec<u8> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data).unwrap();
    decompressed_data
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

    match fs::write(&temp_file_path, data).await {
        Ok(_) => {}
        Err(e) => {
            println!("Error writing to temp file {}: {}", temp_file_path.display(), e);
            return Err(e);
        }
    }
    match fs::rename(&temp_file_path, file_path).await {
        Ok(_) => {}
        Err(e) => {
            println!("Error renaming temp file {} : {}" , temp_file_path.display(), e);
            return Err(e);
        }
    }
    Ok(())
}
