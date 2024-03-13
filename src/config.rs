use clap::Parser;

#[derive(Debug, Parser)]
pub struct Config {
    #[clap(long, default_value = "../feeder_db")]
    pub db_path: String,

    #[clap(long, default_value = "https://alpha-mainnet.starknet.io")]
    pub feeder_gateway_url: String,

    #[clap(long, default_value_t = 600000)]
    pub max_block_to_sync: u64,

    #[clap(long, default_value = "127.0.0.1:3000")]
    pub server_addr: String,
}

impl Config {
    pub fn new() -> Config {
        Config::parse()
    }
}
