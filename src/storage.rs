use rocksdb::{DBCompressionType, Options, DB};
use std::path::PathBuf;
use std::sync::RwLock;

use crate::primitives::{Block, State};

pub struct Storage {
    db: DB,
    max_block_sync: RwLock<Option<Block>>,
    max_state_sync: RwLock<Option<State>>,
}

impl Storage {
    pub fn new(db_path: &PathBuf) -> Result<Storage, String> {
        init_storage(db_path)
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn max_block_sync(&self) -> Option<Block> {
        *self.max_block_sync.read().unwrap()
    }

    pub fn max_state_sync(&self) -> Option<State> {
        *self.max_state_sync.read().unwrap()
    }

    pub fn set_max_block_sync(&self, block: Block) {
        let mut max_block = self.max_block_sync.write().unwrap();
        *max_block = Some(block);
    }

    pub fn set_max_state_sync(&self, state: State) {
        let mut max_state = self.max_state_sync.write().unwrap();
        *max_state = Some(state);
    }
}

// TODO add options to improve performance due to the inmutable nature of the data
fn init_storage(db_path: &PathBuf) -> Result<Storage, String> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Zstd);
    let db = DB::open(&opts, db_path)?;

    let max_block_sync = {
        match is_key_present(&db, &Block(0).key()) {
            true => {
                let mut block = Block(0);
                loop {
                    if !is_key_present(&db, &block.next().key()) {
                        break;
                    }
                    block = block.next();
                }
                Some(block)
            }
            false => None,
        }
    };

    let max_state_sync = {
        match is_key_present(&db, &State(0).key()) {
            true => {
                let mut state = State(0);
                loop {
                    if !is_key_present(&db, &state.next().key()) {
                        break;
                    }
                    state = state.next();
                }
                Some(state)
            }
            false => None,
        }
    };

    Ok(Storage {
        db,
        max_block_sync: RwLock::new(max_block_sync),
        max_state_sync: RwLock::new(max_state_sync),
    })
}

pub fn write_data(db: &DB, key: &str, data: &str) -> Result<(), String> {
    db.put(key.as_bytes(), data)?;
    Ok(())
}

pub fn read_data(db: &DB, key: &str) -> Result<Option<String>, String> {
    let data = db.get(key)?;
    match data {
        Some(value) => Ok(Some(String::from_utf8(value).unwrap())),
        None => Ok(None),
    }
}

pub fn is_key_present(db: &DB, key: &str) -> bool {
    match db.key_may_exist(key) {
        true => matches!(db.get(key), Ok(Some(_))),
        false => false,
    }
}
