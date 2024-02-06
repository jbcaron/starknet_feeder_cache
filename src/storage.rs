use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use std::fs;
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;

pub fn compress_and_write(file_path: &PathBuf, data: &str) -> io::Result<()> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes())?;
    let compressed_data = encoder.finish()?;
    write_atomically(file_path, &compressed_data)?;
    Ok(())
}

pub fn read_and_decompress(file_path: &PathBuf) -> io::Result<String> {
    let compressed_data = fs::read(&file_path)?;
    let mut decoder = GzDecoder::new(&compressed_data[..]);
    let mut decompressed_data = String::new();
    decoder.read_to_string(&mut decompressed_data)?;
    Ok(decompressed_data)
}

fn write_atomically(file_path: &PathBuf, data: &[u8]) -> io::Result<()> {
    let temp_file_path = file_path.with_extension("tmp");

    fs::write(&temp_file_path, data)?;
    fs::rename(&temp_file_path, file_path)?;
    Ok(())
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
