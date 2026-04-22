use std::env;
use std::io::{self, Read, Write};
use std::process;

use dbfs_rust_hotpath::pad_block_bytes;

fn parse_u64(arg: &str, name: &str) -> Result<u64, String> {
    arg.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", name, arg))
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("Usage: persist-pad <used_len> <block_size>".to_string());
    }

    let used_len = parse_u64(&args[0], "used_len")?;
    let block_size = parse_u64(&args[1], "block_size")?;

    let mut payload = Vec::new();
    io::stdin()
        .read_to_end(&mut payload)
        .map_err(|err| err.to_string())?;

    let padded = pad_block_bytes(&payload, used_len, block_size);
    io::stdout()
        .write_all(&padded)
        .map_err(|err| err.to_string())?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(2);
    }
}
