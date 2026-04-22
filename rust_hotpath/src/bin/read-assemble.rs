use std::env;
use std::io::{self, BufRead};
use std::io::Write;
use std::process;

use dbfs_rust_hotpath::assemble_read_slice;

fn parse_u64(arg: &str, name: &str) -> Result<u64, String> {
    arg.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", name, arg))
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.len() % 2 != 0 {
        return Err(format!("invalid hex length: {}", trimmed.len()));
    }

    let mut bytes = Vec::with_capacity(trimmed.len() / 2);
    let chars: Vec<char> = trimmed.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let hi = chars[idx]
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit: {}", chars[idx]))?;
        let lo = chars[idx + 1]
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit: {}", chars[idx + 1]))?;
        bytes.push(((hi << 4) | lo) as u8);
        idx += 2;
    }
    Ok(bytes)
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 5 {
        return Err(
            "Usage: read-assemble <fetch_first> <fetch_last> <offset> <end_offset> <block_size>"
                .to_string(),
        );
    }

    let fetch_first = parse_u64(&args[0], "fetch_first")?;
    let fetch_last = parse_u64(&args[1], "fetch_last")?;
    let offset = parse_u64(&args[2], "offset")?;
    let end_offset = parse_u64(&args[3], "end_offset")?;
    let block_size = parse_u64(&args[4], "block_size")?;

    let stdin = io::stdin();
    let mut blocks = Vec::new();
    for line in stdin.lock().lines() {
        let line = line.map_err(|err| err.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let (block_index_raw, block_hex) = line
            .split_once('|')
            .ok_or_else(|| format!("invalid input line: {}", line))?;
        let block_index = parse_u64(block_index_raw, "block_index")?;
        blocks.push((block_index, decode_hex(block_hex)?));
    }

    let output = assemble_read_slice(fetch_first, fetch_last, offset, end_offset, block_size, &blocks);
    io::stdout()
        .write_all(&output)
        .map_err(|err| err.to_string())?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(2);
    }
}
