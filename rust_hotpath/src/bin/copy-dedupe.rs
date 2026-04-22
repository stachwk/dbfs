use std::env;
use std::io::{self, BufRead};
use std::process;

use dbfs_rust_hotpath::pack_changed_copy_pairs;

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
    if args.len() != 3 {
        return Err("Usage: copy-dedupe <dst_offset> <total_len> <block_size>".to_string());
    }

    let dst_offset = parse_u64(&args[0], "dst_offset")?;
    let total_len = parse_u64(&args[1], "total_len")?;
    let block_size = parse_u64(&args[2], "block_size")?;

    let stdin = io::stdin();
    let mut pairs = Vec::new();
    for line in stdin.lock().lines() {
        let line = line.map_err(|err| err.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let (payload_hex, current_hex) = line
            .split_once('|')
            .ok_or_else(|| format!("invalid input line: {}", line))?;
        let payload = decode_hex(payload_hex)?;
        let current = decode_hex(current_hex)?;
        pairs.push((payload, current));
    }

    for (start, end) in pack_changed_copy_pairs(dst_offset, total_len, block_size, &pairs) {
        println!("{},{}", start, end);
    }

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(2);
    }
}
