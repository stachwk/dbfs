use std::env;
use std::process;

use dbfs_rust_hotpath::pack_changed_ranges;

fn parse_u64(arg: &str, name: &str) -> Result<u64, String> {
    arg.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", name, arg))
}

fn parse_mask(arg: &str) -> Result<Vec<bool>, String> {
    if arg.is_empty() {
        return Ok(Vec::new());
    }

    let mut mask = Vec::new();
    for part in arg.split(',') {
        match part {
            "0" | "false" | "False" => mask.push(false),
            "1" | "true" | "True" => mask.push(true),
            other => return Err(format!("invalid mask value: {}", other)),
        }
    }
    Ok(mask)
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("Usage: copy-pack <off_out> <block_size> <changed_mask>".to_string());
    }

    let off_out = parse_u64(&args[0], "off_out")?;
    let block_size = parse_u64(&args[1], "block_size")?;
    let mask = parse_mask(&args[2])?;

    for (start, end) in pack_changed_ranges(off_out, block_size, &mask) {
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
