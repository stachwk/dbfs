use std::env;
use std::process;

use dbfs_rust_hotpath::copy_segments;

fn parse_u64(arg: &str, name: &str) -> Result<u64, String> {
    arg.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", name, arg))
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 5 {
        return Err(
            "Usage: copy-plan <off_in> <off_out> <length> <block_size> <workers>".to_string(),
        );
    }

    let off_in = parse_u64(&args[0], "off_in")?;
    let off_out = parse_u64(&args[1], "off_out")?;
    let length = parse_u64(&args[2], "length")?;
    let block_size = parse_u64(&args[3], "block_size")?;
    let workers = parse_u64(&args[4], "workers")?;

    for (src, dst, len) in copy_segments(off_in, off_out, length, block_size, workers) {
        println!("{},{},{}", src, dst, len);
    }

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        process::exit(2);
    }
}
