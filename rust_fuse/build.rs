use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let native_lib_dir = manifest_dir.join("native-libs");
    println!("cargo:rustc-link-search=native={}", native_lib_dir.display());
}
