use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reflection_descriptor =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("tkvs-descriptor.bin");

    tonic_build::configure()
        .file_descriptor_set_path(&reflection_descriptor)
        .compile(&["proto/kgtkr/tkvs.proto"], &["proto"])?;
    Ok(())
}
