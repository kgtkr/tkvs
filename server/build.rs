fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .compile(&["proto/kgtkr/tkvs.proto"], &["proto"])?;
    Ok(())
}
