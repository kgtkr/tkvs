fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile(&["../server/proto/kgtkr/tkvs.proto"], &["../server/proto"])?;
    Ok(())
}
