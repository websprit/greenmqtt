fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/greenmqtt_internal.proto");
    println!("cargo:rerun-if-changed=build.rs");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/greenmqtt_internal.proto"], &["../../proto"])?;
    Ok(())
}
