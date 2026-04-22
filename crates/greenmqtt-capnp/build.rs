fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../capnp/greenmqtt_rpc.capnp");
    println!("cargo:rerun-if-changed=build.rs");

    capnpc::CompilerCommand::new()
        .file("../../capnp/greenmqtt_rpc.capnp")
        .src_prefix("../../capnp")
        .import_path("../../capnp")
        .run()?;

    Ok(())
}
