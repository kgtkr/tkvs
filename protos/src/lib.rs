#![deny(warnings)]

pub const REFLECTION_SERVICE_DESCRIPTOR: &[u8] =
    tonic::include_file_descriptor_set!("tkvs-descriptor");

tonic::include_proto!("kgtkr.tkvs");
