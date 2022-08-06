use crypto::digest::Digest;
use crypto::sha2::Sha256;
use std::io::Read;
use std::io::Write;

/*
データ形式:
sha256,データの長さ(u32, little endian),データ
*/

const HASH_LEN: usize = 32;
const LEN_SIZE: usize = 4;

pub fn append(writer: &mut impl Write, value: &[u8]) -> anyhow::Result<()> {
    // valueの長さチェック
    let mut hasher = Sha256::new();
    hasher.input(value);
    let mut hash = [0; HASH_LEN];
    hasher.result(&mut hash);
    let len = u32::try_from(value.len())?;
    writer.write_all(hash.as_slice())?;
    writer.write_all(len.to_le_bytes().as_slice())?;
    writer.write_all(value)?;
    writer.flush()?;
    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("hash mismatch")]
    HashMismatch,
    #[error("unexpected eof")]
    UnexpectedEof,
}

pub fn read(reader: &mut impl Read) -> anyhow::Result<(Vec<u8>, usize)> {
    fn convert_read_exact_error(err: std::io::Error) -> anyhow::Error {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadError::UnexpectedEof.into()
        } else {
            err.into()
        }
    }

    let mut hash = [0; HASH_LEN];
    reader
        .read_exact(&mut hash)
        .map_err(convert_read_exact_error)?;
    let mut len_buf = [0; LEN_SIZE];
    reader
        .read_exact(&mut len_buf)
        .map_err(convert_read_exact_error)?;

    let len = u32::from_le_bytes(len_buf);
    // TODO: データが壊れていてlenが大きすぎる場合にoomする危険がある
    let mut value = vec![0; len as usize];

    reader
        .read_exact(&mut value)
        .map_err(convert_read_exact_error)?;

    let mut hasher = Sha256::new();
    let mut expect_hash = [0; HASH_LEN];
    hasher.input(value.as_slice());
    hasher.result(&mut expect_hash);
    if hash != expect_hash {
        return Err(ReadError::HashMismatch.into());
    }
    Ok((value, HASH_LEN + LEN_SIZE + len as usize))
}

pub fn read_all(reader: &mut impl Read) -> anyhow::Result<(Vec<Vec<u8>>, usize)> {
    let mut records = Vec::new();
    let mut total = 0;
    loop {
        match read(reader) {
            Ok((value, len)) => {
                records.push(value);
                total += len;
            }
            Err(err) => match err.downcast_ref::<ReadError>() {
                Some(_) => {
                    return Ok((records, total));
                }
                _ => {
                    anyhow::bail!("failed to read log: {}", err);
                }
            },
        }
    }
}
