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

pub fn append(writer: &mut impl Write, value: &[u8]) -> std::io::Result<()> {
    let mut hasher = Sha256::new();
    hasher.input(value);
    let mut hash = [0; HASH_LEN];
    hasher.result(&mut hash);
    writer.write_all(hash.as_slice())?;
    writer.write_all((value.len() as u32).to_le_bytes().as_slice())?;
    writer.write_all(value)?;
    Ok(())
}

pub fn read(reader: &mut impl Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut hash = [0; HASH_LEN];
    if reader.read(&mut hash)? != HASH_LEN {
        return Ok(None);
    }
    let mut len_buf = [0; LEN_SIZE];
    if reader.read(&mut len_buf)? != LEN_SIZE {
        return Ok(None);
    }

    let len = u32::from_le_bytes(len_buf);
    // TODO: データが壊れていてlenが大きすぎる場合にoomする危険がある
    let mut value = vec![0; len as usize];

    if reader.read(&mut value)? != len as usize {
        return Ok(None);
    }

    let mut hasher = Sha256::new();
    let mut expect_hash = [0; HASH_LEN];
    hasher.input(value.as_slice());
    hasher.result(&mut expect_hash);
    if hash != expect_hash {
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn read_all(reader: &mut impl Read) -> std::io::Result<Vec<Vec<u8>>> {
    let mut records = Vec::new();
    loop {
        if let Some(record) = read(reader)? {
            records.push(record);
        } else {
            return Ok(records);
        }
    }
}
