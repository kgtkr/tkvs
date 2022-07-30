use super::atomic_append;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

type Key = Vec<u8>;
type Value = Vec<u8>;
type DataBaseValues = BTreeMap<Key, Value>;
type DataBaseWriteSet = BTreeMap<Key, Option<Value>>;

fn apply_write_set(values: &mut DataBaseValues, write_set: &DataBaseWriteSet) {
    for (key, value) in write_set {
        if let Some(value) = value {
            values.insert(key.clone(), value.clone());
        } else {
            values.remove(&key.clone());
        }
    }
}

pub struct DB {
    data_dir: PathBuf,
    values: DataBaseValues,
    logs_len: usize,
    logs_file: File,
    write_set: DataBaseWriteSet,
}

impl DB {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        // とりあえずbincodeを使う
        let values = {
            let data_file = File::open("data").context("failed to open data file")?;
            bincode::deserialize_from(BufReader::new(data_file))
                .context("failed to deserialize data file")?
        };

        let write_sets = {
            let logs_file = File::open("logs").context("failed to open logs file")?;
            let mut logs_file_reader = BufReader::new(&logs_file);
            atomic_append::read_all(&mut logs_file_reader)
                .context("failed to read logs file")?
                .into_iter()
                .map(|log| {
                    bincode::deserialize(log.as_slice()).context("failed to deserialize log")
                })
                .collect::<Result<Vec<_>, _>>()
                .context("failed to deserialize logs")?
        };

        let logs_len = write_sets.len();
        let values = write_sets
            .into_iter()
            .fold(values, |mut values, write_set| {
                apply_write_set(&mut values, &write_set);
                values
            });

        let logs_file = OpenOptions::new()
            .append(true)
            .open(&data_dir.join("logs"))
            .context("failed to open log file")?;

        Ok(DB {
            data_dir,
            values,
            logs_len,
            logs_file,
            write_set: BTreeMap::new(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.write_set
            .get(key)
            .map(|x| x.clone())
            .unwrap_or_else(|| self.values.get(key).cloned())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.write_set.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.write_set.insert(key.to_vec(), None);
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        let write_set = self.write_set.clone();
        let write_set_bytes =
            bincode::serialize(&write_set).context("failed to serialize write set")?;
        atomic_append::append(&mut self.logs_file, write_set_bytes.as_slice())?;
        self.logs_file
            .sync_all()
            .context("failed to sync log file")?;
        self.logs_len += 1;
        self.write_set = BTreeMap::new();
        if self.logs_len > 10 {
            self.snapshot().context("failed to snapshot")?;
        }
        Ok(())
    }

    pub fn snapshot(&mut self) -> anyhow::Result<()> {
        let data_tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.data_dir.join("data.tmp"))
            .context("failed to open data.tmp file")?;

        let mut data_tmp_file_writer = BufWriter::new(&data_tmp_file);
        bincode::serialize_into(&mut data_tmp_file_writer, &self.values)
            .context("failed to serialize data")?;
        data_tmp_file.sync_all().context("failed to sync data")?;
        std::fs::rename(&self.data_dir.join("data.tmp"), &self.data_dir.join("data"))
            .context("failed to rename data.tmp to data")?;
        Ok(())
    }

    pub fn abort(&mut self) {
        self.write_set = BTreeMap::new();
    }
}
