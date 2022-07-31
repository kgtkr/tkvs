use super::atomic_append;
use anyhow::Context;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
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
    trx_count: usize,
    trxs: HashMap<usize, Trx>,
}

struct Trx {
    write_set: DataBaseWriteSet,
}

impl DB {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        // とりあえずbincodeを使う
        let values = {
            match File::open(data_dir.join("data")) {
                Ok(data_file) => bincode::deserialize_from(BufReader::new(data_file))
                    .context("failed to deserialize data file")?,
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        BTreeMap::new()
                    } else {
                        anyhow::bail!("failed to open data file: {}", err);
                    }
                }
            }
        };

        let logs_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&data_dir.join("logs"))
            .context("failed to open log file")?;

        let write_sets = {
            let mut logs_file_reader = BufReader::new(&logs_file);
            let (records, total) = atomic_append::read_all(&mut logs_file_reader)
                .context("failed to read logs file")?;
            logs_file.set_len(total as u64)?;

            records
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
            .create(true)
            .open(&data_dir.join("logs"))
            .context("failed to open log file")?;

        Ok(DB {
            data_dir,
            values,
            logs_len,
            logs_file,
            trx_count: 0,
            trxs: HashMap::new(),
        })
    }

    pub async fn get(&mut self, trx_id: usize, key: &[u8]) -> Option<Vec<u8>> {
        let trx = self.trxs.get(&trx_id).unwrap();

        trx.write_set
            .get(key)
            .map(|x| x.clone())
            .unwrap_or_else(|| self.values.get(key).cloned())
    }

    pub async fn put(&mut self, trx_id: usize, key: &[u8], value: &[u8]) {
        let trx = self.trxs.get_mut(&trx_id).unwrap();

        trx.write_set.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub async fn delete(&mut self, trx_id: usize, key: &[u8]) {
        let trx = self.trxs.get_mut(&trx_id).unwrap();

        trx.write_set.insert(key.to_vec(), None);
    }

    pub async fn commit(&mut self, trx_id: usize) -> anyhow::Result<()> {
        let trx = self.trxs.get_mut(&trx_id).unwrap();

        let write_set = &trx.write_set;
        apply_write_set(&mut self.values, write_set);
        let write_set_bytes =
            bincode::serialize(write_set).context("failed to serialize write set")?;
        let mut logs_file_writer = BufWriter::new(&self.logs_file);
        atomic_append::append(&mut logs_file_writer, write_set_bytes.as_slice())?;
        drop(logs_file_writer);
        self.logs_file
            .sync_all()
            .context("failed to sync log file")?;
        self.logs_len += 1;
        trx.write_set = BTreeMap::new();
        Ok(())
    }

    pub async fn abort(&mut self, trx_id: usize) {
        let trx = self.trxs.get_mut(&trx_id).unwrap();

        trx.write_set = BTreeMap::new();
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
        data_tmp_file_writer
            .flush()
            .context("failed to flush data")?;
        data_tmp_file.sync_all().context("failed to sync data")?;
        std::fs::rename(&self.data_dir.join("data.tmp"), &self.data_dir.join("data"))
            .context("failed to rename data.tmp to data")?;
        self.logs_file
            .set_len(0)
            .context("failed to truncate logs file")?;
        self.logs_len = 0;
        Ok(())
    }

    pub fn new_trx(&mut self) -> usize {
        let trx_id = self.trx_count;
        self.trx_count += 1;
        self.trxs.insert(
            trx_id,
            Trx {
                write_set: BTreeMap::new(),
            },
        );
        trx_id
    }

    pub fn delete_trx(&mut self, trx_id: usize) {
        self.trxs.remove(&trx_id);
    }
}

/*
#[test]
fn test() {
    use rand::distributions::{Alphanumeric, DistString};
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let dirname: String = (0..8).map(|_| rng.sample(Alphanumeric) as char).collect();
    let data_dir = PathBuf::from("test-data").join(dirname);
    std::fs::create_dir(&data_dir).unwrap();

    {
        let mut db = DB::new(data_dir.clone()).unwrap();
        let trx_id = db.new_trx();
        db.put(trx_id, b"k1", b"v1");
        db.commit(trx_id).unwrap();
        assert_eq!(db.get(trx_id, b"k1"), Some(b"v1".to_vec()));
        db.put(trx_id, b"k2", b"v2");
        db.commit(trx_id).unwrap();
        assert_eq!(db.get(trx_id, b"k1"), Some(b"v1".to_vec()));
    }
}
*/
