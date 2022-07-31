use super::atomic_append;
use super::lock::Lock;
use anyhow::Context;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

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

struct DBInner {
    data_dir: PathBuf,
    values: DataBaseValues,
    logs_len: usize,
    logs_file: File,
    trx_count: usize,
    trxs: HashMap<usize, Arc<Mutex<Trx>>>,
    locks: HashMap<Key, Lock>,
}

#[derive(Clone)]
pub struct DB(Arc<Mutex<DBInner>>);

struct Trx {
    write_set: DataBaseWriteSet,
    locks: HashMap<Key, Lock>,
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

        Ok(DB(Arc::new(Mutex::new(DBInner {
            data_dir,
            values,
            logs_len,
            logs_file,
            trx_count: 0,
            trxs: HashMap::new(),
            locks: HashMap::new(),
        }))))
    }

    pub async fn get(&self, trx_id: usize, key: &[u8]) -> Option<Vec<u8>> {
        let mut db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let mut trx = trx.try_lock().unwrap();

        let lock = db.locks.entry(key.to_vec()).or_insert(Lock::new()).clone();
        trx.locks.insert(key.to_vec(), lock.clone());

        drop(db);
        lock.lock_read(trx_id).await.unwrap();

        let db = self.0.lock().await;

        let result = trx
            .write_set
            .get(key)
            .map(|x| x.clone())
            .unwrap_or_else(|| db.values.get(key).cloned());

        drop(trx);
        result
    }

    pub async fn put(&self, trx_id: usize, key: &[u8], value: &[u8]) {
        let mut db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let mut trx = trx.try_lock().unwrap();
        let lock = db.locks.entry(key.to_vec()).or_insert(Lock::new()).clone();
        trx.locks.insert(key.to_vec(), lock.clone());

        drop(db);
        lock.lock_write(trx_id).await.unwrap();

        trx.write_set.insert(key.to_vec(), Some(value.to_vec()));
        drop(trx);
    }

    pub async fn delete(&self, trx_id: usize, key: &[u8]) {
        let mut db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let mut trx = trx.try_lock().unwrap();
        let lock = db.locks.entry(key.to_vec()).or_insert(Lock::new()).clone();
        trx.locks.insert(key.to_vec(), lock.clone());

        drop(db);
        lock.lock_write(trx_id).await.unwrap();

        trx.write_set.insert(key.to_vec(), None);
        drop(trx);
    }

    pub async fn commit(&self, trx_id: usize) -> anyhow::Result<()> {
        let mut db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let mut trx = trx.try_lock().unwrap();

        let write_set = &trx.write_set;
        apply_write_set(&mut db.values, write_set);
        let write_set_bytes =
            bincode::serialize(write_set).context("failed to serialize write set")?;
        let mut logs_file_writer = BufWriter::new(&db.logs_file);
        atomic_append::append(&mut logs_file_writer, write_set_bytes.as_slice())?;
        drop(logs_file_writer);
        db.logs_file.sync_all().context("failed to sync log file")?;
        db.logs_len += 1;
        trx.write_set = BTreeMap::new();
        for (_, lock) in trx.locks.iter() {
            lock.unlock(trx_id).await.unwrap();
        }
        drop(trx);
        Ok(())
    }

    pub async fn abort(&self, trx_id: usize) {
        let db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let mut trx = trx.try_lock().unwrap();

        trx.write_set = BTreeMap::new();
        for (_, lock) in trx.locks.iter() {
            lock.unlock(trx_id).await.unwrap();
        }
        drop(trx);
    }

    pub async fn snapshot(&self) -> anyhow::Result<()> {
        let mut db = self.0.lock().await;

        let data_tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db.data_dir.join("data.tmp"))
            .context("failed to open data.tmp file")?;

        let mut data_tmp_file_writer = BufWriter::new(&data_tmp_file);
        bincode::serialize_into(&mut data_tmp_file_writer, &db.values)
            .context("failed to serialize data")?;
        data_tmp_file_writer
            .flush()
            .context("failed to flush data")?;
        data_tmp_file.sync_all().context("failed to sync data")?;
        std::fs::rename(&db.data_dir.join("data.tmp"), &db.data_dir.join("data"))
            .context("failed to rename data.tmp to data")?;
        db.logs_file
            .set_len(0)
            .context("failed to truncate logs file")?;
        db.logs_len = 0;
        Ok(())
    }

    pub async fn new_trx(&self) -> usize {
        let mut db = self.0.lock().await;

        let trx_id = db.trx_count;
        db.trx_count += 1;
        db.trxs.insert(
            trx_id,
            Arc::new(Mutex::new(Trx {
                write_set: BTreeMap::new(),
                locks: HashMap::new(),
            })),
        );
        trx_id
    }

    pub async fn delete_trx(&self, trx_id: usize) {
        let mut db = self.0.lock().await;
        let trx = db.trxs.get(&trx_id).unwrap().clone();
        let trx = trx.try_lock().unwrap();

        db.trxs.remove(&trx_id);
        drop(trx);
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
