use super::atomic_append;
use super::lock_set::LockSet;
use anyhow::Context;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
use tokio::sync::mpsc;
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
    lock_set: LockSet,
}

#[derive(Clone, Debug)]
pub struct DB(mpsc::Sender<DBMessage>);

#[derive(Debug)]
pub struct Trx {
    db: DB,
    lock_set: LockSet,
    id: usize,
    write_set: DataBaseWriteSet,
    read_set: BTreeMap<Key, Option<Value>>,
}

impl Drop for Trx {
    fn drop(&mut self) {
        self.lock_set.unlock(self.id);
    }
}

impl Trx {
    pub async fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.lock_set.lock_read(key.to_vec(), self.id).await;

        if let Some(value) = self
            .write_set
            .get(key)
            .cloned()
            .or_else(|| self.read_set.get(key).cloned())
        {
            value
        } else {
            // ロックしている間は値が変わることはないので複数回のアクセスを防ぐために値をキャッシュする
            let (tx, rx) = oneshot::channel();
            self.db
                .0
                .send(DBMessage::Get {
                    key: key.to_vec(),
                    resp: tx,
                })
                .await
                .unwrap();
            let value = rx.await.unwrap();
            self.read_set.insert(key.to_vec(), value.clone());
            value
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) {
        self.lock_set.lock_write(key.to_vec(), self.id).await;
        self.write_set.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub async fn delete(&mut self, key: &[u8]) {
        self.lock_set.lock_write(key.to_vec(), self.id).await;
        self.write_set.insert(key.to_vec(), None);
    }

    pub async fn abort(&mut self) {
        self.write_set.clear();
        self.read_set.clear();
        self.lock_set.unlock(self.id);
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .0
            .send(DBMessage::Commit {
                write_set: self.write_set.clone(),
                resp: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()?;

        self.write_set = BTreeMap::new();
        self.read_set.clear();
        self.lock_set.unlock(self.id);
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
enum DBMessage {
    Snapshot {
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
    NewTrx {
        resp: oneshot::Sender<Trx>,
    },
    Commit {
        write_set: DataBaseWriteSet,
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
    Get {
        key: Key,
        resp: oneshot::Sender<Option<Value>>,
    },
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

        let (tx, mut rx) = mpsc::channel(32);
        let db_ = DB(tx);
        let db = db_.clone();
        tokio::spawn(async move {
            let mut state = DBInner {
                data_dir,
                values,
                logs_len,
                logs_file,
                trx_count: 0,
                lock_set: LockSet::new(),
            };
            while let Some(msg) = rx.recv().await {
                match msg {
                    DBMessage::Snapshot { resp } => {
                        let res = (|| {
                            let data_tmp_file = OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&state.data_dir.join("data.tmp"))
                                .context("failed to open data.tmp file")?;

                            let mut data_tmp_file_writer = BufWriter::new(&data_tmp_file);
                            bincode::serialize_into(&mut data_tmp_file_writer, &state.values)
                                .context("failed to serialize data")?;
                            data_tmp_file_writer
                                .flush()
                                .context("failed to flush data")?;
                            data_tmp_file.sync_all().context("failed to sync data")?;
                            std::fs::rename(
                                &state.data_dir.join("data.tmp"),
                                &state.data_dir.join("data"),
                            )
                            .context("failed to rename data.tmp to data")?;
                            state
                                .logs_file
                                .set_len(0)
                                .context("failed to truncate logs file")?;
                            state.logs_len = 0;
                            Ok(())
                        })();
                        resp.send(res).unwrap();
                    }
                    DBMessage::NewTrx { resp } => {
                        let trx_id = state.trx_count;
                        state.trx_count += 1;
                        let res = Trx {
                            write_set: BTreeMap::new(),
                            read_set: BTreeMap::new(),
                            id: trx_id,
                            db: db.clone(),
                            lock_set: state.lock_set.clone(),
                        };
                        resp.send(res).unwrap();
                    }
                    DBMessage::Commit { write_set, resp } => {
                        let res = (|| {
                            apply_write_set(&mut state.values, &write_set);
                            let write_set_bytes = bincode::serialize(&write_set)
                                .context("failed to serialize write set")?;
                            let mut logs_file_writer = BufWriter::new(&state.logs_file);
                            atomic_append::append(
                                &mut logs_file_writer,
                                write_set_bytes.as_slice(),
                            )?;
                            drop(logs_file_writer);
                            state
                                .logs_file
                                .sync_all()
                                .context("failed to sync log file")?;
                            state.logs_len += 1;
                            Ok(())
                        })();
                        resp.send(res).unwrap();
                    }
                    DBMessage::Get { key, resp } => {
                        let res = state.values.get(&key).cloned();
                        resp.send(res).unwrap();
                    }
                }
            }
        });

        Ok(db_)
    }

    pub async fn snapshot(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(DBMessage::Snapshot { resp: tx }).await?;
        rx.await.unwrap()
    }

    pub async fn new_trx(&self) -> Trx {
        let (tx, rx) = oneshot::channel();
        self.0.send(DBMessage::NewTrx { resp: tx }).await.unwrap();
        rx.await.unwrap()
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
