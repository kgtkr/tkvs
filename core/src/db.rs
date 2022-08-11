use crate::range_element::BytesRange;

use super::atomic_append;
use super::lock_set::LockSet;
use anyhow::Context;
use bytes::Bytes;
use nix::sys::stat::Mode;
use std::collections::BTreeMap;

use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;

use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::ops;
use std::os::unix::prelude::RawFd;
use std::path::PathBuf;

use nix::fcntl::{flock, open, FlockArg, OFlag};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type Key = Bytes;
type Value = Bytes;
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
    lock_file: RawFd,
}

#[derive(Clone, Debug)]
pub struct DB(mpsc::Sender<DBMessage>);

#[derive(Debug)]
pub struct Trx {
    db: DB,
    lock_set: LockSet,
    id: usize,
    write_set: DataBaseWriteSet,
    // valueはロックしているので変わることはないが, 範囲ロックは行わないのでコミット時にキーセットに変化がないかチェックを行う
    read_set: Vec<(BytesRange, BTreeSet<Key>)>,
}

impl Drop for Trx {
    fn drop(&mut self) {
        self.lock_set.unlock(self.id);
    }
}

impl Trx {
    pub async fn get(&mut self, key: &Key) -> anyhow::Result<Option<Value>> {
        Ok(self
            .range_inner(
                key.clone()..=key.clone(),
                BTreeSet::from_iter(vec![key.clone()]),
            )
            .await?
            .into_values()
            .next())
    }

    pub async fn range<R: ops::RangeBounds<Key>>(
        &mut self,
        range: R,
    ) -> anyhow::Result<BTreeMap<Key, Value>> {
        self.range_inner(range, BTreeSet::new()).await
    }

    async fn range_inner<R: ops::RangeBounds<Key>>(
        &mut self,
        range: R,
        force_lock_keys: BTreeSet<Key>,
    ) -> anyhow::Result<BTreeMap<Key, Value>> {
        let (tx, rx) = oneshot::channel();

        let range = BytesRange::from_bounds(range);

        self.db
            .0
            .send(DBMessage::GetRangeWithLock {
                range: range.clone(),
                trx_id: self.id,
                resp: tx,
                force_lock_keys,
            })
            .await
            .unwrap();

        let mut values = rx.await.unwrap()?;
        self.read_set
            .push((range.clone(), values.keys().cloned().collect()));

        let write_set_values = self
            .write_set
            .range(range.clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        for (key, value) in write_set_values {
            if let Some(value) = value {
                values.insert(key.clone(), value.clone());
            } else {
                values.remove(&key.clone());
            }
        }

        Ok(values)
    }

    pub async fn put(&mut self, key: Key, value: Value) -> anyhow::Result<()> {
        self.lock_set.lock_write(key.clone(), self.id).await?;
        self.write_set.insert(key, Some(value));
        Ok(())
    }

    pub async fn delete(&mut self, key: Key) -> anyhow::Result<()> {
        self.lock_set.lock_write(key.clone(), self.id).await?;
        self.write_set.insert(key, None);
        Ok(())
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
                read_set: self.read_set.clone(),
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
        read_set: Vec<(BytesRange, BTreeSet<Key>)>,
        resp: oneshot::Sender<anyhow::Result<()>>,
    },
    GetRangeWithLock {
        range: BytesRange,
        trx_id: usize,
        force_lock_keys: BTreeSet<Key>,
        resp: oneshot::Sender<anyhow::Result<BTreeMap<Key, Value>>>,
    },
}

impl DB {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        let lock_file = open(
            &data_dir.join(".lock"),
            OFlag::O_RDWR | OFlag::O_CREAT,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .context("failed to open lock file")?;
        flock(lock_file, FlockArg::LockExclusiveNonblock).context("failed to lock")?;

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
        let (tx_for_trx, mut rx_for_trx) = mpsc::channel(32);
        let db = DB(tx);
        let db_for_trx = DB(tx_for_trx);
        tokio::spawn(async move {
            let mut state = DBInner {
                data_dir,
                values,
                logs_len,
                logs_file,
                trx_count: 0,
                lock_set: LockSet::new(),
                lock_file,
            };
            // avoid leak. see: https://github.com/tokio-rs/tokio/issues/4023
            while let Some(msg) = tokio::select! {
                Some(msg) = rx_for_trx.recv() => {
                    Some(msg)
                },
                maybe_msg = rx.recv() => {
                    maybe_msg
                },
            } {
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
                        let err = res.as_ref().err().map(|e| format!("{}", e));
                        resp.send(res).unwrap();
                        if let Some(err) = err {
                            tracing::error!("snapshot error, and shutdown: {}", err);
                            return;
                        }
                    }
                    DBMessage::NewTrx { resp } => {
                        let trx_id = state.trx_count;
                        state.trx_count += 1;
                        let res = Trx {
                            write_set: BTreeMap::new(),
                            read_set: Vec::new(),
                            id: trx_id,
                            db: db_for_trx.clone(),
                            lock_set: state.lock_set.clone(),
                        };
                        resp.send(res).unwrap();
                    }
                    DBMessage::Commit {
                        write_set,
                        read_set,
                        resp,
                    } => {
                        // TODO: read setチェック
                        let res = (|| {
                            for (range, expect) in read_set {
                                let actual = state
                                    .values
                                    .range(range)
                                    .map(|(key, _)| key.clone())
                                    .collect::<BTreeSet<_>>();
                                if expect != actual {
                                    return Err(anyhow::anyhow!("serializable error"));
                                }
                            }

                            apply_write_set(&mut state.values, &write_set);

                            let write_set_bytes = bincode::serialize(&write_set).unwrap();
                            let mut logs_file_writer = BufWriter::new(&state.logs_file);
                            atomic_append::append(
                                &mut logs_file_writer,
                                write_set_bytes.as_slice(),
                            )
                            .unwrap();
                            drop(logs_file_writer);
                            state
                                .logs_file
                                .sync_all()
                                .context("failed to sync log file")
                                .unwrap();
                            state.logs_len += 1;
                            Ok(())
                        })();
                        resp.send(res).unwrap();
                    }
                    DBMessage::GetRangeWithLock {
                        range,
                        trx_id,
                        force_lock_keys,
                        resp,
                    } => {
                        let res = (|| async {
                            let values = state
                                .values
                                .range(range)
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect::<BTreeMap<_, _>>();
                            let mut lock_keys = values.keys().cloned().collect::<BTreeSet<_>>();
                            lock_keys.extend(force_lock_keys);
                            state.lock_set.lock_read(lock_keys, trx_id).await?;
                            Ok(values)
                        })()
                        .await;
                        resp.send(res).unwrap();
                    }
                }
            }
        });

        Ok(db)
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

impl Drop for DBInner {
    fn drop(&mut self) {
        // unlock lockfile
        flock(self.lock_file, FlockArg::Unlock).unwrap();
    }
}
