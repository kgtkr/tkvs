use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::future::Future;
use tokio::sync::oneshot;

type TrxId = usize;
type RecordKey = Vec<u8>;

enum CurrentLock {
    Read(HashSet<TrxId>),
    Write(TrxId),
}

struct Lock {
    current_lock: CurrentLock,
    writers: VecDeque<(TrxId, oneshot::Sender<()>)>,
    readers: HashMap<TrxId, oneshot::Sender<()>>,
}

enum MaybeLock {
    Locked(Lock),
    Unlocked,
}

impl MaybeLock {
    fn new() -> Self {
        MaybeLock::Unlocked
    }

    async fn lock_read(&mut self, id: TrxId) {
        match self {
            MaybeLock::Unlocked => {
                *self = MaybeLock::Locked(Lock {
                    current_lock: CurrentLock::Read({
                        let mut set = HashSet::new();
                        set.insert(id);
                        set
                    }),
                    writers: VecDeque::new(),
                    readers: HashMap::new(),
                });
            }
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers,
            }) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) || writers.len() == 0 => {
                    cur_ids.insert(id);
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {}
                _ => {
                    let (tx, rx) = oneshot::channel();
                    readers.insert(id, tx);
                    rx.await.unwrap();
                }
            },
        }
    }

    async fn lock_write(&mut self, id: usize) {
        match self {
            MaybeLock::Unlocked => {
                *self = MaybeLock::Locked(Lock {
                    current_lock: CurrentLock::Write(id),
                    writers: VecDeque::new(),
                    readers: HashMap::new(),
                });
            }
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers: _,
            }) => match current_lock {
                CurrentLock::Read(cur_ids)
                    if cur_ids
                        == &({
                            let mut set = HashSet::new();
                            set.insert(id);
                            set
                        }) =>
                {
                    *current_lock = CurrentLock::Write(id);
                }
                CurrentLock::Write(cur_id) if cur_id == &id => None,
                _ => {
                    let (tx, rx) = oneshot::channel();
                    writers.push_back((id, tx));
                    rx.await.unwrap();
                }
            },
        }
    }

    fn unlock(&mut self, id: usize) -> anyhow::Result<()> {
        match self {
            MaybeLock::Unlocked => anyhow::bail!("unlock called on unlocked lock"),
            MaybeLock::Locked(Lock { current_lock, .. }) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) => {
                    cur_ids.remove(&id);
                    if cur_ids.is_empty() {
                        self.current_lock_unlock();
                    }
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {
                    self.current_lock_unlock();
                }
                _ => anyhow::bail!("unlock called on locked lock"),
            },
        }
        Ok(())
    }

    fn current_lock_unlock(&mut self) {
        match self {
            MaybeLock::Unlocked => unreachable!(),
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers,
            }) => {
                if let Some((id, tx)) = writers.pop_front() {
                    *current_lock = CurrentLock::Write(id);
                    tx.send(()).unwrap();
                } else if readers.len() > 0 {
                    *current_lock = CurrentLock::Read(readers.keys().cloned().collect());
                    let txs = readers.drain().map(|(_, tx)| tx).collect::<Vec<_>>();
                    for tx in txs {
                        tx.send(()).unwrap();
                    }
                } else {
                    *self = MaybeLock::Unlocked;
                }
            }
        }
    }
}
