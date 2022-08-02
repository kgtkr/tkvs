use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot::Receiver;

use tokio::sync::oneshot;

type TrxId = usize;
type RecordKey = Vec<u8>;

#[derive(Debug)]
enum CurrentLock {
    Read(HashSet<TrxId>),
    Write(TrxId),
}

#[derive(Debug)]
struct Lock {
    current_lock: CurrentLock,
    // トランザクションは直列に実行されるため, TrxIdは重複しない
    writers: VecDeque<TrxId>,
    readers: HashSet<TrxId>,
}

#[derive(Debug)]
enum MaybeLock {
    Locked(Lock),
    Unlocked,
}

impl MaybeLock {
    fn new() -> Self {
        MaybeLock::Unlocked
    }

    fn lock_read(&mut self, id: TrxId) -> bool {
        match self {
            MaybeLock::Unlocked => {
                *self = MaybeLock::Locked(Lock {
                    current_lock: CurrentLock::Read({
                        let mut set = HashSet::new();
                        set.insert(id);
                        set
                    }),
                    writers: VecDeque::new(),
                    readers: HashSet::new(),
                });
                false
            }
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers,
            }) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) || writers.is_empty() => {
                    cur_ids.insert(id);
                    false
                }
                CurrentLock::Write(cur_id) if cur_id == &id => false,
                _ => {
                    readers.insert(id);
                    true
                }
            },
        }
    }

    fn lock_write(&mut self, id: usize) -> bool {
        match self {
            MaybeLock::Unlocked => {
                *self = MaybeLock::Locked(Lock {
                    current_lock: CurrentLock::Write(id),
                    writers: VecDeque::new(),
                    readers: HashSet::new(),
                });
                false
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
                    false
                }
                CurrentLock::Write(cur_id) if cur_id == &id => false,
                _ => {
                    writers.push_back(id);
                    true
                }
            },
        }
    }

    fn unlock(&mut self, id: usize) -> Option<HashSet<TrxId>> {
        match self {
            MaybeLock::Unlocked => None,
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                ..
            }) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) => {
                    cur_ids.remove(&id);
                    if cur_ids.is_empty() {
                        Some(self.current_lock_unlock())
                    } else if cur_ids.len() == 1 {
                        let cur_id = *cur_ids.iter().next().unwrap();
                        if let Some(waiter_idx) = writers.iter().position(|id| *id == cur_id) {
                            let waiter_id = writers.remove(waiter_idx).unwrap();
                            *current_lock = CurrentLock::Write(waiter_id);
                            Some(HashSet::from([waiter_id]))
                        } else {
                            Some(HashSet::new())
                        }
                    } else {
                        Some(HashSet::new())
                    }
                }
                CurrentLock::Write(cur_id) if cur_id == &id => Some(self.current_lock_unlock()),
                _ => None,
            },
        }
    }

    fn current_lock_unlock(&mut self) -> HashSet<TrxId> {
        match self {
            MaybeLock::Unlocked => unreachable!(),
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers,
            }) => {
                if let Some(id) = writers.pop_front() {
                    *current_lock = CurrentLock::Write(id);
                    HashSet::from([id])
                } else if !readers.is_empty() {
                    *current_lock = CurrentLock::Read(readers.clone());
                    readers.drain().collect::<HashSet<_>>()
                } else {
                    *self = MaybeLock::Unlocked;
                    HashSet::new()
                }
            }
        }
    }

    fn current_lock_ids(&self) -> HashSet<TrxId> {
        match self {
            MaybeLock::Unlocked => HashSet::new(),
            MaybeLock::Locked(Lock { current_lock, .. }) => match current_lock {
                CurrentLock::Read(cur_ids) => cur_ids.clone(),
                CurrentLock::Write(cur_id) => HashSet::from([*cur_id]),
            },
        }
    }

    fn wait_ids(&self) -> HashSet<TrxId> {
        match self {
            MaybeLock::Unlocked => HashSet::new(),
            MaybeLock::Locked(Lock {
                current_lock: _,
                writers,
                readers,
            }) => writers
                .iter()
                .cloned()
                .chain(readers.iter().cloned())
                .collect::<HashSet<_>>(),
        }
    }
}

#[derive(Debug)]
struct LockSetState {
    locks: HashMap<RecordKey, MaybeLock>,
    txs: HashMap<TrxId, oneshot::Sender<()>>,
}

#[derive(Clone, Debug)]
pub struct LockSet(Arc<Mutex<LockSetState>>);

impl LockSet {
    pub fn new() -> Self {
        LockSet(Arc::new(Mutex::new(LockSetState {
            locks: HashMap::new(),
            txs: HashMap::new(),
        })))
    }

    pub async fn lock_read(&self, key: RecordKey, id: TrxId) {
        let rx = {
            let mut lock_set = self.0.lock().unwrap();
            let lock = lock_set.locks.entry(key).or_insert_with(MaybeLock::new);
            if lock.lock_read(id) {
                let (tx, rx) = oneshot::channel();
                lock_set.txs.insert(id, tx);
                drop(lock_set);
                self.check_deadlock();
                Some(rx)
            } else {
                None
            }
        };
        match rx {
            Some(f) => f.await.unwrap(),
            None => (),
        }
    }

    pub async fn lock_write(&self, key: RecordKey, id: TrxId) {
        let rx = {
            let mut lock_set = self.0.lock().unwrap();
            let lock = lock_set.locks.entry(key).or_insert_with(MaybeLock::new);
            if lock.lock_write(id) {
                let (tx, rx) = oneshot::channel();
                lock_set.txs.insert(id, tx);
                drop(lock_set);
                self.check_deadlock();
                Some(rx)
            } else {
                None
            }
        };
        match rx {
            Some(f) => f.await.unwrap(),
            None => (),
        }
    }

    pub fn unlock(&self, id: TrxId) {
        let mut lock_set = self.0.lock().unwrap();
        let lock_set = &mut *lock_set;
        for lock in lock_set.locks.values_mut() {
            if lock.current_lock_ids().contains(&id) {
                let ids = lock.unlock(id).unwrap();
                for id in ids {
                    lock_set.txs.remove(&id).unwrap().send(()).unwrap();
                }
            }
        }
    }

    fn check_deadlock(&self) {
        let mut lock_set = self.0.lock().unwrap();
        lock_set
            .locks
            .retain(|_, lock| matches!(lock, MaybeLock::Locked(_)));

        let mut graph = HashMap::new();
        for lock in lock_set.locks.values() {
            let tos = lock.current_lock_ids();
            let froms = lock.wait_ids();
            for &to in &tos {
                graph.entry(to).or_insert_with(HashSet::new);
                for &from in &froms {
                    if from != to {
                        graph.entry(from).or_insert_with(HashSet::new).insert(to);
                    }
                }
            }
        }
        drop(lock_set);

        let mut sorted_ids = Vec::new();
        let mut indegree = HashMap::new();
        for &id in graph.keys() {
            indegree.insert(id, 0);
        }
        for (_, tos) in graph.iter() {
            for to in tos {
                *indegree.get_mut(to).unwrap() += 1;
            }
        }
        let mut queue = VecDeque::new();
        for (id, indegree) in indegree.iter() {
            if *indegree == 0 {
                queue.push_back(*id);
            }
        }
        while let Some(id) = queue.pop_front() {
            sorted_ids.push(id);
            for &to in graph.get(&id).unwrap() {
                *indegree.get_mut(&to).unwrap() -= 1;
                if *indegree.get(&to).unwrap() == 0 {
                    queue.push_back(to);
                }
            }
        }

        if sorted_ids.len() != graph.len() {
            panic!("deadlock detected"); // TODO: handle deadlock
        }
    }
}
