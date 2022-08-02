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
    writers: VecDeque<(TrxId, oneshot::Sender<()>)>,
    readers: HashMap<TrxId, oneshot::Sender<()>>,
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

    fn lock_read(&mut self, id: TrxId) -> Option<Receiver<()>> {
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
                None
            }
            MaybeLock::Locked(Lock {
                current_lock,
                writers,
                readers,
            }) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) || writers.is_empty() => {
                    cur_ids.insert(id);
                    None
                }
                CurrentLock::Write(cur_id) if cur_id == &id => None,
                _ => {
                    let (tx, rx) = oneshot::channel();
                    readers.insert(id, tx);
                    Some(rx)
                }
            },
        }
    }

    fn lock_write(&mut self, id: usize) -> Option<Receiver<()>> {
        match self {
            MaybeLock::Unlocked => {
                *self = MaybeLock::Locked(Lock {
                    current_lock: CurrentLock::Write(id),
                    writers: VecDeque::new(),
                    readers: HashMap::new(),
                });
                None
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
                    None
                }
                CurrentLock::Write(cur_id) if cur_id == &id => None,
                _ => {
                    let (tx, rx) = oneshot::channel();
                    writers.push_back((id, tx));
                    Some(rx)
                }
            },
        }
    }

    fn unlock(&mut self, id: usize) -> Option<()> {
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
                        self.current_lock_unlock();
                    } else if cur_ids.len() == 1 {
                        let cur_id = *cur_ids.iter().next().unwrap();
                        if let Some(waiter_idx) = writers.iter().position(|(id, _)| *id == cur_id) {
                            let (waiter_id, waiter_tx) = writers.remove(waiter_idx).unwrap();
                            *current_lock = CurrentLock::Write(waiter_id);
                            waiter_tx.send(()).unwrap();
                        }
                    }

                    Some(())
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {
                    self.current_lock_unlock();
                    Some(())
                }
                _ => None,
            },
        }
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
                } else if !readers.is_empty() {
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

    fn current_lock_ids(&self) -> HashSet<TrxId> {
        match self {
            MaybeLock::Unlocked => HashSet::new(),
            MaybeLock::Locked(Lock { current_lock, .. }) => match current_lock {
                CurrentLock::Read(cur_ids) => cur_ids.clone(),
                CurrentLock::Write(cur_id) => {
                    let mut set = HashSet::new();
                    set.insert(*cur_id);
                    set
                }
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
            }) => {
                let mut set = HashSet::new();
                for (id, _) in writers {
                    set.insert(*id);
                }
                for id in readers.keys() {
                    set.insert(*id);
                }
                set
            }
        }
    }
}

#[derive(Debug)]
struct LockSetState {
    locks: HashMap<RecordKey, MaybeLock>,
}

#[derive(Clone, Debug)]
pub struct LockSet(Arc<Mutex<LockSetState>>);

impl LockSet {
    pub fn new() -> Self {
        LockSet(Arc::new(Mutex::new(LockSetState {
            locks: HashMap::new(),
        })))
    }

    pub async fn lock_read(&self, key: RecordKey, id: TrxId) {
        let rx = {
            let mut lock_set = self.0.lock().unwrap();
            let lock = lock_set.locks.entry(key).or_insert_with(MaybeLock::new);
            let rx = lock.lock_read(id);
            drop(lock_set);
            self.check_deadlock();
            rx
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
            let rx = lock.lock_write(id);
            drop(lock_set);
            self.check_deadlock();
            rx
        };
        match rx {
            Some(f) => f.await.unwrap(),
            None => (),
        }
    }

    pub fn unlock(&self, id: TrxId) {
        let mut lock_set = self.0.lock().unwrap();
        for lock in lock_set.locks.values_mut() {
            if lock.current_lock_ids().contains(&id) {
                lock.unlock(id).unwrap()
            }
        }
        drop(lock_set);
    }

    fn check_deadlock(&self) {
        let mut lock_set = self.0.lock().unwrap();
        lock_set
            .locks
            .retain(|_, lock| matches!(lock, MaybeLock::Locked(_)));

        let all_ids = lock_set
            .locks
            .iter()
            .fold(HashSet::new(), |mut ids, (_, lock)| {
                ids.extend(lock.current_lock_ids());
                ids.extend(lock.wait_ids());
                ids
            });

        let mut graph = HashMap::new();
        for id in &all_ids {
            graph.insert(id, HashSet::new());
        }
        for lock in lock_set.locks.values() {
            let tos = lock.current_lock_ids();
            let froms = lock.wait_ids();
            for &to in &tos {
                for &from in &froms {
                    if from != to {
                        graph.get_mut(&from).unwrap().insert(to);
                    }
                }
            }
        }
        drop(lock_set);

        let mut sorted_ids = Vec::new();
        let mut indegree = HashMap::new();
        for &id in &all_ids {
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

        if sorted_ids.len() != all_ids.len() {
            panic!("deadlock detected"); // TODO: handle deadlock
        }
    }
}
