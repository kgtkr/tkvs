use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

enum LockWaiter {
    Read(HashMap<usize, oneshot::Sender<()>>),
    Write(usize, oneshot::Sender<()>),
}

impl LockWaiter {
    fn unlock(self) -> anyhow::Result<CurrentLock> {
        match self {
            LockWaiter::Read(map) => {
                let mut ids = HashSet::new();
                for (id, sender) in map.into_iter() {
                    sender.send(()).map_err(|_| anyhow::anyhow!("send error"))?;
                    ids.insert(id);
                }
                Ok(CurrentLock::Read(ids))
            }
            LockWaiter::Write(id, sender) => {
                sender.send(()).map_err(|_| anyhow::anyhow!("send error"))?;
                Ok(CurrentLock::Write(id))
            }
        }
    }
}

enum CurrentLock {
    Read(HashSet<usize>),
    Write(usize),
}

enum LockState {
    Unlocked,
    Locked(CurrentLock, VecDeque<LockWaiter>),
}

enum LockModify {
    InsertHead(usize),
    InsertLast(usize),
    Noop,
}

#[derive(Clone)]
pub struct Lock(Arc<Mutex<LockState>>);

impl Lock {
    pub fn new() -> Self {
        Lock(Arc::new(Mutex::new(LockState::Unlocked)))
    }

    pub async fn lock_read(&self, id: usize) -> anyhow::Result<()> {
        let mut lock = self.0.lock().await;
        match &mut *lock {
            LockState::Unlocked => {
                *lock = LockState::Locked(
                    CurrentLock::Read({
                        let mut set = HashSet::new();
                        set.insert(id);
                        set
                    }),
                    VecDeque::new(),
                );
            }
            LockState::Locked(current_lock, waiters) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) || waiters.len() == 0 => {
                    // Read lockなら無条件でこれを行ってもいいが, writer lockが長時間待たされることを防ぐため, waitersがいない場合のみおこなう
                    cur_ids.insert(id);
                    // TODO: 所有権の関係で上で変更するのがめんどくさすぎるのでとりあえずpanic
                    Self::check_deadlock(&*lock, LockModify::Noop).unwrap();
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {}
                _ => {
                    let (tx, rx) = oneshot::channel();
                    if let Some(LockWaiter::Read(read_waiters)) = waiters.back_mut() {
                        read_waiters.insert(id, tx);
                        Self::check_deadlock(&*lock, LockModify::Noop).unwrap();
                    } else {
                        waiters.push_back(LockWaiter::Read({
                            let mut map = HashMap::new();
                            map.insert(id, tx);
                            map
                        }));
                        Self::check_deadlock(&*lock, LockModify::Noop).unwrap();
                    }
                    drop(lock);
                    rx.await?;
                }
            },
        }
        Ok(())
    }

    pub async fn lock_write(&self, id: usize) -> anyhow::Result<()> {
        let mut lock = self.0.lock().await;

        match &mut *lock {
            LockState::Unlocked => {
                *lock = LockState::Locked(CurrentLock::Write(id), VecDeque::new());
            }
            LockState::Locked(current_lock, waiters) => match current_lock {
                CurrentLock::Read(cur_ids)
                    if cur_ids
                        == &({
                            let mut set = HashSet::new();
                            set.insert(id);
                            set
                        }) =>
                {
                    *current_lock = CurrentLock::Write(id);
                    Self::check_deadlock(&*lock, LockModify::Noop).unwrap();
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {}
                _ => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push_back(LockWaiter::Write(id, tx));
                    Self::check_deadlock(&*lock, LockModify::Noop).unwrap();
                    drop(lock);
                    rx.await?;
                }
            },
        }
        Ok(())
    }

    pub async fn unlock(&self, id: usize) -> anyhow::Result<()> {
        let mut lock = self.0.lock().await;

        match &mut *lock {
            LockState::Unlocked => anyhow::bail!("unlock called on unlocked lock"),
            LockState::Locked(current_lock, waiters) => match current_lock {
                CurrentLock::Read(cur_ids) if cur_ids.contains(&id) => {
                    cur_ids.remove(&id);
                    if cur_ids.is_empty() {
                        match waiters.pop_front() {
                            Some(head) => {
                                *current_lock = head.unlock()?;
                            }
                            None => {
                                *lock = LockState::Unlocked;
                            }
                        }
                    }
                }
                CurrentLock::Write(cur_id) if cur_id == &id => match waiters.pop_front() {
                    Some(head) => {
                        *current_lock = head.unlock()?;
                    }
                    None => {
                        *lock = LockState::Unlocked;
                    }
                },
                _ => anyhow::bail!("unlock called on locked lock"),
            },
        }
        Ok(())
    }

    fn check_deadlock(lock_state: &LockState, modify: LockModify) -> anyhow::Result<()> {
        match lock_state {
            LockState::Unlocked => {}
            LockState::Locked(current_lock, waiters) => {
                let mut ids_list = Vec::new();
                {
                    let mut ids = HashSet::new();
                    match current_lock {
                        CurrentLock::Read(cur_ids) => {
                            ids.extend(cur_ids);
                        }
                        CurrentLock::Write(cur_id) => {
                            ids.insert(*cur_id);
                        }
                    }
                    ids_list.push(ids);
                }
                for waiter in waiters {
                    let mut ids = HashSet::new();
                    match waiter {
                        LockWaiter::Read(read_waiters) => {
                            ids.extend(read_waiters.keys());
                        }
                        LockWaiter::Write(cur_id, _) => {
                            ids.insert(*cur_id);
                        }
                    }
                    ids_list.push(ids);
                }
                match modify {
                    LockModify::InsertHead(id) => {
                        ids_list.first_mut().unwrap().insert(id);
                    }
                    LockModify::InsertLast(id) => {
                        ids_list.last_mut().unwrap().insert(id);
                    }
                    LockModify::Noop => {}
                }

                let all_ids: HashSet<usize> =
                    ids_list.iter().fold(HashSet::new(), |mut acc, ids| {
                        acc.extend(ids);
                        acc
                    });

                let mut graph = HashMap::new();
                for id in &all_ids {
                    graph.insert(id, HashSet::new());
                }
                for (ids1, ids2) in ids_list.iter().zip(ids_list.iter().skip(1)) {
                    for id1 in ids1 {
                        for id2 in ids2 {
                            graph.get_mut(id2).unwrap().insert(*id1);
                        }
                    }
                }

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
                    anyhow::bail!("deadlock detected");
                }
            }
        }
        Ok(())
    }
}
