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
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {}
                _ => {
                    let (tx, rx) = oneshot::channel();
                    if let Some(LockWaiter::Read(read_waiters)) = waiters.back_mut() {
                        read_waiters.insert(id, tx);
                    } else {
                        waiters.push_back(LockWaiter::Read({
                            let mut map = HashMap::new();
                            map.insert(id, tx);
                            map
                        }));
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
                }
                CurrentLock::Write(cur_id) if cur_id == &id => {}
                _ => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push_back(LockWaiter::Write(id, tx));
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
}
