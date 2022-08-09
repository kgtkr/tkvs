#![deny(warnings)]

use std::collections::HashMap;

use rand::Rng;
use std::env;
use std::process::abort;
use std::sync::{Arc, Mutex};
use tokio::process::Command;

#[tokio::main]
async fn main() {
    let mut vm = Command::new("make").arg("up-vm").spawn().unwrap();
    assert!(Command::new("make")
        .arg("wait-vm")
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    assert!(Command::new("make")
        .arg("reset-tkvs-server")
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    assert!(Command::new("make")
        .arg("run-tkvs-server")
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let is_kill = Arc::new(Mutex::new(false));
    let mut client = tkvs_protos::tkvs_client::TkvsClient::connect(format!(
        "http://localhost:{}",
        env::var("grpc_port").unwrap()
    ))
    .await
    .unwrap();
    let session_id = client
        .start_session(tkvs_protos::StartSessionRequest {})
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let mut handles = Vec::new();
    {
        let mut client = client.clone();
        let is_kill = is_kill.clone();
        handles.push(tokio::spawn(async move {
            loop {
                if *is_kill.lock().unwrap() {
                    return;
                }
                let result = client.snapshot(tkvs_protos::SnapshotRequest {}).await;
                if let Err(e) = result {
                    if *is_kill.lock().unwrap() {
                        return;
                    } else {
                        eprintln!("{}", e);
                        abort();
                    }
                }
                let dur =
                    { std::time::Duration::from_millis(rand::thread_rng().gen_range(0..1000)) };
                tokio::time::sleep(dur).await;
            }
        }));
    }

    {
        let mut client = client.clone();
        let is_kill = is_kill.clone();
        let session_id = session_id.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                if *is_kill.lock().unwrap() {
                    return;
                }
                // session busyで失敗する可能性がある
                let _ = client
                    .keep_alive_session(tkvs_protos::KeepAliveSessionRequest {
                        session_id: session_id.clone(),
                    })
                    .await;
            }
        }));
    }

    let committed_handle = {
        let mut client = client.clone();
        let is_kill = is_kill.clone();
        let session_id = session_id.clone();
        tokio::spawn(async move {
            let mut committed_state = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
            loop {
                if *is_kill.lock().unwrap() {
                    return vec![committed_state];
                }

                let mut uncommitted_state = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
                let count = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(1..10)
                };
                for _ in 0..count {
                    let action = {
                        let mut rng = rand::thread_rng();
                        rng.gen_range(0..2)
                    };
                    let all_keys = committed_state
                        .keys()
                        .chain(uncommitted_state.keys())
                        .collect::<Vec<_>>();
                    let exist_key = if all_keys.is_empty() {
                        None
                    } else {
                        Some(all_keys[rand::thread_rng().gen_range(0..all_keys.len())].clone())
                    };

                    let non_exist_key = {
                        let mut rng = rand::thread_rng();
                        let len = rng.gen_range(1..100);
                        (0..len)
                            .map(|_| rng.gen_range(0..255u8))
                            .collect::<Vec<_>>()
                    };
                    let key = {
                        let mut rng = rand::thread_rng();
                        match exist_key {
                            None => non_exist_key,
                            Some(exist_key) => {
                                if rng.gen_range(0..2) == 0 {
                                    exist_key
                                } else {
                                    non_exist_key
                                }
                            }
                        }
                    };
                    let op_result = match action {
                        0 => {
                            let value = {
                                let mut rng = rand::thread_rng();
                                (0..rng.gen_range(1..100))
                                    .map(|_| rng.gen_range(0..255u8))
                                    .collect::<Vec<_>>()
                            };
                            uncommitted_state.insert(key.clone(), Some(value.clone()));
                            println!("set {:?} {:?}", key, value);
                            client
                                .put(tkvs_protos::PutRequest {
                                    session_id: session_id.clone(),
                                    key: key.clone(),
                                    value: value.clone(),
                                })
                                .await
                                .map(|_| ())
                        }
                        _ => {
                            uncommitted_state.insert(key.clone(), None);
                            println!("delete {:?}", key);
                            client
                                .delete(tkvs_protos::DeleteRequest {
                                    session_id: session_id.clone(),
                                    key: key.clone(),
                                })
                                .await
                                .map(|_| ())
                        }
                    };

                    if let Err(e) = op_result {
                        if *is_kill.lock().unwrap() {
                            return vec![committed_state];
                        } else {
                            eprintln!("{}", e);
                            abort();
                        }
                    }
                }

                println!("commit");
                let result = client
                    .commit(tkvs_protos::CommitRequest {
                        session_id: session_id.clone(),
                    })
                    .await;
                if let Err(e) = result {
                    if *is_kill.lock().unwrap() {
                        // コミットがエラーになった場合は適用されていてもされていなくても正常
                        return vec![committed_state.clone(), {
                            committed_state.extend(uncommitted_state);
                            committed_state
                        }];
                    } else {
                        eprintln!("{}", e);
                        abort();
                    }
                }
                committed_state.extend(uncommitted_state);
            }
        })
    };

    let dur = { std::time::Duration::from_millis(rand::thread_rng().gen_range(10..30)) };
    tokio::time::sleep(dur).await;
    *is_kill.lock().unwrap() = true;
    vm.start_kill().unwrap();
    for handle in handles {
        handle.await.unwrap();
    }
    let committed_states = committed_handle.await.unwrap();
    println!("{:?}", committed_states);
}
