use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::io::Cursor;
use std::{collections::HashMap, future::Future, io::Write, sync::Arc};
use tkvs_core::DB;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;

// Parserを継承した構造体はArgの代わりに使用することが可能。
#[derive(Parser)]
#[clap(name = "tkvs", author = "kgtkr")]
struct AppArg {
    data_dir: String,
}

#[derive(Parser)]
#[clap(name = "")]
struct CmdArg {
    #[clap(subcommand)]
    action: CmdAction,
}

#[derive(Subcommand)]
enum CmdAction {
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    Commit,
    Abort,
    Snapshot,
    NewTrx,
    SwTrx { id: usize },
}

async fn task_run(name: String, future: impl Future<Output = anyhow::Result<Option<String>>>) {
    let result = future.await;
    print!("{}", name);
    match result {
        Ok(result) => {
            print!(" success");
            if let Some(result) = result {
                print!(" -> {}", result);
            }
        }
        Err(e) => {
            print!(" failed -> {}", e);
        }
    }
    println!();
}

#[tokio::main]
async fn main() {
    let arg: AppArg = AppArg::parse();

    let db = DB::new(arg.data_dir.into()).unwrap();
    let mut trxs = HashMap::new();
    let trx = db.new_trx().await;
    let mut trx_id = trx.id();
    trxs.insert(trx_id, Arc::new(Mutex::new(trx)));
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    while let Some(line) = {
        println!("trx:{}>", trx_id);
        lines.next_line().await.unwrap()
    } {
        let arg = CmdArg::try_parse_from(&mut std::iter::once("").chain(line.split_whitespace()));
        match arg {
            Ok(arg) => {
                let db = db.clone();
                match arg.action {
                    CmdAction::Put { key, value } => {
                        let trx = trxs.get(&trx_id).unwrap().clone();

                        tokio::spawn(async move {
                            task_run(
                                format!("trx:{} put", trx_id),
                                (|| async {
                                    let mut trx = trx
                                        .try_lock()
                                        .map_err(|_| anyhow::anyhow!("trx is busy"))?;
                                    trx.put(
                                        Bytes::copy_from_slice(key.as_bytes()),
                                        Bytes::copy_from_slice(value.as_bytes()),
                                    )
                                    .await?;
                                    Ok(None)
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::Get { key } => {
                        let trx = trxs.get(&trx_id).unwrap().clone();

                        tokio::spawn(async move {
                            task_run(
                                format!("trx:{} get", trx_id),
                                (|| async {
                                    let mut trx = trx
                                        .try_lock()
                                        .map_err(|_| anyhow::anyhow!("trx is busy"))?;
                                    let value =
                                        trx.get(&Bytes::copy_from_slice(key.as_bytes())).await?;
                                    Ok(Some(
                                        value
                                            .map(|value| {
                                                String::from_utf8_lossy(&value).to_string()
                                            })
                                            .unwrap_or_else(|| "<not found>".to_string()),
                                    ))
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::Delete { key } => {
                        let trx = trxs.get(&trx_id).unwrap().clone();

                        tokio::spawn(async move {
                            task_run(
                                format!("trx:{} delete", trx_id),
                                (|| async {
                                    let mut trx = trx
                                        .try_lock()
                                        .map_err(|_| anyhow::anyhow!("trx is busy"))?;

                                    trx.delete(Bytes::copy_from_slice(key.as_bytes())).await?;
                                    Ok(None)
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::Commit => {
                        let trx = trxs.get(&trx_id).unwrap().clone();

                        tokio::spawn(async move {
                            task_run(
                                format!("trx:{} commit", trx_id),
                                (|| async {
                                    let mut trx = trx
                                        .try_lock()
                                        .map_err(|_| anyhow::anyhow!("trx is busy"))?;

                                    trx.commit().await?;
                                    Ok(None)
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::Abort => {
                        let trx = trxs.get(&trx_id).unwrap().clone();

                        tokio::spawn(async move {
                            task_run(
                                format!("trx:{} abort", trx_id),
                                (|| async {
                                    let mut trx = trx
                                        .try_lock()
                                        .map_err(|_| anyhow::anyhow!("trx is busy"))?;

                                    trx.abort().await;

                                    Ok(None)
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::Snapshot => {
                        tokio::spawn(async move {
                            task_run(
                                format!("snapshot"),
                                (|| async {
                                    db.snapshot().await?;
                                    Ok(None)
                                })(),
                            )
                            .await;
                        });
                    }
                    CmdAction::NewTrx => {
                        let trx = db.new_trx().await;
                        trx_id = trx.id();
                        trxs.insert(trx_id, Arc::new(Mutex::new(trx)));
                    }
                    CmdAction::SwTrx { id } => {
                        if trxs.contains_key(&id) {
                            trx_id = id;
                        } else {
                            println!("trx-{} is not found", id);
                        }
                    }
                }
            }
            Err(e) => {
                println!("{}", format!("{}", e).trim());
            }
        }
    }
}
