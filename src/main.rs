use std::{collections::HashMap, future::Future, io::Write, sync::Arc};
use tkvs::DB;
use tokio::sync::Mutex;

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
    let db = DB::new(std::env::args().nth(1).unwrap().into()).unwrap();
    let mut trxs = HashMap::new();
    let trx = db.new_trx().await;
    let mut trx_id = trx.id();
    trxs.insert(trx_id, Arc::new(Mutex::new(trx)));

    loop {
        println!("trx:{}>", trx_id);
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let mut iter = line.split_whitespace();
        let cmd = iter.next().unwrap();
        let db = db.clone();
        match cmd {
            "put" => {
                let key = iter.next().unwrap().to_string();
                let value = iter.next().unwrap().to_string();
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    task_run(
                        format!("trx:{} put", trx_id),
                        (|| async {
                            let mut trx =
                                trx.try_lock().map_err(|_| anyhow::anyhow!("trx is busy"))?;
                            trx.put(key.as_bytes(), value.as_bytes()).await?;
                            Ok(None)
                        })(),
                    )
                    .await;
                });
            }
            "get" => {
                let key = iter.next().unwrap().to_string();
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    task_run(
                        format!("trx:{} get", trx_id),
                        (|| async {
                            let mut trx =
                                trx.try_lock().map_err(|_| anyhow::anyhow!("trx is busy"))?;
                            let value = trx.get(key.as_bytes()).await?;
                            Ok(Some(
                                value
                                    .map(|value| String::from_utf8_lossy(&value).to_string())
                                    .unwrap_or_else(|| "<not found>".to_string()),
                            ))
                        })(),
                    )
                    .await;
                });
            }
            "delete" => {
                let key = iter.next().unwrap().to_string();
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    task_run(
                        format!("trx:{} delete", trx_id),
                        (|| async {
                            let mut trx =
                                trx.try_lock().map_err(|_| anyhow::anyhow!("trx is busy"))?;

                            trx.delete(key.as_bytes()).await?;
                            Ok(None)
                        })(),
                    )
                    .await;
                });
            }
            "commit" => {
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    task_run(
                        format!("trx:{} commit", trx_id),
                        (|| async {
                            let mut trx =
                                trx.try_lock().map_err(|_| anyhow::anyhow!("trx is busy"))?;

                            trx.commit().await?;
                            Ok(None)
                        })(),
                    )
                    .await;
                });
            }
            "abort" => {
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    task_run(
                        format!("trx:{} abort", trx_id),
                        (|| async {
                            let mut trx =
                                trx.try_lock().map_err(|_| anyhow::anyhow!("trx is busy"))?;

                            trx.abort().await;

                            Ok(None)
                        })(),
                    )
                    .await;
                });
            }
            "snapshot" => {
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
            "new-trx" => {
                let trx = db.new_trx().await;
                trx_id = trx.id();
                trxs.insert(trx_id, Arc::new(Mutex::new(trx)));
            }
            "sw-trx" => {
                let id = iter.next().unwrap().parse().unwrap();
                if trxs.contains_key(&id) {
                    trx_id = id;
                } else {
                    println!("trx-{} is not found", id);
                }
            }
            _ => println!("unknown command: {}", cmd),
        }
    }
}
