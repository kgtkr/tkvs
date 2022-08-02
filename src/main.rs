use std::{collections::HashMap, io::Write, sync::Arc};
use tkvs::DB;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let db = DB::new(std::env::args().nth(1).unwrap().into()).unwrap();
    let mut trxs = HashMap::new();
    let trx = db.new_trx().await;
    let mut trx_id = trx.id();
    trxs.insert(trx_id, Arc::new(Mutex::new(trx)));

    loop {
        println!("trx:{}> ", trx_id);
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
                    let mut trx = trx.try_lock().unwrap();
                    trx.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                    println!("trx:{} put success", trx_id);
                });
            }
            "get" => {
                let key = iter.next().unwrap().to_string();
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    let mut trx = trx.try_lock().unwrap();
                    let value = trx.get(key.as_bytes()).await.unwrap();
                    println!(
                        "trx:{} get success: {}",
                        trx_id,
                        value
                            .map(|value| String::from_utf8_lossy(&value).to_string())
                            .unwrap_or_else(|| "<not found>".to_string())
                    );
                });
            }
            "delete" => {
                let key = iter.next().unwrap().to_string();
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    let mut trx = trx.try_lock().unwrap();

                    trx.delete(key.as_bytes()).await.unwrap();
                    println!("trx:{} delete success", trx_id);
                });
            }
            "commit" => {
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    let mut trx = trx.try_lock().unwrap();

                    trx.commit().await.unwrap();
                    println!("trx:{} commit success", trx_id);
                });
            }
            "abort" => {
                let trx = trxs.get(&trx_id).unwrap().clone();

                tokio::spawn(async move {
                    let mut trx = trx.try_lock().unwrap();

                    trx.abort().await;
                    println!("trx:{} abort success", trx_id);
                });
            }
            "snapshot" => {
                db.snapshot().await.unwrap();
            }
            "new-trx" => {
                let trx = db.new_trx().await;
                trx_id = trx.id();
                trxs.insert(trx_id, Arc::new(Mutex::new(trx)));
            }
            "sw-trx" => {
                trx_id = iter.next().unwrap().parse().unwrap();
            }
            _ => println!("unknown command: {}", cmd),
        }
    }
}
