use std::io::Write;
use tkvs::DB;
use tokio::task;

#[tokio::main]
async fn main() {
    let db = DB::new(std::env::args().nth(1).unwrap().into()).unwrap();
    let mut trx_id = db.new_trx().await;

    loop {
        print!("trx:{}> ", trx_id);
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

                tokio::spawn(async move {
                    db.put(trx_id, key.as_bytes(), value.as_bytes()).await;
                    println!("trx:{} put success", trx_id);
                });
            }
            "get" => {
                let key = iter.next().unwrap().to_string();

                tokio::spawn(async move {
                    let value = db.get(trx_id, key.as_bytes()).await;
                    println!(
                        "trx:{} get success: {}",
                        trx_id,
                        value
                            .map(|value| String::from_utf8_lossy(&value).to_string())
                            .unwrap_or("<not found>".to_string())
                    );
                });
            }
            "delete" => {
                let key = iter.next().unwrap().to_string();

                tokio::spawn(async move {
                    db.delete(trx_id, key.as_bytes()).await;
                    println!("trx:{} delete success", trx_id);
                });
            }
            "commit" => {
                tokio::spawn(async move {
                    db.commit(trx_id).await.unwrap();
                    println!("trx:{} commit success", trx_id);
                });
            }
            "abort" => {
                tokio::spawn(async move {
                    db.abort(trx_id).await;
                    println!("trx:{} abort success", trx_id);
                });
            }
            "snapshot" => {
                db.snapshot().await.unwrap();
            }
            "new-trx" => {
                trx_id = db.new_trx().await;
            }
            "sw-trx" => {
                trx_id = iter.next().unwrap().parse().unwrap();
            }
            _ => println!("unknown command: {}", cmd),
        }
    }
}
