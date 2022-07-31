use std::io::Write;
use tkvs::DB;

#[tokio::main]
async fn main() {
    let mut db = DB::new(std::env::args().nth(1).unwrap().into()).unwrap();
    let mut trx_id = db.new_trx();

    loop {
        print!("trx:{}> ", trx_id);
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let mut iter = line.split_whitespace();
        let cmd = iter.next().unwrap();
        match cmd {
            "put" => {
                let key = iter.next().unwrap();
                let value = iter.next().unwrap();
                db.put(trx_id, key.as_bytes(), value.as_bytes()).await;
            }
            "get" => {
                let key = iter.next().unwrap();
                println!(
                    "{}",
                    db.get(trx_id, key.as_bytes())
                        .await
                        .map(|value| String::from_utf8_lossy(&value).to_string())
                        .unwrap_or("<not found>".to_string())
                );
            }
            "delete" => {
                let key = iter.next().unwrap();
                db.delete(trx_id, key.as_bytes()).await;
            }
            "commit" => {
                db.commit(trx_id).await.unwrap();
            }
            "abort" => {
                db.abort(trx_id).await;
            }
            "snapshot" => {
                db.snapshot().unwrap();
            }
            "new-trx" => {
                trx_id = db.new_trx();
            }
            "sw-trx" => {
                trx_id = iter.next().unwrap().parse().unwrap();
            }
            _ => println!("unknown command: {}", cmd),
        }
    }
}
