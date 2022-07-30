use std::io::Write;
use tkvs::DB;

fn main() {
    let mut db = DB::new(std::env::args().nth(1).unwrap().into()).unwrap();
    loop {
        print!("> ");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let mut iter = line.split_whitespace();
        let cmd = iter.next().unwrap();
        match cmd {
            "put" => {
                let key = iter.next().unwrap();
                let value = iter.next().unwrap();
                db.put(key.as_bytes(), value.as_bytes());
            }
            "get" => {
                let key = iter.next().unwrap();
                println!(
                    "{}",
                    db.get(key.as_bytes())
                        .map(|value| String::from_utf8_lossy(&value).to_string())
                        .unwrap_or("<not found>".to_string())
                );
            }
            "delete" => {
                let key = iter.next().unwrap();
                db.delete(key.as_bytes());
            }
            "commit" => {
                db.commit().unwrap();
            }
            "abort" => {
                db.abort();
            }
            "snapshot" => {
                db.snapshot().unwrap();
            }
            _ => println!("unknown command: {}", cmd),
        }
    }
}
