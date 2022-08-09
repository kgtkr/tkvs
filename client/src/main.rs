use std::io::Write;

use clap::{Parser, Subcommand};
use tokio::io::AsyncBufReadExt;

#[derive(Parser)]
#[clap(name = "tkvs-client", author = "kgtkr")]
struct AppArg {
    endpoint: String,
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
}

#[tokio::main]
async fn main() {
    let arg: AppArg = AppArg::parse();

    let mut client = tkvs_protos::tkvs_client::TkvsClient::connect(arg.endpoint)
        .await
        .unwrap();
    let session_id = client
        .start_session(tkvs_protos::StartSessionRequest {})
        .await
        .unwrap()
        .into_inner()
        .session_id;

    {
        let client = client.clone();
        let session_id = session_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                // session busyで失敗する可能性がある
                let _ = client
                    .clone()
                    .keep_alive_session(tkvs_protos::KeepAliveSessionRequest {
                        session_id: session_id.clone(),
                    })
                    .await;
            }
        });
    }

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    while let Some(line) = {
        print!("> ");
        std::io::stdout().flush().unwrap();
        lines.next_line().await.unwrap()
    } {
        let arg = CmdArg::try_parse_from(&mut std::iter::once("").chain(line.split_whitespace()));
        match arg {
            Ok(arg) => match arg.action {
                CmdAction::Put { key, value } => {
                    match client
                        .put(tkvs_protos::PutRequest {
                            session_id: session_id.clone(),
                            key: key.into_bytes(),
                            value: value.into_bytes(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("ok");
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Get { key } => {
                    match client
                        .get(tkvs_protos::GetRequest {
                            session_id: session_id.clone(),
                            key: key.into_bytes(),
                        })
                        .await
                    {
                        Ok(result) => {
                            println!(
                                "ok: {}",
                                result
                                    .into_inner()
                                    .value
                                    .map(|value| String::from_utf8_lossy(value.as_slice())
                                        .to_string())
                                    .unwrap_or("<none>".to_string())
                            );
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Delete { key } => {
                    match client
                        .delete(tkvs_protos::DeleteRequest {
                            session_id: session_id.clone(),
                            key: key.into_bytes(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("ok");
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Commit => {
                    match client
                        .commit(tkvs_protos::CommitRequest {
                            session_id: session_id.clone(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("ok");
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Abort => {
                    match client
                        .abort(tkvs_protos::AbortRequest {
                            session_id: session_id.clone(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("ok");
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Snapshot => {
                    match client.snapshot(tkvs_protos::SnapshotRequest {}).await {
                        Ok(_) => {
                            println!("ok");
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
            },
            Err(e) => {
                println!("{}", format!("{}", e).trim());
            }
        }
    }
}
