#![deny(warnings)]

use clap::{Parser, Subcommand};
use lazy_regex::regex_captures;
use rustyline::Editor;
use std::{ops::Bound, str::FromStr};

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
    Range { range: Range },
    Commit,
    Abort,
    Snapshot,
}

#[derive(Debug, Clone)]
struct Range(Bound<String>, Bound<String>);

impl FromStr for Range {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((_, start_symbol, start, end, end_symbol)) =
            regex_captures!(r"^([\(\[])(.*),(.*)([\)\]])$", s)
        {
            let start = if start.is_empty() {
                Bound::Unbounded
            } else if start_symbol == "[" {
                Bound::Included(start.to_string())
            } else {
                Bound::Excluded(start.to_string())
            };

            let end = if end.is_empty() {
                Bound::Unbounded
            } else if end_symbol == "]" {
                Bound::Included(end.to_string())
            } else {
                Bound::Excluded(end.to_string())
            };

            Ok(Range(start, end))
        } else {
            Err("invalid range. valid example: [a,b] or (a,b] or [,b] or [,]".to_string())
        }
    }
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

    let mut rl = Editor::<()>::new().unwrap();

    while let Ok(line) = rl.readline("> ") {
        let arg = CmdArg::try_parse_from(&mut std::iter::once("").chain(line.split_whitespace()));
        rl.add_history_entry(line.as_str());

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
                                    .unwrap_or_else(|| "<none>".to_string())
                            );
                        }
                        Err(e) => {
                            println!("err: {}", e);
                        }
                    }
                }
                CmdAction::Range { range } => {
                    fn convert_bound(bound: Bound<String>) -> tkvs_protos::Bound {
                        tkvs_protos::Bound {
                            bound: match bound {
                                Bound::Unbounded => None,
                                Bound::Included(key) => {
                                    Some(tkvs_protos::bound::Bound::Inclusive(key.into_bytes()))
                                }
                                Bound::Excluded(key) => {
                                    Some(tkvs_protos::bound::Bound::Exclusive(key.into_bytes()))
                                }
                            },
                        }
                    }

                    match client
                        .range(tkvs_protos::RangeRequest {
                            session_id: session_id.clone(),
                            start: Some(convert_bound(range.0)),
                            end: Some(convert_bound(range.1)),
                        })
                        .await
                    {
                        Ok(result) => {
                            for tkvs_protos::KeyValue { key, value } in
                                result.into_inner().key_values
                            {
                                println!(
                                    "{}:{}",
                                    String::from_utf8_lossy(key.as_slice()),
                                    String::from_utf8_lossy(value.as_slice())
                                );
                            }
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
