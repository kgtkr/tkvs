#![deny(warnings)]
#![allow(clippy::mutable_key_type)] // bytes::Bytes で問題が発生するため

use bytes::Bytes;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Instant;
use std::{net::SocketAddr, sync::Mutex};
use tkvs_core::{Trx, DB};
use tokio::sync::Mutex as TokioMutex;
use tonic::{transport::Server, Code, Request, Response, Status};
mod app_config;

const MAX_TTL: u64 = 60;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = app_config::AppConfig::from_env().unwrap();

    let addr = SocketAddr::new(config.ip, config.port);
    let tkvs = TkvsService {
        sessions: Arc::new(Mutex::new(HashMap::new())),
        db: DB::new(config.data.into()).unwrap(),
    };

    // sessions expire check
    {
        let sessions = tkvs.sessions.clone();
        tokio::spawn(async move {
            loop {
                {
                    let mut sessions = sessions.lock().unwrap();
                    let now = Instant::now();
                    sessions.retain(|_, session| session.expire > now);
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tkvs_protos::REFLECTION_SERVICE_DESCRIPTOR)
        .build()
        .unwrap();

    tracing::info!("gRPC server is starting on {}", addr);

    Server::builder()
        .trace_fn(|_| tracing::info_span!("gRPC server"))
        .add_service(tkvs_protos::tkvs_server::TkvsServer::new(tkvs))
        .add_service(reflection)
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Debug)]
struct Session {
    trx: Arc<TokioMutex<Trx>>,
    expire: Instant,
}

impl Session {
    fn update_expire(&mut self) {
        self.expire = Instant::now() + std::time::Duration::from_secs(MAX_TTL);
    }
}

#[derive(Debug)]
struct TkvsService {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    db: DB,
}

fn session_busy_status() -> Status {
    Status::new(Code::Unavailable, "session is busy".to_string())
}

impl TkvsService {
    fn get_trx(&self, session_id: &str) -> Result<Arc<TokioMutex<Trx>>, Status> {
        let trx = self.session_with(session_id.to_string(), |session| session.get().trx.clone())?;

        Ok(trx)
    }

    fn remove_session(&self, session_id: &str) -> Result<(), Status> {
        self.session_with(session_id.to_string(), |session| session.remove())?;
        Ok(())
    }

    fn session_with<A>(
        &self,
        session_id: String,
        f: impl FnOnce(OccupiedEntry<String, Session>) -> A,
    ) -> Result<A, Status> {
        let mut sessions = self.sessions.lock().unwrap();
        let session_entry = match sessions.entry(session_id) {
            Entry::Occupied(entry) => Ok(entry),
            Entry::Vacant(_) => Err(Status::new(
                Code::NotFound,
                "session is not found".to_string(),
            )),
        }?;
        Ok(f(session_entry))
    }
}

#[tonic::async_trait]
impl tkvs_protos::tkvs_server::Tkvs for TkvsService {
    #[tracing::instrument]
    async fn start_session(
        &self,
        _request: Request<tkvs_protos::StartSessionRequest>,
    ) -> Result<Response<tkvs_protos::StartSessionResponse>, Status> {
        let session_id = {
            let mut rng = rand::thread_rng();
            (0..32)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect::<String>()
        };
        let trx = self.db.new_trx().await;
        let session = Session {
            trx: Arc::new(TokioMutex::new(trx)),
            expire: Instant::now() + std::time::Duration::from_secs(MAX_TTL),
        };
        {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.insert(session_id.clone(), session);
            drop(sessions);
        }
        let response = tkvs_protos::StartSessionResponse {
            session_id,
            ttl: MAX_TTL,
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn end_session(
        &self,
        request: Request<tkvs_protos::EndSessionRequest>,
    ) -> Result<Response<tkvs_protos::EndSessionResponse>, Status> {
        let message = request.into_inner();
        self.remove_session(message.session_id.as_str())?;
        let response = tkvs_protos::EndSessionResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn keep_alive_session(
        &self,
        request: Request<tkvs_protos::KeepAliveSessionRequest>,
    ) -> Result<Response<tkvs_protos::KeepAliveSessionResponse>, Status> {
        let message = request.into_inner();
        self.session_with(message.session_id, |mut session| {
            session.get_mut().update_expire()
        })?;
        let response = tkvs_protos::KeepAliveSessionResponse { ttl: MAX_TTL };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn get(
        &self,
        request: Request<tkvs_protos::GetRequest>,
    ) -> Result<Response<tkvs_protos::GetResponse>, Status> {
        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        let value = trx
            .get(&Bytes::from(message.key))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        let response = tkvs_protos::GetResponse {
            value: value.map(|b| Vec::from(&b[..])),
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn range(
        &self,
        request: Request<tkvs_protos::RangeRequest>,
    ) -> Result<Response<tkvs_protos::RangeResponse>, Status> {
        fn bound_convert(bound: Option<tkvs_protos::bound::Bound>) -> Bound<Bytes> {
            match bound {
                Some(tkvs_protos::bound::Bound::Inclusive(key)) => {
                    Bound::Included(Bytes::from(key))
                }
                Some(tkvs_protos::bound::Bound::Exclusive(key)) => {
                    Bound::Excluded(Bytes::from(key))
                }
                None => Bound::Unbounded,
            }
        }

        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        let start = bound_convert(message.start.and_then(|bound| bound.bound));
        let end = bound_convert(message.end.and_then(|bound| bound.bound));

        let value = trx
            .range((start, end))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        let response = tkvs_protos::RangeResponse {
            key_values: value
                .into_iter()
                .map(|(k, v)| tkvs_protos::KeyValue {
                    key: k.to_vec(),
                    value: v.to_vec(),
                })
                .collect(),
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn put(
        &self,
        request: Request<tkvs_protos::PutRequest>,
    ) -> Result<Response<tkvs_protos::PutResponse>, Status> {
        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        trx.put(Bytes::from(message.key), Bytes::from(message.value))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        let response = tkvs_protos::PutResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn delete(
        &self,
        request: Request<tkvs_protos::DeleteRequest>,
    ) -> Result<Response<tkvs_protos::DeleteResponse>, Status> {
        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        trx.delete(Bytes::from(message.key))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        let response = tkvs_protos::DeleteResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn commit(
        &self,
        request: Request<tkvs_protos::CommitRequest>,
    ) -> Result<Response<tkvs_protos::CommitResponse>, Status> {
        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        trx.commit()
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        let response = tkvs_protos::CommitResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn abort(
        &self,
        request: Request<tkvs_protos::AbortRequest>,
    ) -> Result<Response<tkvs_protos::AbortResponse>, Status> {
        let message = request.into_inner();
        let trx = self.get_trx(message.session_id.as_str())?;
        let mut trx = trx.try_lock().map_err(|_| session_busy_status())?;
        trx.abort().await;
        let response = tkvs_protos::AbortResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn snapshot(
        &self,
        _request: Request<tkvs_protos::SnapshotRequest>,
    ) -> Result<Response<tkvs_protos::SnapshotResponse>, Status> {
        self.db.snapshot().await.map_err(|e| {
            tracing::error!("{}", e);
            Status::new(Code::Aborted, e.to_string())
        })?;
        let response = tkvs_protos::SnapshotResponse {};
        Ok(Response::new(response))
    }
}
