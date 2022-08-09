#![deny(warnings)]

use bytes::Bytes;
use dashmap::mapref;
use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use std::net::SocketAddr;
use std::time::Instant;
use tkvs_core::{Trx, DB};
use tonic::{transport::Server, Code, Request, Response, Status};
mod app_config;

const MAX_TTL: u64 = 60;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = app_config::AppConfig::from_env().unwrap();

    let addr = SocketAddr::new(config.ip, config.port);
    let tkvs = TkvsService {
        sessions: DashMap::new(),
        db: DB::new(config.data.into()).unwrap(),
    };

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
    trx: Trx,
    expire: Instant,
}

impl Session {
    fn update_expire(&mut self) {
        self.expire = Instant::now() + std::time::Duration::from_secs(MAX_TTL);
    }
}

#[derive(Debug)]
struct TkvsService {
    sessions: DashMap<String, Session>,
    db: DB,
}

impl TkvsService {
    fn get_session_entry(
        &self,
        session_id: String,
    ) -> Result<mapref::entry::Entry<String, Session>, Status> {
        self.sessions
            .try_entry(session_id)
            .ok_or_else(|| Status::new(Code::Unavailable, "session is busy".to_string()))
    }

    fn get_session(
        &self,
        session_id: String,
    ) -> Result<mapref::entry::OccupiedEntry<String, Session>, Status> {
        match self.get_session_entry(session_id)? {
            mapref::entry::Entry::Occupied(entry) => Ok(entry),
            mapref::entry::Entry::Vacant(_) => Err(Status::new(
                Code::NotFound,
                "session is not found".to_string(),
            )),
        }
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
            trx,
            expire: Instant::now() + std::time::Duration::from_secs(MAX_TTL),
        };
        self.sessions.insert(session_id.clone(), session);
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
        let session = self.get_session(message.session_id)?;
        session.remove();
        let response = tkvs_protos::EndSessionResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn keep_alive_session(
        &self,
        request: Request<tkvs_protos::KeepAliveSessionRequest>,
    ) -> Result<Response<tkvs_protos::KeepAliveSessionResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        session.update_expire();
        let response = tkvs_protos::KeepAliveSessionResponse { ttl: MAX_TTL };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn get(
        &self,
        request: Request<tkvs_protos::GetRequest>,
    ) -> Result<Response<tkvs_protos::GetResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        let trx = &mut session.trx;
        let value = trx
            .get(&Bytes::from(message.key))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        session.update_expire();
        let response = tkvs_protos::GetResponse {
            value: value.map(|b| Vec::from(&b[..])),
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn put(
        &self,
        request: Request<tkvs_protos::PutRequest>,
    ) -> Result<Response<tkvs_protos::PutResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        let trx = &mut session.trx;
        trx.put(Bytes::from(message.key), Bytes::from(message.value))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        session.update_expire();
        let response = tkvs_protos::PutResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn delete(
        &self,
        request: Request<tkvs_protos::DeleteRequest>,
    ) -> Result<Response<tkvs_protos::DeleteResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        let trx = &mut session.trx;
        trx.delete(Bytes::from(message.key))
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        session.update_expire();
        let response = tkvs_protos::DeleteResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn commit(
        &self,
        request: Request<tkvs_protos::CommitRequest>,
    ) -> Result<Response<tkvs_protos::CommitResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        let trx = &mut session.trx;
        trx.commit()
            .await
            .map_err(|e| Status::new(Code::Aborted, e.to_string()))?;
        session.update_expire();
        let response = tkvs_protos::CommitResponse {};
        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn abort(
        &self,
        request: Request<tkvs_protos::AbortRequest>,
    ) -> Result<Response<tkvs_protos::AbortResponse>, Status> {
        let message = request.into_inner();
        let mut session = self.get_session(message.session_id)?;
        let session = session.get_mut();
        let trx = &mut session.trx;
        trx.abort().await;
        session.update_expire();
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
