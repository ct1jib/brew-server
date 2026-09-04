use crate::{config::Config, monitor::Monitor, telemetry::TelemetryState};
use std::{collections::{HashMap, HashSet}, time::{Duration, Instant}};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

pub type ClientId = Uuid;

#[derive(Clone)]
pub struct Client {
    pub tx: mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub client_id: ClientId,
    pub groups: HashSet<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Group,
    Private,
}

#[derive(Debug, Clone)]
pub struct ActiveCall {
    pub kind: CallKind,
    pub owner: ClientId,
    pub source_issi: u32,
    pub destination: u32,
    pub priority: u8,
    pub peers: HashSet<ClientId>,
}

#[derive(Debug, Clone)]
pub struct SdsRoute {
    pub source_client: ClientId,
    pub targets: HashSet<ClientId>,
    pub source_issi: u32,
    pub destination: u32,
    pub created_at: Instant,
}

#[derive(Default)]
pub struct Inner {
    pub clients: HashMap<ClientId, Client>,
    pub subscribers: HashMap<u32, Subscriber>,
    pub group_clients: HashMap<u32, HashSet<ClientId>>,
    pub calls: HashMap<Uuid, ActiveCall>,
    pub group_floor: HashMap<u32, Uuid>,
    pub sds_routes: HashMap<Uuid, SdsRoute>,
    pub digest_nonces: HashMap<String, Instant>,
    pub auth_sessions: HashMap<String, Instant>,
}

pub struct AppState {
    pub config: Config,
    pub inner: RwLock<Inner>,
    pub monitor: Monitor,
    pub telemetry: RwLock<TelemetryState>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            inner: RwLock::new(Inner::default()),
            monitor: Monitor::new(),
            telemetry: RwLock::new(TelemetryState::default()),
        }
    }

    pub async fn send_many(&self, clients: &HashSet<ClientId>, packet: &[u8]) {
        let inner = self.inner.read().await;
        for id in clients {
            if let Some(client) = inner.clients.get(id) {
                let _ = client.tx.send(packet.to_vec());
            }
        }
    }

    pub async fn purge_ephemeral(&self) {
        let now = Instant::now();
        let session_ttl = Duration::from_secs(self.config.auth.session_ttl_seconds.max(1));
        let mut inner = self.inner.write().await;
        inner.digest_nonces.retain(|_, at| now.duration_since(*at) < Duration::from_secs(120));
        inner.auth_sessions.retain(|_, at| now.duration_since(*at) < session_ttl);
        inner.sds_routes.retain(|_, route| now.duration_since(route.created_at) < Duration::from_secs(60));
    }

    pub async fn cleanup_client(&self, id: ClientId) {
        let mut inner = self.inner.write().await;
        inner.clients.remove(&id);

        let removed_issis: Vec<u32> = inner.subscribers.iter()
            .filter_map(|(issi, sub)| (sub.client_id == id).then_some(*issi)).collect();
        for issi in removed_issis { inner.subscribers.remove(&issi); }

        for clients in inner.group_clients.values_mut() { clients.remove(&id); }
        inner.group_clients.retain(|_, clients| !clients.is_empty());

        let removed_calls: Vec<Uuid> = inner.calls.iter()
            .filter_map(|(uuid, call)| (call.owner == id || call.peers.contains(&id)).then_some(*uuid)).collect();
        for uuid in removed_calls {
            if let Some(call) = inner.calls.remove(&uuid) {
                if call.kind == CallKind::Group && inner.group_floor.get(&call.destination) == Some(&uuid) {
                    inner.group_floor.remove(&call.destination);
                }
            }
        }
        inner.sds_routes.retain(|_, route| route.source_client != id && !route.targets.contains(&id));
    }
}
