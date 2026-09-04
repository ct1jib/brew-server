use serde::Serialize;
use std::{collections::{HashMap, VecDeque}, time::{SystemTime, UNIX_EPOCH}};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

fn now_ms() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[derive(Debug, Clone, Serialize)]
pub struct CallRecord { pub uuid: Uuid, pub kind: String, pub source: u32, pub destination: u32, pub priority: u8, pub started_at_ms: u64, pub ended_at_ms: Option<u64>, pub voice_frames: u64 }
#[derive(Debug, Clone, Serialize)]
pub struct SdsRecord { pub uuid: Uuid, pub source: u32, pub destination: u32, pub at_ms: u64, pub reports: u32 }
#[derive(Debug, Clone, Serialize)]
pub struct LiveEvent { pub event: String, pub at_ms: u64, pub data: serde_json::Value }
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot { pub connected_bluestations: usize, pub subscribers: usize, pub groups: usize, pub active_calls: Vec<CallRecord>, pub recent_calls: Vec<CallRecord>, pub recent_sds: Vec<SdsRecord>, pub total_calls: u64, pub total_sds: u64, pub voice_frames: u64 }

#[derive(Default)] struct Inner { active: HashMap<Uuid, CallRecord>, calls: VecDeque<CallRecord>, sds: VecDeque<SdsRecord>, total_calls: u64, total_sds: u64, voice_frames: u64 }

pub struct Monitor { inner: RwLock<Inner>, tx: broadcast::Sender<LiveEvent> }
impl Monitor {
    pub fn new() -> Self { let (tx, _) = broadcast::channel(256); Self { inner: RwLock::new(Inner::default()), tx } }
    pub fn subscribe(&self) -> broadcast::Receiver<LiveEvent> { self.tx.subscribe() }
    pub fn emit(&self, event: &str, data: serde_json::Value) { let _ = self.tx.send(LiveEvent { event: event.into(), at_ms: now_ms(), data }); }
    pub async fn call_started(&self, uuid: Uuid, kind: &str, source: u32, destination: u32, priority: u8) {
        let rec = CallRecord { uuid, kind: kind.into(), source, destination, priority, started_at_ms: now_ms(), ended_at_ms: None, voice_frames: 0 };
        let mut i = self.inner.write().await; if i.active.insert(uuid, rec.clone()).is_none() { i.total_calls += 1; }
        drop(i); self.emit("call_started", serde_json::json!(rec));
    }
    pub async fn call_ended(&self, uuid: Uuid) { let mut i = self.inner.write().await; if let Some(mut r)=i.active.remove(&uuid) { r.ended_at_ms=Some(now_ms()); i.calls.push_front(r.clone()); while i.calls.len()>200 { i.calls.pop_back(); } drop(i); self.emit("call_ended", serde_json::json!(r)); } }
    pub async fn voice_frame(&self, uuid: Uuid) { let mut i=self.inner.write().await; i.voice_frames+=1; if let Some(r)=i.active.get_mut(&uuid){r.voice_frames+=1;} }
    pub async fn sds(&self, uuid: Uuid, source: u32, destination: u32) { let r=SdsRecord{uuid,source,destination,at_ms:now_ms(),reports:0}; let mut i=self.inner.write().await; i.total_sds+=1; i.sds.push_front(r.clone()); while i.sds.len()>200{i.sds.pop_back();} drop(i); self.emit("sds",serde_json::json!(r)); }
    pub async fn sds_report(&self, uuid: Uuid) { let mut i=self.inner.write().await; if let Some(r)=i.sds.iter_mut().find(|r|r.uuid==uuid){r.reports+=1;} }
    pub async fn snapshot(&self, clients: usize, subscribers: usize, groups: usize) -> Snapshot { let i=self.inner.read().await; Snapshot{connected_bluestations:clients,subscribers,groups,active_calls:i.active.values().cloned().collect(),recent_calls:i.calls.iter().take(50).cloned().collect(),recent_sds:i.sds.iter().take(50).cloned().collect(),total_calls:i.total_calls,total_sds:i.total_sds,voice_frames:i.voice_frames} }
}
