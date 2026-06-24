use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = AuthDecision> + Send + 'a>>;
pub type SharedAuthorizer = Arc<dyn ConnectionAuthorizer>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrustKey {
    pub peer_id: String,
    pub forward_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRequest {
    pub peer_id: String,
    pub forward_key: String,
    pub target_addr: String,
    pub proto: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny,
    #[allow(dead_code)]
    AllowAlways,
    #[allow(dead_code)]
    DenyAlways,
}

impl AuthDecision {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allow | Self::AllowAlways)
    }

    fn trust_decision(self) -> Option<TrustDecision> {
        match self {
            Self::AllowAlways => Some(TrustDecision::Allow),
            Self::DenyAlways => Some(TrustDecision::Deny),
            Self::Allow | Self::Deny => None,
        }
    }
}

pub trait ConnectionAuthorizer: Send + Sync {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a>;
}

pub fn allow_all() -> SharedAuthorizer {
    Arc::new(AllowAllAuthorizer)
}

#[derive(Debug, Default)]
struct AllowAllAuthorizer;

impl ConnectionAuthorizer for AllowAllAuthorizer {
    fn authorize<'a>(&'a self, _req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async { AuthDecision::Allow })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthPolicy {
    AutoAccept,
    AllowPeers(HashSet<String>),
    DenyUnknown,
}

impl AuthPolicy {
    pub fn allow_peers<I, S>(peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::AllowPeers(peers.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone)]
pub struct TrustStore {
    path: PathBuf,
    entries: Arc<RwLock<HashMap<TrustKey, TrustDecision>>>,
}

impl TrustStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => serde_json::from_str::<Vec<TrustEntry>>(&text)?
                .into_iter()
                .map(|entry| (entry.key, entry.decision))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    pub async fn get(&self, key: &TrustKey) -> Option<TrustDecision> {
        self.entries.read().await.get(key).copied()
    }

    pub async fn remember(&self, key: TrustKey, decision: TrustDecision) -> Result<()> {
        self.entries.write().await.insert(key, decision);
        self.persist().await
    }

    async fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let entries = self
            .entries
            .read()
            .await
            .iter()
            .map(|(key, decision)| TrustEntry {
                key: key.clone(),
                decision: *decision,
            })
            .collect::<Vec<_>>();
        let text = serde_json::to_string_pretty(&entries)?;
        tokio::fs::write(&self.path, text).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrustEntry {
    key: TrustKey,
    decision: TrustDecision,
}

#[derive(Debug, Clone)]
pub struct PolicyAuthorizer {
    policy: AuthPolicy,
    store: TrustStore,
}

impl PolicyAuthorizer {
    pub fn new(policy: AuthPolicy, store: TrustStore) -> Self {
        Self { policy, store }
    }

    pub fn shared(policy: AuthPolicy, store: TrustStore) -> SharedAuthorizer {
        Arc::new(Self::new(policy, store))
    }

    async fn decide(&self, req: &AuthRequest) -> AuthDecision {
        let key = TrustKey {
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
        };
        if let Some(decision) = self.store.get(&key).await {
            return match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
        }

        let decision = match &self.policy {
            AuthPolicy::AutoAccept => AuthDecision::Allow,
            AuthPolicy::AllowPeers(peers) if peers.contains(&req.peer_id) => AuthDecision::Allow,
            AuthPolicy::AllowPeers(_) | AuthPolicy::DenyUnknown => AuthDecision::Deny,
        };
        if let Some(trust) = decision.trust_decision() {
            let _ = self.store.remember(key, trust).await;
        }
        decision
    }
}

impl ConnectionAuthorizer for PolicyAuthorizer {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async move { self.decide(req).await })
    }
}

pub fn default_trust_store_path() -> PathBuf {
    let base = std::env::var_os("P2P_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("APPDATA").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("p2p").join("trust.json")
}

#[cfg(test)]
mod tests;
