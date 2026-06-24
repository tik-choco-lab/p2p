use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrustKey {
    pub peer_id: String,
    pub forward_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustEntry {
    pub key: TrustKey,
    pub decision: TrustDecision,
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

    pub async fn list(&self) -> Vec<TrustEntry> {
        let mut entries = self
            .entries
            .read()
            .await
            .iter()
            .map(|(key, decision)| TrustEntry {
                key: key.clone(),
                decision: *decision,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| {
            a.key
                .peer_id
                .cmp(&b.key.peer_id)
                .then_with(|| a.key.forward_key.cmp(&b.key.forward_key))
        });
        entries
    }

    pub async fn remember(&self, key: TrustKey, decision: TrustDecision) -> Result<()> {
        self.entries.write().await.insert(key, decision);
        self.persist().await
    }

    pub async fn remove(&self, key: &TrustKey) -> Result<bool> {
        let removed = self.entries.write().await.remove(key).is_some();
        if removed {
            self.persist().await?;
        }
        Ok(removed)
    }

    async fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let text = serde_json::to_string_pretty(&self.list().await)?;
        tokio::fs::write(&self.path, text).await?;
        Ok(())
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
