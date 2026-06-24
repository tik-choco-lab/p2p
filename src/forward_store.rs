use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::controller::{Direction, ForwardSpec, Proto};

/// A persisted forward definition. Stored with primitive fields so the
/// controller enums don't need serde derives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedForward {
    /// "serve" or "connect".
    pub direction: String,
    /// "tcp" or "udp".
    pub proto: String,
    pub addr: String,
    pub listen_port: i32,
    pub target: String,
}

impl PersistedForward {
    pub fn from_spec(spec: &ForwardSpec) -> Self {
        let direction = match spec.direction {
            Direction::Serve => "serve",
            Direction::Connect => "connect",
        };
        Self {
            direction: direction.to_string(),
            proto: spec.proto.as_str().to_string(),
            addr: spec.addr.clone(),
            listen_port: spec.listen_port,
            target: spec.target.clone(),
        }
    }

    pub fn to_spec(&self) -> Result<ForwardSpec> {
        let direction = match self.direction.as_str() {
            "serve" => Direction::Serve,
            "connect" => Direction::Connect,
            other => anyhow::bail!("unknown direction: {}", other),
        };
        Ok(ForwardSpec {
            direction,
            proto: Proto::from_name(&self.proto)?,
            addr: self.addr.clone(),
            listen_port: self.listen_port,
            target: self.target.clone(),
        })
    }
}

/// Persists approved forward definitions so they are re-established on the next
/// launch. Keyed by target so each forward appears once.
#[derive(Debug, Clone)]
pub struct ForwardStore {
    path: PathBuf,
    entries: Arc<RwLock<Vec<PersistedForward>>>,
}

impl ForwardStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => serde_json::from_str::<Vec<PersistedForward>>(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    pub async fn list(&self) -> Vec<PersistedForward> {
        self.entries.read().await.clone()
    }

    pub async fn add(&self, spec: &ForwardSpec) -> Result<()> {
        let entry = PersistedForward::from_spec(spec);
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.target != entry.target);
            entries.push(entry);
        }
        self.persist().await
    }

    pub async fn remove(&self, target: &str) -> Result<bool> {
        let removed = {
            let mut entries = self.entries.write().await;
            let before = entries.len();
            entries.retain(|e| e.target != target);
            entries.len() != before
        };
        if removed {
            self.persist().await?;
        }
        Ok(removed)
    }

    async fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let text = serde_json::to_string_pretty(&*self.entries.read().await)?;
        tokio::fs::write(&self.path, text).await?;
        Ok(())
    }
}

pub fn default_forward_store_path() -> PathBuf {
    let base = std::env::var_os("P2P_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("APPDATA").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("p2p").join("forwards.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ForwardSpec {
        ForwardSpec {
            direction: Direction::Connect,
            proto: Proto::Tcp,
            addr: String::new(),
            listen_port: 8080,
            target: "tcp:127.0.0.1:80".into(),
        }
    }

    #[tokio::test]
    async fn round_trips_specs_through_disk() {
        let dir = std::env::temp_dir().join(format!("p2p-fwd-{}", uuid::Uuid::new_v4()));
        let path = dir.join("forwards.json");

        let store = ForwardStore::load(&path).await.unwrap();
        store.add(&spec()).await.unwrap();

        let reloaded = ForwardStore::load(&path).await.unwrap();
        let entries = reloaded.list().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].to_spec().unwrap(), spec());

        assert!(reloaded.remove("tcp:127.0.0.1:80").await.unwrap());
        assert!(ForwardStore::load(&path)
            .await
            .unwrap()
            .list()
            .await
            .is_empty());

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn add_is_idempotent_per_target() {
        let dir = std::env::temp_dir().join(format!("p2p-fwd-{}", uuid::Uuid::new_v4()));
        let path = dir.join("forwards.json");
        let store = ForwardStore::load(&path).await.unwrap();
        store.add(&spec()).await.unwrap();
        store.add(&spec()).await.unwrap();
        assert_eq!(store.list().await.len(), 1);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
