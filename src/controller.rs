use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::error;

use crate::forward_runtime::ForwardRuntime;
use crate::rtc::RTCManager;
use crate::{tcp, udp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Serve,
    Connect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            _ => Err(anyhow!("unsupported protocol: {}", name)),
        }
    }

    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardSpec {
    pub direction: Direction,
    pub proto: Proto,
    pub addr: String,
    pub listen_port: i32,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardState {
    Listening,
    Error(String),
    #[allow(dead_code)]
    Stopped,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardStatus {
    pub key: String,
    pub spec: ForwardSpec,
    pub active_conns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub state: ForwardState,
}

#[allow(dead_code)]
struct ForwardHandle {
    spec: ForwardSpec,
    runtime: ForwardRuntime,
    state: ForwardState,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ForwardController {
    rtc_manager: Option<RTCManager>,
    forwards: Arc<RwLock<HashMap<String, ForwardHandle>>>,
}

impl ForwardController {
    pub fn new(rtc_manager: RTCManager) -> Self {
        Self {
            rtc_manager: Some(rtc_manager),
            forwards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    fn new_inert() -> Self {
        Self {
            rtc_manager: None,
            forwards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_forward(&self, spec: ForwardSpec) -> Result<String> {
        let key = spec.target.clone();
        if key.is_empty() {
            return Err(anyhow!("forward target must not be empty"));
        }

        let mut forwards = self.forwards.write().await;
        if forwards.contains_key(&key) {
            return Err(anyhow!("forward already exists: {}", key));
        }
        let runtime = ForwardRuntime::new();
        forwards.insert(
            key.clone(),
            ForwardHandle {
                spec: spec.clone(),
                runtime: runtime.clone(),
                state: ForwardState::Listening,
                task: None,
            },
        );
        drop(forwards);

        let task = self.spawn_forward(spec, key.clone(), runtime).await;
        if let Some(task) = task {
            if let Some(handle) = self.forwards.write().await.get_mut(&key) {
                handle.task = Some(task);
            }
        }
        Ok(key)
    }

    #[allow(dead_code)]
    pub async fn remove_forward(&self, key: &str) -> Result<()> {
        let Some(handle) = self.forwards.write().await.remove(key) else {
            return Err(anyhow!("forward not found: {}", key));
        };

        handle.runtime.cancel();
        if let Some(task) = handle.task {
            task.abort();
        }
        if handle.spec.direction == Direction::Serve {
            if let Some(manager) = &self.rtc_manager {
                manager.unpublish_tunnel_target(key).await;
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn list_forwards(&self) -> Vec<ForwardStatus> {
        let forwards = self.forwards.read().await;
        let mut statuses = forwards
            .iter()
            .map(|(key, handle)| {
                let metrics = handle.runtime.metrics();
                ForwardStatus {
                    key: key.clone(),
                    spec: handle.spec.clone(),
                    active_conns: metrics.active_conns,
                    bytes_in: metrics.bytes_in,
                    bytes_out: metrics.bytes_out,
                    state: handle.state.clone(),
                }
            })
            .collect::<Vec<_>>();
        statuses.sort_by(|a, b| a.key.cmp(&b.key));
        statuses
    }

    async fn spawn_forward(
        &self,
        spec: ForwardSpec,
        key: String,
        runtime: ForwardRuntime,
    ) -> Option<JoinHandle<()>> {
        let manager = self.rtc_manager.clone()?;
        let state = self.forwards.clone();

        Some(tokio::spawn(async move {
            let result = match spec.proto {
                Proto::Tcp => {
                    tcp::TcpManager::listen_and_serve_with_target(
                        manager,
                        spec.listen_port,
                        spec.addr.clone(),
                        spec.target.clone(),
                        runtime,
                    )
                    .await
                }
                Proto::Udp => {
                    udp::UdpManager::listen_and_serve_with_target(
                        manager,
                        spec.listen_port,
                        spec.addr.clone(),
                        spec.target.clone(),
                        runtime,
                    )
                    .await
                }
            };

            if let Err(err) = result {
                error!("forward {} failed: {}", key, err);
                if let Some(handle) = state.write().await.get_mut(&key) {
                    handle.state = ForwardState::Error(err.to_string());
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests;
