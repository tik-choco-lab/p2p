mod chat;
mod connect;
mod serve;
pub(crate) mod session;
mod shell;

use std::path::{Path, PathBuf};

use anyhow::Result;

pub(crate) use chat::run_chat;
pub(crate) use connect::run_connect;
pub(crate) use serve::run_serve;
#[allow(unused_imports)]
pub(crate) use shell::{run_control_shell, run_tui};

pub(crate) fn generate_room_id() -> String {
    let mut buf = [0u8; 4];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    hex::encode(buf)
}

pub(crate) async fn load_or_create_node_id() -> Result<String> {
    let path = default_node_id_path();
    match tokio::fs::read_to_string(&path).await {
        Ok(text) => {
            let id = text.trim();
            if !id.is_empty() {
                return Ok(id.to_string());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }

    let id = uuid::Uuid::new_v4().to_string();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&path, format!("{id}\n")).await?;
    Ok(id)
}

fn default_node_id_path() -> PathBuf {
    config_dir().join("node_id")
}

fn config_dir() -> PathBuf {
    let base = std::env::var_os("P2P_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("APPDATA").map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("p2p")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("p2p-app-{name}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn node_id_is_persisted_and_reused() {
        let dir = temp_config_dir("node-id");
        temp_env::with_var("P2P_CONFIG_DIR", Some(dir.as_os_str()), || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let first = load_or_create_node_id().await.unwrap();
                let second = load_or_create_node_id().await.unwrap();

                assert_eq!(first, second);
                assert_eq!(
                    tokio::fs::read_to_string(default_node_id_path())
                        .await
                        .unwrap()
                        .trim(),
                    first
                );
            });
        });
    }

    #[test]
    fn empty_node_id_file_is_replaced() {
        let dir = temp_config_dir("empty-node-id");
        temp_env::with_var("P2P_CONFIG_DIR", Some(dir.as_os_str()), || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let path = default_node_id_path();
                tokio::fs::create_dir_all(path.parent().unwrap())
                    .await
                    .unwrap();
                tokio::fs::write(&path, "\n").await.unwrap();

                let id = load_or_create_node_id().await.unwrap();

                assert!(!id.is_empty());
                assert_eq!(tokio::fs::read_to_string(path).await.unwrap().trim(), id);
            });
        });
    }
}
