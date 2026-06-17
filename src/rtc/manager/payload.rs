#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum P2pPayload {
    Role { role: String },
    Chat { text: String },
    Tunnel { data: Vec<u8> },
    Stdio { data: Vec<u8> },
}
