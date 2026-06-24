#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum P2pPayload {
    Role {
        role: String,
    },
    Capabilities {
        forwards: Vec<String>,
    },
    Chat {
        text: String,
    },
    Tunnel {
        data: Vec<u8>,
    },
    Stdio {
        data: Vec<u8>,
    },
    ForwardRequest {
        req_id: String,
        proto: String,
        remote_addr: String,
        target: String,
    },
    ForwardResponse {
        req_id: String,
        target: String,
        accepted: bool,
    },
}
