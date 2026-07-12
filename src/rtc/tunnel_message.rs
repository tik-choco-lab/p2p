use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunnelMessage {
    #[serde(rename = "type")]
    pub msg_type: String,

    #[serde(rename = "conn_id")]
    pub conn_id: String,

    #[serde(rename = "target", default, skip_serializing_if = "String::is_empty")]
    pub target: String,

    #[serde(
        rename = "payload",
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::rtc::wire_bytes::option_base64"
    )]
    pub payload: Option<Vec<u8>>,

    /// Per-conn, monotonically increasing (starting at 1) sequence number for
    /// `data` messages, assigned by the sender. Lets the receiver detect the
    /// gaps and duplicates that mistlib's `ReorderBuffer` (drops messages
    /// delayed >1s) and the tunnel's send-retry path (may redeliver an
    /// already-delivered payload) can otherwise introduce silently. `None`
    /// for non-`data` messages and for messages from peers running an older
    /// version that predates this field -- omitted from the wire when unset
    /// so older peers (which ignore unknown fields) keep working.
    #[serde(rename = "seq", default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}
