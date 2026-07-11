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
}
