use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SignalMessage {
    #[serde(rename = "Type")]
    pub msg_type: String,

    #[serde(rename = "Data", skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    #[serde(rename = "SenderId")]
    pub sender_id: String,

    #[serde(rename = "ReceiverId", skip_serializing_if = "Option::is_none")]
    pub receiver_id: Option<String>,

    #[serde(rename = "RoomId", skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,

    #[serde(rename = "Role", skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    #[serde(rename = "MsgID", skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,

    #[serde(rename = "Hops", skip_serializing_if = "Option::is_none")]
    pub hops: Option<i32>,
}
