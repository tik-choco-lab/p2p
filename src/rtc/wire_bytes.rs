//! Byte-vector (de)serialization helpers for wire messages.
//!
//! Bytes are encoded as a `"b64:"`-prefixed base64 string. The legacy forms
//! below are only accepted on decode for cross-version compatibility with
//! older peers/messages; new output is always base64.
use base64::prelude::*;
use serde::{Deserialize, Deserializer, Serializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    /// A string payload: either the current `"b64:"`-prefixed base64 form,
    /// or a legacy plain-hex string (no prefix).
    Str(String),
    /// A legacy raw JSON byte array, from before string encoding was used.
    Legacy(Vec<u8>),
}

fn decode_repr<E: serde::de::Error>(repr: BytesRepr) -> Result<Vec<u8>, E> {
    match repr {
        BytesRepr::Str(encoded) => decode_string(&encoded).map_err(E::custom),
        BytesRepr::Legacy(bytes) => Ok(bytes),
    }
}

fn encode_bytes(bytes: &[u8]) -> String {
    format!("b64:{}", BASE64_STANDARD.encode(bytes))
}

fn decode_string(encoded: &str) -> Result<Vec<u8>, String> {
    if let Some(base64) = encoded.strip_prefix("b64:") {
        BASE64_STANDARD.decode(base64).map_err(|e| e.to_string())
    } else {
        hex::decode(encoded).map_err(|e| e.to_string())
    }
}

/// `#[serde(with = "...")]` helper for a required `Vec<u8>` field. The field
/// name in the serialized struct is unaffected by this module's name --
/// only the encoding of its value is controlled here.
pub(crate) mod vec_base64 {
    use super::*;

    pub(crate) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_bytes(bytes))
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = BytesRepr::deserialize(deserializer)?;
        decode_repr(repr)
    }
}

/// `#[serde(with = "...")]` helper for an optional `Vec<u8>` field.
pub(crate) mod option_base64 {
    use super::*;

    pub(crate) fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(bytes) => serializer.serialize_some(&encode_bytes(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = Option::<BytesRepr>::deserialize(deserializer)?;
        repr.map(decode_repr).transpose()
    }
}
