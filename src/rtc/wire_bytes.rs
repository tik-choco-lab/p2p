use base64::prelude::*;
use serde::{Deserialize, Deserializer, Serializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    Hex(String),
    Legacy(Vec<u8>),
}

fn decode_repr<E: serde::de::Error>(repr: BytesRepr) -> Result<Vec<u8>, E> {
    match repr {
        BytesRepr::Hex(encoded) => decode_string(&encoded).map_err(E::custom),
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

pub(crate) mod vec_hex {
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

pub(crate) mod option_hex {
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
