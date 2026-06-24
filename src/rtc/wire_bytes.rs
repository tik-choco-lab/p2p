use serde::{Deserialize, Deserializer, Serializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    Hex(String),
    Legacy(Vec<u8>),
}

fn decode_repr<E: serde::de::Error>(repr: BytesRepr) -> Result<Vec<u8>, E> {
    match repr {
        BytesRepr::Hex(hex) => hex::decode(hex).map_err(E::custom),
        BytesRepr::Legacy(bytes) => Ok(bytes),
    }
}

pub(crate) mod vec_hex {
    use super::*;

    pub(crate) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
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
            Some(bytes) => serializer.serialize_some(&hex::encode(bytes)),
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
