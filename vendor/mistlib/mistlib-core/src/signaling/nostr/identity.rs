use super::signature::{generate_secret_key_bytes, xonly_public_key_hex};
use super::util::{hex_encode, now_unix_seconds};
use crate::error::{MistError, Result};
use crate::types::NodeId;
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub struct SignalingSecretKey([u8; 32]);

impl SignalingSecretKey {
    pub fn generate() -> Self {
        Self(generate_secret_key_bytes())
    }

    pub fn from_bytes_for_tests(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SignalingSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignalingSecretKey(<redacted>)")
    }
}

#[derive(Clone)]
pub struct TemporarySignalingIdentity {
    pub public_key: String,
    secret_key: SignalingSecretKey,
}

impl TemporarySignalingIdentity {
    pub fn generate() -> Self {
        Self::from_secret_key(SignalingSecretKey::generate())
    }

    pub fn from_secret_key(secret_key: SignalingSecretKey) -> Self {
        let public_key = xonly_public_key_hex(secret_key.as_bytes())
            .expect("SignalingSecretKey must be a valid secp256k1 secret key");
        Self {
            public_key,
            secret_key,
        }
    }

    pub fn secret_key(&self) -> &SignalingSecretKey {
        &self.secret_key
    }

    pub fn short_public_key(&self) -> String {
        hex_encode(&self.public_key.as_bytes()[..8.min(self.public_key.len())])
    }
}

impl fmt::Debug for TemporarySignalingIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporarySignalingIdentity")
            .field("public_key", &self.public_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryEntry {
    pub signaling_pubkey: String,
    pub expires_at: u64,
    pub topology_rank: String,
}

#[derive(Default, Debug)]
pub struct DiscoveryTable {
    by_pubkey: HashMap<String, DiscoveryEntry>,
    node_to_pubkey: HashMap<NodeId, String>,
}

impl DiscoveryTable {
    pub fn insert_pubkey(&mut self, signaling_pubkey: String, expires_at: u64) {
        let topology_rank = signaling_pubkey.clone();
        self.insert_pubkey_with_rank(signaling_pubkey, expires_at, topology_rank);
    }

    pub fn insert_pubkey_with_rank(
        &mut self,
        signaling_pubkey: String,
        expires_at: u64,
        topology_rank: String,
    ) {
        self.sweep_expired(now_unix_seconds());
        let expires_at = self
            .by_pubkey
            .get(&signaling_pubkey)
            .map_or(expires_at, |entry| entry.expires_at.max(expires_at));
        self.by_pubkey.insert(
            signaling_pubkey.clone(),
            DiscoveryEntry {
                signaling_pubkey,
                expires_at,
                topology_rank,
            },
        );
    }

    pub fn bind_node(&mut self, node_id: NodeId, signaling_pubkey: String, expires_at: u64) {
        let _ = self.bind_node_checked(node_id, signaling_pubkey, expires_at);
    }

    pub fn bind_node_checked(
        &mut self,
        node_id: NodeId,
        signaling_pubkey: String,
        expires_at: u64,
    ) -> Result<bool> {
        let topology_rank = signaling_pubkey.clone();
        self.bind_node_checked_with_rank(node_id, signaling_pubkey, expires_at, topology_rank)
    }

    pub fn bind_node_checked_with_rank(
        &mut self,
        node_id: NodeId,
        signaling_pubkey: String,
        expires_at: u64,
        topology_rank: String,
    ) -> Result<bool> {
        self.bind_node_checked_with_rank_and_rebind(
            node_id,
            signaling_pubkey,
            expires_at,
            topology_rank,
            false,
        )
    }

    pub fn bind_node_checked_with_rank_and_rebind(
        &mut self,
        node_id: NodeId,
        signaling_pubkey: String,
        expires_at: u64,
        topology_rank: String,
        allow_rebind: bool,
    ) -> Result<bool> {
        self.sweep_expired(now_unix_seconds());
        let known = match self.node_to_pubkey.get(&node_id) {
            Some(existing) if existing == &signaling_pubkey => true,
            Some(existing) if allow_rebind => {
                self.by_pubkey.remove(existing);
                false
            }
            Some(_) => {
                return Err(MistError::Signaling(
                    "Nostr sender node id changed pubkey".to_string(),
                ))
            }
            None => false,
        };
        self.insert_pubkey_with_rank(signaling_pubkey.clone(), expires_at, topology_rank);
        self.node_to_pubkey.insert(node_id, signaling_pubkey);
        Ok(known)
    }

    pub fn pubkey_for_node(&mut self, node_id: &NodeId) -> Option<String> {
        self.sweep_expired(now_unix_seconds());
        self.node_to_pubkey.get(node_id).cloned()
    }

    pub fn active_pubkeys(&mut self) -> Vec<String> {
        self.sweep_expired(now_unix_seconds());
        self.by_pubkey.keys().cloned().collect()
    }

    pub fn responder_pubkeys_for(
        &mut self,
        subject_pubkey: &str,
        subject_rank: &str,
        local_pubkey: &str,
        local_rank: &str,
        limit: usize,
    ) -> Vec<String> {
        self.sweep_expired(now_unix_seconds());
        if limit == 0 {
            return Vec::new();
        }

        let mut ranked: Vec<(String, String)> = self
            .by_pubkey
            .values()
            .map(|entry| (entry.topology_rank.clone(), entry.signaling_pubkey.clone()))
            .collect();

        if !ranked.iter().any(|(_, pubkey)| pubkey == local_pubkey) {
            ranked.push((local_rank.to_string(), local_pubkey.to_string()));
        }
        if !ranked.iter().any(|(_, pubkey)| pubkey == subject_pubkey) {
            ranked.push((subject_rank.to_string(), subject_pubkey.to_string()));
        }

        ranked.sort();
        ranked.dedup_by(|left, right| left.1 == right.1);
        if ranked.len() <= 1 {
            return Vec::new();
        }

        let Some(subject_index) = ranked
            .iter()
            .position(|(_, pubkey)| pubkey == subject_pubkey)
        else {
            return Vec::new();
        };

        let mut responders = Vec::with_capacity(limit);
        for offset in 1..ranked.len() {
            let index = (subject_index + ranked.len() - offset) % ranked.len();
            let pubkey = &ranked[index].1;
            if pubkey == subject_pubkey {
                continue;
            }
            responders.push(pubkey.clone());
            if responders.len() == limit {
                break;
            }
        }
        responders
    }

    pub fn expires_at_for_pubkey(&self, signaling_pubkey: &str) -> Option<u64> {
        self.by_pubkey
            .get(signaling_pubkey)
            .map(|entry| entry.expires_at)
    }

    pub fn sweep_expired(&mut self, now: u64) {
        self.by_pubkey.retain(|_, entry| entry.expires_at > now);
        self.node_to_pubkey
            .retain(|_, pubkey| self.by_pubkey.contains_key(pubkey));
    }

    pub fn clear(&mut self) {
        self.by_pubkey.clear();
        self.node_to_pubkey.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::DiscoveryTable;

    #[test]
    fn responder_pubkeys_use_predecessors_on_rank_ring() {
        let mut table = DiscoveryTable::default();
        table.insert_pubkey_with_rank("a".to_string(), u64::MAX, "01".to_string());
        table.insert_pubkey_with_rank("b".to_string(), u64::MAX, "02".to_string());
        table.insert_pubkey_with_rank("c".to_string(), u64::MAX, "03".to_string());

        let responders = table.responder_pubkeys_for("d", "04", "c", "03", 2);
        assert_eq!(responders, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn responder_pubkeys_wrap_for_lowest_rank_subject() {
        let mut table = DiscoveryTable::default();
        table.insert_pubkey_with_rank("a".to_string(), u64::MAX, "01".to_string());
        table.insert_pubkey_with_rank("b".to_string(), u64::MAX, "02".to_string());
        table.insert_pubkey_with_rank("c".to_string(), u64::MAX, "03".to_string());

        let responders = table.responder_pubkeys_for("z", "00", "c", "03", 2);
        assert_eq!(responders, vec!["c".to_string(), "b".to_string()]);
    }
}
