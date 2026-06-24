use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use audit::{AuthAuditLog, AuthEvent, AuthEventSource};
#[allow(unused_imports)]
pub use pending::PendingAuthorization;
pub use pending::{PendingAuthorizations, PendingAuthorizer};
pub use trust::{default_trust_store_path, TrustDecision, TrustEntry, TrustKey, TrustStore};

mod audit;
mod pending;
mod trust;

pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = AuthDecision> + Send + 'a>>;
pub type SharedAuthorizer = Arc<dyn ConnectionAuthorizer>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRequest {
    pub peer_id: String,
    pub forward_key: String,
    pub target_addr: String,
    pub proto: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny,
    #[allow(dead_code)]
    AllowAlways,
    #[allow(dead_code)]
    DenyAlways,
}

impl AuthDecision {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allow | Self::AllowAlways)
    }

    fn trust_decision(self) -> Option<TrustDecision> {
        match self {
            Self::AllowAlways => Some(TrustDecision::Allow),
            Self::DenyAlways => Some(TrustDecision::Deny),
            Self::Allow | Self::Deny => None,
        }
    }
}

pub trait ConnectionAuthorizer: Send + Sync {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a>;
}

pub fn allow_all() -> SharedAuthorizer {
    Arc::new(AllowAllAuthorizer)
}

#[derive(Debug, Default)]
struct AllowAllAuthorizer;

impl ConnectionAuthorizer for AllowAllAuthorizer {
    fn authorize<'a>(&'a self, _req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async { AuthDecision::Allow })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthPolicy {
    AutoAccept,
    AllowPeers(HashSet<String>),
    DenyUnknown,
}

impl AuthPolicy {
    pub fn allow_peers<I, S>(peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::AllowPeers(peers.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone)]
pub struct PolicyAuthorizer {
    policy: AuthPolicy,
    store: TrustStore,
    audit_log: Option<AuthAuditLog>,
}

impl PolicyAuthorizer {
    pub fn new(policy: AuthPolicy, store: TrustStore) -> Self {
        Self {
            policy,
            store,
            audit_log: None,
        }
    }

    pub fn shared(policy: AuthPolicy, store: TrustStore) -> SharedAuthorizer {
        Arc::new(Self::new(policy, store))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_audit_log(policy: AuthPolicy, store: TrustStore, audit_log: AuthAuditLog) -> Self {
        Self {
            policy,
            store,
            audit_log: Some(audit_log),
        }
    }

    #[allow(dead_code)]
    pub fn shared_with_audit_log(
        policy: AuthPolicy,
        store: TrustStore,
        audit_log: AuthAuditLog,
    ) -> SharedAuthorizer {
        Arc::new(Self::with_audit_log(policy, store, audit_log))
    }

    async fn decide(&self, req: &AuthRequest) -> AuthDecision {
        let key = TrustKey {
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
        };
        if let Some(decision) = self.store.get(&key).await {
            let auth_decision = match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
            self.record(req, auth_decision, AuthEventSource::TrustStore)
                .await;
            return auth_decision;
        }

        let decision = match &self.policy {
            AuthPolicy::AutoAccept => AuthDecision::Allow,
            AuthPolicy::AllowPeers(peers) if peers.contains(&req.peer_id) => AuthDecision::Allow,
            AuthPolicy::AllowPeers(_) | AuthPolicy::DenyUnknown => AuthDecision::Deny,
        };
        if let Some(trust) = decision.trust_decision() {
            let _ = self.store.remember(key, trust).await;
        }
        self.record(req, decision, AuthEventSource::Policy).await;
        decision
    }

    async fn record(&self, req: &AuthRequest, decision: AuthDecision, source: AuthEventSource) {
        if let Some(log) = &self.audit_log {
            log.record(req, decision, source).await;
        }
    }
}

impl ConnectionAuthorizer for PolicyAuthorizer {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async move { self.decide(req).await })
    }
}

#[cfg(test)]
mod tests;
