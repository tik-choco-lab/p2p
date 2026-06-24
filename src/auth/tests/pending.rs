use super::{request, temp_store_path};
use crate::auth::*;

use tokio::time::{sleep, timeout, Duration};

#[tokio::test]
async fn pending_authorizer_queues_unknown_until_resolved() {
    let store = TrustStore::load(temp_store_path("pending-queue"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });
    let items = wait_for_pending(&pending).await;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].request.peer_id, "peer-a");
    assert!(pending.resolve(items[0].id, AuthDecision::Allow).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Allow);
}

#[tokio::test]
async fn pending_authorizer_remembers_allow_always() {
    let path = temp_store_path("pending-remember");
    let store = TrustStore::load(&path).await.unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store.clone(), pending.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });
    let id = wait_for_pending(&pending).await[0].id;
    assert!(pending.resolve(id, AuthDecision::AllowAlways).await);

    assert_eq!(task.await.unwrap(), AuthDecision::AllowAlways);
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            })
            .await,
        Some(TrustDecision::Allow)
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn pending_authorizer_uses_trust_store_without_queuing() {
    let path = temp_store_path("pending-trust");
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            },
            TrustDecision::Deny,
        )
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;

    assert_eq!(decision, AuthDecision::Deny);
    assert!(pending.list().await.is_empty());
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn pending_authorizer_records_pending_decision_events() {
    let store = TrustStore::load(temp_store_path("pending-audit"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let audit_log = AuthAuditLog::default();
    let authorizer = PendingAuthorizer::with_audit_log(store, pending.clone(), audit_log.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });
    let id = wait_for_pending(&pending).await[0].id;
    assert!(pending.resolve(id, AuthDecision::Deny).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Deny);

    let events = audit_log.list().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].decision, AuthDecision::Deny);
    assert_eq!(events[0].source, AuthEventSource::Pending);
}

async fn wait_for_pending(pending: &PendingAuthorizations) -> Vec<PendingAuthorization> {
    timeout(Duration::from_secs(1), async {
        loop {
            let items = pending.list().await;
            if !items.is_empty() {
                return items;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending auth request should be queued")
}
