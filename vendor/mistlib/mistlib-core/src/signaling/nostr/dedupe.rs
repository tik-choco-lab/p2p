use std::collections::HashMap;
use web_time::{Duration, Instant};

const DEFAULT_MAX_ENTRIES: usize = 4096;

#[derive(Debug)]
pub struct DedupeCache {
    ttl: Duration,
    max_entries: usize,
    seen: HashMap<String, Instant>,
}

impl DedupeCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            max_entries: DEFAULT_MAX_ENTRIES,
            seen: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn with_max_entries(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries,
            seen: HashMap::new(),
        }
    }

    pub fn check_and_insert(&mut self, event_id: &str) -> bool {
        let now = Instant::now();
        self.sweep(now);
        if self.seen.contains_key(event_id) {
            return false;
        }
        self.evict_oldest_until_below_limit();
        self.seen.insert(event_id.to_string(), now);
        true
    }

    pub fn sweep(&mut self, now: Instant) {
        let ttl = self.ttl;
        self.seen
            .retain(|_, inserted_at| now.duration_since(*inserted_at) < ttl);
    }

    pub fn clear(&mut self) {
        self.seen.clear();
    }

    fn evict_oldest_until_below_limit(&mut self) {
        while self.seen.len() >= self.max_entries {
            let Some(oldest_id) = self
                .seen
                .iter()
                .min_by_key(|(_, inserted_at)| **inserted_at)
                .map(|(event_id, _)| event_id.clone())
            else {
                break;
            };
            self.seen.remove(&oldest_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_and_insert_rejects_duplicate_within_ttl() {
        let mut cache = DedupeCache::new(Duration::from_secs(60));

        assert!(cache.check_and_insert("event-1"));
        assert!(!cache.check_and_insert("event-1"));
    }

    #[test]
    fn sweep_removes_entries_older_than_ttl() {
        let mut cache = DedupeCache::new(Duration::from_secs(1));
        let now = Instant::now();

        cache
            .seen
            .insert("expired".to_string(), now - Duration::from_secs(2));
        cache
            .seen
            .insert("fresh".to_string(), now - Duration::from_millis(500));

        cache.sweep(now);

        assert!(!cache.seen.contains_key("expired"));
        assert!(cache.seen.contains_key("fresh"));
    }

    #[test]
    fn check_and_insert_caps_entries_by_evicting_oldest() {
        let mut cache = DedupeCache::with_max_entries(Duration::from_secs(60), 2);
        let now = Instant::now();

        cache
            .seen
            .insert("oldest".to_string(), now - Duration::from_secs(2));
        cache
            .seen
            .insert("newer".to_string(), now - Duration::from_secs(1));

        assert!(cache.check_and_insert("newest"));

        assert_eq!(cache.seen.len(), 2);
        assert!(!cache.seen.contains_key("oldest"));
        assert!(cache.seen.contains_key("newer"));
        assert!(cache.seen.contains_key("newest"));
    }
}
