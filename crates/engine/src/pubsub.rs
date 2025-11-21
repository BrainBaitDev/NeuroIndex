use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

/// Message published to a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub topic: String,
    pub payload: String,
    pub timestamp: u64,
}

/// Subscription handle
pub struct Subscription {
    id: u64,
    pattern: String,
    receiver: mpsc::Receiver<Message>,
}

impl Subscription {
    /// Try to receive a message (non-blocking)
    pub fn try_recv(&self) -> Result<Message, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Receive a message (blocking)
    pub fn recv(&self) -> Result<Message, mpsc::RecvError> {
        self.receiver.recv()
    }

    /// Get subscription ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get subscription pattern
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

struct Subscriber {
    sender: mpsc::Sender<Message>,
    pattern: String,
}

/// Pub/Sub manager for topic-based messaging
pub struct PubSubManager {
    subscribers: Arc<Mutex<HashMap<u64, Subscriber>>>,
    next_id: AtomicU64,
    message_count: AtomicU64,
}

impl PubSubManager {
    /// Create a new Pub/Sub manager
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            message_count: AtomicU64::new(0),
        }
    }

    /// Subscribe to a topic pattern
    ///
    /// Patterns support wildcards:
    /// - `*` matches any single segment (e.g., `user.*` matches `user.created`, `user.deleted`)
    /// - Exact match for no wildcards (e.g., `user.created`)
    pub fn subscribe(&self, pattern: String) -> Subscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel();

        let subscriber = Subscriber {
            sender,
            pattern: pattern.clone(),
        };

        self.subscribers.lock().unwrap().insert(id, subscriber);

        Subscription {
            id,
            pattern,
            receiver,
        }
    }

    /// Unsubscribe by subscription ID
    pub fn unsubscribe(&self, subscription_id: u64) -> bool {
        self.subscribers
            .lock()
            .unwrap()
            .remove(&subscription_id)
            .is_some()
    }

    /// Publish a message to a topic
    pub fn publish(&self, topic: String, payload: String) -> usize {
        let message = Message {
            topic: topic.clone(),
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        self.message_count.fetch_add(1, Ordering::Relaxed);

        let subscribers = self.subscribers.lock().unwrap();
        let mut delivered = 0;

        for subscriber in subscribers.values() {
            if self.pattern_matches(&subscriber.pattern, &topic) {
                // Ignore send errors (subscriber may have dropped receiver)
                if subscriber.sender.send(message.clone()).is_ok() {
                    delivered += 1;
                }
            }
        }

        delivered
    }

    /// Check if a pattern matches a topic
    fn pattern_matches(&self, pattern: &str, topic: &str) -> bool {
        // Exact match
        if pattern == topic {
            return true;
        }

        // Wildcard matching
        if pattern.contains('*') {
            let pattern_parts: Vec<&str> = pattern.split('.').collect();
            let topic_parts: Vec<&str> = topic.split('.').collect();

            if pattern_parts.len() != topic_parts.len() {
                return false;
            }

            for (p, t) in pattern_parts.iter().zip(topic_parts.iter()) {
                if *p != "*" && *p != *t {
                    return false;
                }
            }

            return true;
        }

        false
    }

    /// Get number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }

    /// Get total messages published
    pub fn message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
    }

    /// Get all active subscription patterns
    pub fn active_patterns(&self) -> Vec<String> {
        self.subscribers
            .lock()
            .unwrap()
            .values()
            .map(|s| s.pattern.clone())
            .collect()
    }
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_subscribe_and_publish() {
        let manager = PubSubManager::new();

        let sub = manager.subscribe("test.topic".to_string());

        manager.publish("test.topic".to_string(), "Hello".to_string());

        let msg = sub.recv().unwrap();
        assert_eq!(msg.topic, "test.topic");
        assert_eq!(msg.payload, "Hello");
    }

    #[test]
    fn test_wildcard_pattern() {
        let manager = PubSubManager::new();

        let sub = manager.subscribe("user.*".to_string());

        manager.publish("user.created".to_string(), "Alice".to_string());
        manager.publish("user.deleted".to_string(), "Bob".to_string());
        manager.publish("order.created".to_string(), "Order1".to_string());

        let msg1 = sub.try_recv().unwrap();
        assert_eq!(msg1.topic, "user.created");

        let msg2 = sub.try_recv().unwrap();
        assert_eq!(msg2.topic, "user.deleted");

        // Should not receive order.created
        assert!(sub.try_recv().is_err());
    }

    #[test]
    fn test_multiple_subscribers() {
        let manager = PubSubManager::new();

        let sub1 = manager.subscribe("events.*".to_string());
        let sub2 = manager.subscribe("events.important".to_string());

        let delivered = manager.publish("events.important".to_string(), "Alert!".to_string());

        assert_eq!(delivered, 2); // Both subscribers receive

        let msg1 = sub1.try_recv().unwrap();
        let msg2 = sub2.try_recv().unwrap();

        assert_eq!(msg1.payload, "Alert!");
        assert_eq!(msg2.payload, "Alert!");
    }

    #[test]
    fn test_unsubscribe() {
        let manager = PubSubManager::new();

        let sub = manager.subscribe("test".to_string());
        let sub_id = sub.id();

        assert_eq!(manager.subscriber_count(), 1);

        let removed = manager.unsubscribe(sub_id);
        assert!(removed);
        assert_eq!(manager.subscriber_count(), 0);

        // Publishing should deliver to 0 subscribers
        let delivered = manager.publish("test".to_string(), "msg".to_string());
        assert_eq!(delivered, 0);
    }

    #[test]
    fn test_pattern_matching() {
        let manager = PubSubManager::new();

        assert!(manager.pattern_matches("user.created", "user.created"));
        assert!(manager.pattern_matches("user.*", "user.created"));
        assert!(manager.pattern_matches("user.*", "user.deleted"));
        assert!(manager.pattern_matches("*.created", "user.created"));
        assert!(manager.pattern_matches("*.created", "order.created"));

        assert!(!manager.pattern_matches("user.*", "user"));
        assert!(!manager.pattern_matches("user.*", "user.created.extra"));
        assert!(!manager.pattern_matches("user.created", "user.deleted"));
    }

    #[test]
    fn test_message_count() {
        let manager = PubSubManager::new();

        let _sub = manager.subscribe("test".to_string());

        assert_eq!(manager.message_count(), 0);

        manager.publish("test".to_string(), "msg1".to_string());
        manager.publish("test".to_string(), "msg2".to_string());

        assert_eq!(manager.message_count(), 2);
    }

    #[test]
    fn test_no_subscribers() {
        let manager = PubSubManager::new();

        let delivered = manager.publish("test".to_string(), "msg".to_string());
        assert_eq!(delivered, 0);
    }

    #[test]
    fn test_dropped_receiver() {
        let manager = PubSubManager::new();

        {
            let _sub = manager.subscribe("test".to_string());
            // Subscription dropped here
        }

        // Should not panic, just return 0 delivered
        let delivered = manager.publish("test".to_string(), "msg".to_string());
        assert_eq!(delivered, 0);
    }

    #[test]
    fn test_active_patterns() {
        let manager = PubSubManager::new();

        let _sub1 = manager.subscribe("user.*".to_string());
        let _sub2 = manager.subscribe("order.created".to_string());

        let patterns = manager.active_patterns();
        assert_eq!(patterns.len(), 2);
        assert!(patterns.contains(&"user.*".to_string()));
        assert!(patterns.contains(&"order.created".to_string()));
    }

    #[test]
    fn test_concurrent_publish() {
        let manager = Arc::new(PubSubManager::new());

        let sub = manager.subscribe("test".to_string());

        let manager_clone = Arc::clone(&manager);
        let handle = thread::spawn(move || {
            for i in 0..10 {
                manager_clone.publish("test".to_string(), format!("msg{}", i));
                thread::sleep(Duration::from_millis(1));
            }
        });

        handle.join().unwrap();

        // Should receive all 10 messages
        let mut count = 0;
        while sub.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 10);
    }
}
