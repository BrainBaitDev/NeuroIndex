/// Change event streaming for real-time subscriptions
///
/// This module provides a trait-based system for external components (e.g., WebSocket servers)
/// to receive notifications when the database changes.
///
/// ## Design
///
/// - **Trait-based**: `ChangeSubscriber` allows any component to register for notifications
/// - **Feature-gated**: Only compiled with `change-events` feature to avoid overhead
/// - **Post-commit**: Notifications are sent after successful operations
/// - **Type-erased**: Uses `String` for keys/values to avoid generic complexity in subscribers
///
/// ## Example
///
/// ```rust,ignore
/// use engine::change_events::ChangeSubscriber;
/// use std::sync::Arc;
///
/// struct MySubscriber;
///
/// impl ChangeSubscriber for MySubscriber {
///     fn on_put(&self, key: &str, value: &str) {
///         println!("PUT: {} = {}", key, value);
///     }
///
///     fn on_delete(&self, key: &str) {
///         println!("DELETE: {}", key);
///     }
/// }
///
/// let mut engine = Engine::with_shards(4, 16);
/// engine.register_subscriber(Arc::new(MySubscriber));
/// ```

use std::fmt::Debug;

/// Trait for components that want to receive database change notifications
///
/// Implementors will receive callbacks for:
/// - `on_put`: Key-value insertions/updates
/// - `on_delete`: Key deletions
/// - `on_tag`: Tag assignments (optional)
/// - `on_untag`: Tag removals (optional)
/// - `on_expire`: TTL expirations (optional)
///
/// **Thread Safety**: All methods must be `Send + Sync` as they're called from
/// multiple engine shards concurrently.
pub trait ChangeSubscriber: Send + Sync + Debug {
    /// Called after a successful PUT operation
    ///
    /// # Arguments
    /// - `key`: The key that was inserted/updated (as String)
    /// - `value`: The new value (as String, may be JSON-encoded)
    fn on_put(&self, key: &str, value: &str);

    /// Called after a successful DELETE operation
    ///
    /// # Arguments
    /// - `key`: The key that was deleted
    fn on_delete(&self, key: &str);

    /// Called when a tag is added to a key (optional hook)
    ///
    /// Default implementation does nothing. Override if your subscriber
    /// needs to track tag changes.
    fn on_tag(&self, _key: &str, _tag: u64) {}

    /// Called when a tag is removed from a key (optional hook)
    fn on_untag(&self, _key: &str, _tag: u64) {}

    /// Called when a key expires due to TTL (optional hook)
    ///
    /// Note: This is only called during active expiration (cleanup worker),
    /// not during lazy expiration on GET.
    fn on_expire(&self, _key: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct TestSubscriber {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl TestSubscriber {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl ChangeSubscriber for TestSubscriber {
        fn on_put(&self, key: &str, value: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("PUT:{}:{}", key, value));
        }

        fn on_delete(&self, key: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("DELETE:{}", key));
        }

        fn on_tag(&self, key: &str, tag: u64) {
            self.events
                .lock()
                .unwrap()
                .push(format!("TAG:{}:{}", key, tag));
        }
    }

    #[test]
    fn test_subscriber_trait() {
        let sub = TestSubscriber::new();

        sub.on_put("key1", "value1");
        sub.on_delete("key2");
        sub.on_tag("key3", 42);

        let events = sub.get_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], "PUT:key1:value1");
        assert_eq!(events[1], "DELETE:key2");
        assert_eq!(events[2], "TAG:key3:42");
    }
}
