use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// Type of secondary index
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    /// Hash index for exact match lookups (fastest)
    Hash,
    /// Range index for ordered queries (sorted)
    Range,
    /// Prefix index for text prefix searches
    Prefix { prefix_len: usize },
}

/// Index definition
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    pub name: String,
    pub index_type: IndexType,
    pub extractor: fn(&str) -> Option<String>,
}

/// Secondary index manager
pub struct SecondaryIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    /// Active indexes
    indexes: Arc<RwLock<HashMap<String, Index<K>>>>,
}

/// Internal index structure
struct Index<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    index_type: IndexType,
    /// For Hash and Prefix indexes: indexed_value -> Vec<K>
    hash_map: HashMap<String, Vec<K>>,
    /// For Range indexes: indexed_value -> Vec<K> (sorted by key)
    range_map: BTreeMap<String, Vec<K>>,
}

impl<K> SecondaryIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash + Ord,
{
    /// Create a new secondary index manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new index
    pub fn create_index(&self, name: String, index_type: IndexType) -> bool {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(&name) {
            return false; // Index already exists
        }

        let index = Index {
            index_type,
            hash_map: HashMap::new(),
            range_map: BTreeMap::new(),
        };

        indexes.insert(name, index);
        true
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> bool {
        let mut indexes = self.indexes.write().unwrap();
        indexes.remove(name).is_some()
    }

    /// Add a key to an index
    pub fn add_to_index(&self, index_name: &str, indexed_value: String, key: K) {
        let mut indexes = self.indexes.write().unwrap();

        if let Some(index) = indexes.get_mut(index_name) {
            match index.index_type {
                IndexType::Hash | IndexType::Prefix { .. } => {
                    index
                        .hash_map
                        .entry(indexed_value)
                        .or_insert_with(Vec::new)
                        .push(key);
                }
                IndexType::Range => {
                    index
                        .range_map
                        .entry(indexed_value)
                        .or_insert_with(Vec::new)
                        .push(key);
                }
            }
        }
    }

    /// Remove a key from an index
    pub fn remove_from_index(&self, index_name: &str, indexed_value: &str, key: &K) {
        let mut indexes = self.indexes.write().unwrap();

        if let Some(index) = indexes.get_mut(index_name) {
            match index.index_type {
                IndexType::Hash | IndexType::Prefix { .. } => {
                    if let Some(keys) = index.hash_map.get_mut(indexed_value) {
                        keys.retain(|k| k != key);
                        if keys.is_empty() {
                            index.hash_map.remove(indexed_value);
                        }
                    }
                }
                IndexType::Range => {
                    if let Some(keys) = index.range_map.get_mut(indexed_value) {
                        keys.retain(|k| k != key);
                        if keys.is_empty() {
                            index.range_map.remove(indexed_value);
                        }
                    }
                }
            }
        }
    }

    /// Query exact match in hash index
    pub fn query_exact(&self, index_name: &str, value: &str) -> Vec<K> {
        let indexes = self.indexes.read().unwrap();

        if let Some(index) = indexes.get(index_name) {
            if let Some(keys) = index.hash_map.get(value) {
                return keys.clone();
            }
        }

        Vec::new()
    }

    /// Query range in range index (between min and max, inclusive)
    pub fn query_range(&self, index_name: &str, min: &str, max: &str) -> Vec<K> {
        let indexes = self.indexes.read().unwrap();

        if let Some(index) = indexes.get(index_name) {
            let mut result = Vec::new();
            for (_, keys) in index.range_map.range(min.to_string()..=max.to_string()) {
                result.extend(keys.clone());
            }
            return result;
        }

        Vec::new()
    }

    /// Query prefix in prefix index
    pub fn query_prefix(&self, index_name: &str, prefix: &str) -> Vec<K> {
        let indexes = self.indexes.read().unwrap();

        if let Some(index) = indexes.get(index_name) {
            if let Some(keys) = index.hash_map.get(prefix) {
                return keys.clone();
            }
        }

        Vec::new()
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read().unwrap();
        indexes.keys().cloned().collect()
    }

    /// Get index type
    pub fn get_index_type(&self, name: &str) -> Option<IndexType> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name).map(|idx| idx.index_type.clone())
    }

    /// Get index stats
    pub fn index_stats(&self, name: &str) -> Option<IndexStats> {
        let indexes = self.indexes.read().unwrap();

        if let Some(index) = indexes.get(name) {
            let (entries, total_keys) = match &index.index_type {
                IndexType::Hash | IndexType::Prefix { .. } => (
                    index.hash_map.len(),
                    index.hash_map.values().map(|v| v.len()).sum(),
                ),
                IndexType::Range => (
                    index.range_map.len(),
                    index.range_map.values().map(|v| v.len()).sum(),
                ),
            };

            return Some(IndexStats {
                name: name.to_string(),
                index_type: index.index_type.clone(),
                entries,
                total_keys,
            });
        }

        None
    }
}

impl<K> Default for SecondaryIndexManager<K>
where
    K: Clone + Eq + std::hash::Hash + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub name: String,
    pub index_type: IndexType,
    pub entries: usize,
    pub total_keys: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_drop_index() {
        let manager = SecondaryIndexManager::<String>::new();

        assert!(manager.create_index("email_idx".to_string(), IndexType::Hash));
        assert!(!manager.create_index("email_idx".to_string(), IndexType::Hash)); // Already exists

        assert!(manager.drop_index("email_idx"));
        assert!(!manager.drop_index("email_idx")); // Doesn't exist
    }

    #[test]
    fn test_hash_index() {
        let manager = SecondaryIndexManager::<String>::new();
        manager.create_index("email_idx".to_string(), IndexType::Hash);

        manager.add_to_index(
            "email_idx",
            "alice@example.com".to_string(),
            "user:1".to_string(),
        );
        manager.add_to_index(
            "email_idx",
            "bob@example.com".to_string(),
            "user:2".to_string(),
        );
        manager.add_to_index(
            "email_idx",
            "alice@example.com".to_string(),
            "user:3".to_string(),
        );

        let keys = manager.query_exact("email_idx", "alice@example.com");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:3".to_string()));

        let keys = manager.query_exact("email_idx", "bob@example.com");
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_range_index() {
        let manager = SecondaryIndexManager::<String>::new();
        manager.create_index("age_idx".to_string(), IndexType::Range);

        manager.add_to_index("age_idx", "25".to_string(), "user:1".to_string());
        manager.add_to_index("age_idx", "30".to_string(), "user:2".to_string());
        manager.add_to_index("age_idx", "35".to_string(), "user:3".to_string());

        let keys = manager.query_range("age_idx", "25", "30");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_remove_from_index() {
        let manager = SecondaryIndexManager::<String>::new();
        manager.create_index("email_idx".to_string(), IndexType::Hash);

        manager.add_to_index(
            "email_idx",
            "alice@example.com".to_string(),
            "user:1".to_string(),
        );
        manager.remove_from_index("email_idx", "alice@example.com", &"user:1".to_string());

        let keys = manager.query_exact("email_idx", "alice@example.com");
        assert!(keys.is_empty());
    }

    #[test]
    fn test_list_indexes() {
        let manager = SecondaryIndexManager::<String>::new();

        manager.create_index("email_idx".to_string(), IndexType::Hash);
        manager.create_index("age_idx".to_string(), IndexType::Range);

        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 2);
        assert!(indexes.contains(&"email_idx".to_string()));
        assert!(indexes.contains(&"age_idx".to_string()));
    }

    #[test]
    fn test_index_stats() {
        let manager = SecondaryIndexManager::<String>::new();
        manager.create_index("email_idx".to_string(), IndexType::Hash);

        manager.add_to_index(
            "email_idx",
            "alice@example.com".to_string(),
            "user:1".to_string(),
        );
        manager.add_to_index(
            "email_idx",
            "bob@example.com".to_string(),
            "user:2".to_string(),
        );
        manager.add_to_index(
            "email_idx",
            "alice@example.com".to_string(),
            "user:3".to_string(),
        );

        let stats = manager.index_stats("email_idx").unwrap();
        assert_eq!(stats.entries, 2); // 2 unique emails
        assert_eq!(stats.total_keys, 3); // 3 total keys
    }

    #[test]
    fn test_prefix_index() {
        let manager = SecondaryIndexManager::<String>::new();
        manager.create_index(
            "name_prefix_idx".to_string(),
            IndexType::Prefix { prefix_len: 3 },
        );

        manager.add_to_index("name_prefix_idx", "ali".to_string(), "user:1".to_string());
        manager.add_to_index("name_prefix_idx", "bob".to_string(), "user:2".to_string());
        manager.add_to_index("name_prefix_idx", "ali".to_string(), "user:3".to_string());

        let keys = manager.query_prefix("name_prefix_idx", "ali");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:3".to_string()));
    }
}
