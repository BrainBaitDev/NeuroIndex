// Advanced query operations for Phase 7
// Based on DeWitt et al. "Implementation Techniques for Main Memory Database Systems"

use std::collections::HashMap;
use std::hash::Hash;

/// Hash Join Builder - implements Grace/Hybrid hash join algorithm
/// Reference: DeWitt §4.2 - "Hash join is superior to sort-merge in memory-resident environments"
pub struct HashJoinBuilder<K, L, R> {
    build_side: HashMap<K, Vec<L>>,
    _phantom: std::marker::PhantomData<R>,
}

impl<K: Eq + Hash + Clone, L: Clone, R> HashJoinBuilder<K, L, R> {
    /// Create a new hash join builder
    /// Build phase: construct hash table from smaller relation
    pub fn new() -> Self {
        Self {
            build_side: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Build phase: insert records from the "build" side (typically smaller table)
    pub fn build<F>(&mut self, records: Vec<(K, L)>, _key_extractor: F)
    where
        F: Fn(&L) -> K,
    {
        for (key, left) in records {
            self.build_side
                .entry(key)
                .or_insert_with(Vec::new)
                .push(left);
        }
    }

    /// Probe phase: stream through "probe" side and emit join results
    pub fn probe<F>(&self, probe_records: Vec<(K, R)>, mut output: F)
    where
        F: FnMut(&K, &L, &R),
    {
        for (key, right) in &probe_records {
            if let Some(left_matches) = self.build_side.get(key) {
                for left in left_matches {
                    output(key, left, right);
                }
            }
        }
    }

    /// Execute full hash join and collect results
    pub fn execute(&self, probe_records: Vec<(K, R)>) -> Vec<(K, L, R)>
    where
        R: Clone,
    {
        let mut results = Vec::new();
        self.probe(probe_records, |k, l, r| {
            results.push((k.clone(), l.clone(), r.clone()));
        });
        results
    }
}

/// Aggregation operations for GROUP BY queries
pub struct Aggregator<K, V> {
    groups: HashMap<K, Vec<V>>,
}

impl<K: Eq + Hash + Clone, V> Aggregator<K, V> {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Group records by key
    pub fn group_by<F>(&mut self, records: Vec<V>, key_fn: F)
    where
        F: Fn(&V) -> K,
    {
        for record in records {
            let key = key_fn(&record);
            self.groups.entry(key).or_insert_with(Vec::new).push(record);
        }
    }

    /// Compute aggregate for each group
    pub fn aggregate<T, F>(&self, mut agg_fn: F) -> HashMap<K, T>
    where
        F: FnMut(&K, &[V]) -> T,
    {
        self.groups
            .iter()
            .map(|(k, vals)| (k.clone(), agg_fn(k, vals)))
            .collect()
    }

    /// Count aggregation
    pub fn count(&self) -> HashMap<K, usize> {
        self.groups
            .iter()
            .map(|(k, vals)| (k.clone(), vals.len()))
            .collect()
    }

    /// Sum aggregation for numeric types
    pub fn sum<T, F>(&self, mut value_fn: F) -> HashMap<K, T>
    where
        T: Default + std::ops::AddAssign + Clone,
        F: FnMut(&V) -> T,
    {
        self.groups
            .iter()
            .map(|(k, vals)| {
                let mut sum = T::default();
                for v in vals {
                    sum += value_fn(v);
                }
                (k.clone(), sum)
            })
            .collect()
    }

    /// Average aggregation
    pub fn avg<F>(&self, value_fn: F) -> HashMap<K, f64>
    where
        F: Fn(&V) -> f64,
    {
        self.groups
            .iter()
            .map(|(k, vals)| {
                let sum: f64 = vals.iter().map(|v| value_fn(v)).sum();
                let avg = if vals.is_empty() {
                    0.0
                } else {
                    sum / vals.len() as f64
                };
                (k.clone(), avg)
            })
            .collect()
    }

    /// Min aggregation
    pub fn min<T, F>(&self, value_fn: F) -> HashMap<K, Option<T>>
    where
        T: Ord + Clone,
        F: Fn(&V) -> T,
    {
        self.groups
            .iter()
            .map(|(k, vals)| {
                let min = vals.iter().map(|v| value_fn(v)).min();
                (k.clone(), min)
            })
            .collect()
    }

    /// Max aggregation
    pub fn max<T, F>(&self, value_fn: F) -> HashMap<K, Option<T>>
    where
        T: Ord + Clone,
        F: Fn(&V) -> T,
    {
        self.groups
            .iter()
            .map(|(k, vals)| {
                let max = vals.iter().map(|v| value_fn(v)).max();
                (k.clone(), max)
            })
            .collect()
    }

    /// Get all groups
    pub fn into_groups(self) -> HashMap<K, Vec<V>> {
        self.groups
    }
}

/// Query planner helpers for optimization
pub struct QueryPlanner;

impl QueryPlanner {
    /// Estimate selectivity of a predicate (for cost-based optimization)
    pub fn estimate_selectivity<T, F>(sample: &[T], predicate: F) -> f64
    where
        F: Fn(&T) -> bool,
    {
        if sample.is_empty() {
            return 0.0;
        }
        let matching = sample.iter().filter(|x| predicate(x)).count();
        matching as f64 / sample.len() as f64
    }

    /// Decide join order based on cardinality
    pub fn optimize_join_order(left_card: usize, right_card: usize) -> (bool, String) {
        // Smaller relation should be build side
        if left_card <= right_card {
            (
                true,
                format!(
                    "Build on left ({} rows), probe on right ({} rows)",
                    left_card, right_card
                ),
            )
        } else {
            (
                false,
                format!(
                    "Build on right ({} rows), probe on left ({} rows)",
                    right_card, left_card
                ),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_join() {
        let left = vec![(1, "Alice"), (2, "Bob"), (3, "Charlie")];
        let right = vec![(1, 100), (2, 200), (4, 400)];

        let mut join = HashJoinBuilder::new();
        join.build(left.clone(), |_| 0);

        let results = join.execute(right);

        assert_eq!(results.len(), 2);
        assert!(results.contains(&(1, "Alice", 100)));
        assert!(results.contains(&(2, "Bob", 200)));
    }

    #[test]
    fn test_aggregator_count() {
        let records = vec![("A", 10), ("B", 20), ("A", 30), ("B", 40), ("A", 50)];

        let mut agg = Aggregator::new();
        agg.group_by(records, |&(key, _val)| key);

        let counts = agg.count();
        assert_eq!(counts.get(&"A"), Some(&3));
        assert_eq!(counts.get(&"B"), Some(&2));
    }

    #[test]
    fn test_aggregator_sum() {
        let records = vec![("A", 10), ("B", 20), ("A", 30), ("B", 40)];

        let mut agg = Aggregator::new();
        agg.group_by(records.clone(), |&(key, _val)| key);

        let sums = agg.sum(|&(_key, val)| val);
        assert_eq!(sums.get(&"A"), Some(&40));
        assert_eq!(sums.get(&"B"), Some(&60));
    }

    #[test]
    fn test_aggregator_avg() {
        let records = vec![("A", 10), ("A", 20), ("A", 30)];

        let mut agg = Aggregator::new();
        agg.group_by(records, |&(key, _val)| key);

        let avgs = agg.avg(|&(_key, val)| val as f64);
        assert_eq!(avgs.get(&"A"), Some(&20.0));
    }

    #[test]
    fn test_selectivity_estimation() {
        let sample = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let selectivity = QueryPlanner::estimate_selectivity(&sample, |&x| x % 2 == 0);
        assert_eq!(selectivity, 0.5);
    }

    #[test]
    fn test_join_order_optimization() {
        let (build_left, msg) = QueryPlanner::optimize_join_order(100, 1000);
        assert!(build_left);
        assert!(msg.contains("Build on left"));

        let (build_left, msg) = QueryPlanner::optimize_join_order(1000, 100);
        assert!(!build_left);
        assert!(msg.contains("Build on right"));
    }
}
