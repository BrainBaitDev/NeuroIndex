use parking_lot::RwLock;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

mod prefetch;
pub use prefetch::{prefetch_children, prefetch_node};

const MAX_KEYS: usize = 8;

#[derive(Clone)]
struct KV<K, V> {
    key: K,
    val: V,
}

struct Node<K, V> {
    height: i32,
    items: Vec<KV<K, V>>,
    left: Option<Box<Node<K, V>>>,
    right: Option<Box<Node<K, V>>>,
}

impl<K: Ord, V> Node<K, V> {
    fn new(key: K, val: V) -> Self {
        Self {
            height: 1,
            items: vec![KV { key, val }],
            left: None,
            right: None,
        }
    }

    fn height(node: &Option<Box<Self>>) -> i32 {
        node.as_ref().map_or(0, |n| n.height)
    }

    fn update_height(node: &mut Box<Self>) {
        let lh = Self::height(&node.left);
        let rh = Self::height(&node.right);
        node.height = 1 + lh.max(rh);
    }

    fn balance_factor(node: &Self) -> i32 {
        Self::height(&node.left) - Self::height(&node.right)
    }

    fn rotate_left(root: &mut Option<Box<Self>>) {
        let mut node = root.take().expect("rotate_left on empty node");
        let mut new_root = node
            .right
            .take()
            .expect("rotate_left requires right child to exist");
        node.right = new_root.left.take();
        Self::update_height(&mut node);
        new_root.left = Some(node);
        Self::update_height(&mut new_root);
        *root = Some(new_root);
    }

    fn rotate_right(root: &mut Option<Box<Self>>) {
        let mut node = root.take().expect("rotate_right on empty node");
        let mut new_root = node
            .left
            .take()
            .expect("rotate_right requires left child to exist");
        node.left = new_root.right.take();
        Self::update_height(&mut node);
        new_root.right = Some(node);
        Self::update_height(&mut new_root);
        *root = Some(new_root);
    }

    fn rebalance(node: &mut Option<Box<Self>>) {
        if node.is_none() {
            return;
        }
        if let Some(ref mut n) = node {
            Self::update_height(n);
        }
        let balance = {
            let n = node.as_ref().unwrap();
            Self::balance_factor(n)
        };
        if balance > 1 {
            let left_balance = node
                .as_ref()
                .and_then(|n| {
                    n.left
                        .as_ref()
                        .map(|left| Self::balance_factor(left.as_ref()))
                })
                .unwrap_or(0);
            if left_balance < 0 {
                if let Some(ref mut n) = node {
                    Self::rotate_left(&mut n.left);
                }
            }
            Self::rotate_right(node);
        } else if balance < -1 {
            let right_balance = node
                .as_ref()
                .and_then(|n| {
                    n.right
                        .as_ref()
                        .map(|right| Self::balance_factor(right.as_ref()))
                })
                .unwrap_or(0);
            if right_balance > 0 {
                if let Some(ref mut n) = node {
                    Self::rotate_right(&mut n.right);
                }
            }
            Self::rotate_left(node);
        }
        if let Some(ref mut n) = node {
            Self::update_height(n);
        }
    }

    fn insert(node: &mut Option<Box<Self>>, key: K, val: V) -> Option<V> {
        if node.is_none() {
            *node = Some(Box::new(Self::new(key, val)));
            return None;
        }

        let mut replaced = None;
        {
            let n = node.as_mut().unwrap();
            match n.items.binary_search_by(|kv| kv.key.cmp(&key)) {
                Ok(idx) => {
                    replaced = Some(std::mem::replace(&mut n.items[idx].val, val));
                }
                Err(pos) => {
                    if n.items.len() < MAX_KEYS {
                        n.items.insert(pos, KV { key, val });
                    } else if pos == 0 {
                        replaced = Self::insert(&mut n.left, key, val);
                    } else if pos == n.items.len() {
                        replaced = Self::insert(&mut n.right, key, val);
                    } else {
                        let len = n.items.len();
                        if pos <= len / 2 {
                            let displaced = n.items.remove(0);
                            let insert_idx = pos - 1;
                            let left = &mut n.left;
                            Self::insert(left, displaced.key, displaced.val);
                            n.items.insert(insert_idx, KV { key, val });
                        } else {
                            let displaced = n.items.pop().expect("node items empty");
                            let right = &mut n.right;
                            Self::insert(right, displaced.key, displaced.val);
                            n.items.insert(pos, KV { key, val });
                        }
                    }
                }
            }
        }
        Self::rebalance(node);
        replaced
    }

    fn get<'a>(node: &'a Option<Box<Self>>, key: &K) -> Option<&'a V> {
        let n = node.as_ref()?;

        // Prefetch both children to reduce latency on next recursive call
        #[cfg(target_arch = "x86_64")]
        {
            prefetch_children(&n.left, &n.right, prefetch::hint::T0);
        }

        if let Ok(idx) = n.items.binary_search_by(|kv| kv.key.cmp(key)) {
            return Some(&n.items[idx].val);
        }
        if n.items.is_empty() {
            return None;
        }
        if key < &n.items[0].key {
            Self::get(&n.left, key)
        } else if key > &n.items[n.items.len() - 1].key {
            Self::get(&n.right, key)
        } else {
            None
        }
    }

    fn extract_min(node: &mut Option<Box<Self>>) -> KV<K, V> {
        if node.as_ref().unwrap().left.is_some() {
            let kv = Self::extract_min(&mut node.as_mut().unwrap().left);
            Self::rebalance(node);
            kv
        } else {
            let mut current = node.take().expect("extract_min on empty node");
            let kv = current.items.remove(0);
            if current.items.is_empty() {
                *node = current.right.take();
            } else {
                let mut new_root = Some(current);
                Self::rebalance(&mut new_root);
                *node = new_root;
            }
            kv
        }
    }

    #[allow(dead_code)]
    fn extract_max(node: &mut Option<Box<Self>>) -> KV<K, V> {
        if node.as_ref().unwrap().right.is_some() {
            let kv = Self::extract_max(&mut node.as_mut().unwrap().right);
            Self::rebalance(node);
            kv
        } else {
            let mut current = node.take().expect("extract_max on empty node");
            let kv = current.items.pop().expect("node has no items");
            if current.items.is_empty() {
                *node = current.left.take();
            } else {
                let mut new_root = Some(current);
                Self::rebalance(&mut new_root);
                *node = new_root;
            }
            kv
        }
    }

    fn delete(node: &mut Option<Box<Self>>, key: &K) -> Option<V> {
        if node.is_none() {
            return None;
        }
        let mut replacement: Option<Option<Box<Self>>> = None;
        let removed = {
            let n = node.as_mut().unwrap();
            let len = n.items.len();
            match n.items.binary_search_by(|kv| kv.key.cmp(key)) {
                Ok(idx) => {
                    let val = n.items.remove(idx).val;
                    if n.items.is_empty() {
                        if n.left.is_some() && n.right.is_some() {
                            let successor = Self::extract_min(&mut n.right);
                            n.items.push(successor);
                            Some(val)
                        } else {
                            let repl = if n.left.is_some() {
                                n.left.take()
                            } else {
                                n.right.take()
                            };
                            replacement = Some(repl);
                            Some(val)
                        }
                    } else {
                        Some(val)
                    }
                }
                Err(_) => {
                    if len == 0 {
                        None
                    } else if key < &n.items[0].key {
                        Self::delete(&mut n.left, key)
                    } else if key > &n.items[len - 1].key {
                        Self::delete(&mut n.right, key)
                    } else {
                        None
                    }
                }
            }
        };
        if let Some(new_node) = replacement {
            *node = new_node;
        }
        if node.is_some() {
            Self::rebalance(node);
        }
        removed
    }

    fn satisfies_lower(key: &K, lower: &Bound<&K>) -> bool {
        match lower {
            Bound::Unbounded => true,
            Bound::Included(b) => key >= *b,
            Bound::Excluded(b) => key > *b,
        }
    }

    fn satisfies_upper(key: &K, upper: &Bound<&K>) -> bool {
        match upper {
            Bound::Unbounded => true,
            Bound::Included(b) => key <= *b,
            Bound::Excluded(b) => key < *b,
        }
    }

    fn should_visit_left(n: &Self, lower: &Bound<&K>) -> bool {
        if n.items.is_empty() {
            return false;
        }
        let min_key = &n.items[0].key;
        match *lower {
            Bound::Unbounded => true,
            Bound::Included(b) => b < min_key,
            Bound::Excluded(b) => b < min_key,
        }
    }

    fn should_visit_right(n: &Self, upper: &Bound<&K>) -> bool {
        if n.items.is_empty() {
            return false;
        }
        let max_key = &n.items[n.items.len() - 1].key;
        match *upper {
            Bound::Unbounded => true,
            Bound::Included(b) => b > max_key,
            Bound::Excluded(b) => b > max_key,
        }
    }

    fn collect_range(
        node: &Option<Box<Self>>,
        lower: &Bound<&K>,
        upper: &Bound<&K>,
        out: &mut Vec<(K, V)>,
    ) where
        K: Clone,
        V: Clone,
    {
        if let Some(n) = node {
            if Self::should_visit_left(n, lower) {
                Self::collect_range(&n.left, lower, upper, out);
            }
            for kv in &n.items {
                if Self::satisfies_lower(&kv.key, lower) && Self::satisfies_upper(&kv.key, upper) {
                    out.push((kv.key.clone(), kv.val.clone()));
                }
            }
            if Self::should_visit_right(n, upper) {
                Self::collect_range(&n.right, lower, upper, out);
            }
        }
    }
}

pub struct RangeIter<K, V> {
    inner: std::vec::IntoIter<(K, V)>,
}

impl<K, V> RangeIter<K, V> {
    fn new(data: Vec<(K, V)>) -> Self {
        Self {
            inner: data.into_iter(),
        }
    }
}

impl<K, V> Iterator for RangeIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K, V> ExactSizeIterator for RangeIter<K, V> {}

pub struct TTree<K, V> {
    root: RwLock<Option<Box<Node<K, V>>>>,
    len: AtomicUsize,
}

impl<K: Ord, V> TTree<K, V> {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(None),
            len: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(AtomicOrdering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_usage(&self) -> usize {
        self.len() * (std::mem::size_of::<K>() + std::mem::size_of::<V>())
    }

    pub fn insert(&self, key: K, val: V) -> Option<V>
    where
        V: Clone,
    {
        let mut guard = self.root.write();
        let replaced = Node::insert(&mut *guard, key, val);
        if replaced.is_none() {
            self.len.fetch_add(1, AtomicOrdering::Relaxed);
        }
        replaced
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let guard = self.root.read();
        Node::get(&*guard, key).cloned()
    }

    pub fn delete(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut guard = self.root.write();
        let removed = Node::delete(&mut *guard, key);
        if removed.is_some() {
            self.len.fetch_sub(1, AtomicOrdering::Relaxed);
        }
        removed
    }

    pub fn range(&self, lower: Bound<&K>, upper: Bound<&K>) -> RangeIter<K, V>
    where
        K: Clone,
        V: Clone,
    {
        let guard = self.root.read();
        let mut out = Vec::new();
        Node::collect_range(&*guard, &lower, &upper, &mut out);
        RangeIter::new(out)
    }

    pub fn iter(&self) -> RangeIter<K, V>
    where
        K: Clone,
        V: Clone,
    {
        self.range(Bound::Unbounded, Bound::Unbounded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use std::ops::Bound::{Excluded, Included};

    #[test]
    fn insert_and_get() {
        let tree = TTree::new();
        for i in 0..1024 {
            assert_eq!(tree.insert(i, i + 10), None);
        }
        assert_eq!(tree.len(), 1024);
        for i in 0..1024 {
            assert_eq!(tree.get(&i), Some(i + 10));
        }
    }

    #[test]
    fn replace_and_delete() {
        let tree = TTree::new();
        for i in 0..256 {
            tree.insert(i, i);
        }
        for i in 0..256 {
            assert_eq!(tree.insert(i, i * 2), Some(i));
        }
        for i in (0..256).step_by(3) {
            assert_eq!(tree.delete(&i), Some(i * 2));
            assert_eq!(tree.get(&i), None);
        }
        assert_eq!(
            tree.len(),
            256 - (256 / 3 + if 256 % 3 == 0 { 0 } else { 1 })
        );
    }

    #[test]
    fn range_query() {
        let tree = TTree::new();
        for i in 0..100 {
            tree.insert(i, i);
        }
        let collected: Vec<_> = tree.range(Included(&20), Excluded(&30)).collect();
        assert_eq!(collected, (20..30).map(|i| (i, i)).collect::<Vec<_>>());
    }

    #[test]
    fn iter_sorted() {
        let tree = TTree::new();
        for i in 0..500 {
            tree.insert(i, i);
        }
        let iter: Vec<_> = tree.iter().collect();
        assert_eq!(iter.len(), 500);
        let mut last_key = None;
        for (k, v) in iter {
            assert_eq!(k, v);
            if let Some(prev) = last_key {
                assert!(prev < k);
            }
            last_key = Some(k);
        }
    }

    #[test]
    fn randomized_insert_delete() {
        let tree = TTree::new();
        let mut values: Vec<i32> = (0..500).collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        values.shuffle(&mut rng);
        for v in &values {
            tree.insert(*v, *v);
        }
        values.shuffle(&mut rng);
        for v in &values {
            assert_eq!(tree.delete(v), Some(*v));
            assert_eq!(tree.get(v), None);
        }
        assert!(tree.is_empty());
    }

    #[test]
    fn delete_specific_key() {
        let tree = TTree::new();
        for i in 0..200 {
            tree.insert(i, i);
        }
        assert_eq!(tree.delete(&30), Some(30));
        assert_eq!(tree.get(&30), None);
        let collected: Vec<_> = tree.iter().map(|(k, _)| k).collect();
        assert!(!collected.contains(&30));
    }
}
