/// ART Node types based on Leis et al. 2013 paper
/// "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases"

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Maximum prefix length before compression
const MAX_PREFIX_LEN: usize = 8;

/// ART Node variants (adaptive sizing)
#[derive(Clone)]
pub enum Node<K, V> {
    /// 4 children (sparse)
    Node4(Box<Node4<K, V>>),
    /// 16 children (medium density)
    Node16(Box<Node16<K, V>>),
    /// 48 children (dense with index array)
    Node48(Box<Node48<K, V>>),
    /// 256 children (fully dense)
    Node256(Box<Node256<K, V>>),
    /// Leaf node with key and value
    Leaf(LeafNode<K, V>),
}

/// Leaf node containing key and value
#[derive(Clone)]
pub struct LeafNode<K, V> {
    pub key_bytes: Vec<u8>,
    pub key: K,  // Original key for range queries
    pub value: V,
}

impl<K, V> LeafNode<K, V> {
    pub fn new(key_bytes: Vec<u8>, key: K, value: V) -> Self {
        Self { key_bytes, key, value }
    }
    
    pub fn matches_key(&self, search_key: &[u8]) -> bool {
        self.key_bytes.as_slice() == search_key
    }
}

/// Common node header
#[derive(Clone, Copy)]
pub struct NodeHeader {
    /// Number of non-null children
    pub num_children: u16,
    /// Compressed path prefix
    pub prefix: [u8; MAX_PREFIX_LEN],
    /// Actual prefix length (may exceed MAX_PREFIX_LEN)
    pub prefix_len: u32,
}

impl NodeHeader {
    pub fn new() -> Self {
        Self {
            num_children: 0,
            prefix: [0; MAX_PREFIX_LEN],
            prefix_len: 0,
        }
    }

    /// Check if key prefix matches node prefix
    pub fn check_prefix(&self, key: &[u8], depth: usize) -> usize {
        let min_len = usize::min(self.prefix_len as usize, MAX_PREFIX_LEN);
        
        for i in 0..min_len {
            if depth + i >= key.len() || key[depth + i] != self.prefix[i] {
                return i;
            }
        }
        
        min_len
    }
}

/// Node4: 4 children (sorted array, linear search)
#[derive(Clone)]
pub struct Node4<K, V> {
    pub header: NodeHeader,
    pub keys: [u8; 4],
    pub children: [Option<Box<Node<K, V>>>; 4],
}

impl<V> Node4<K, V> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader::new(),
            keys: [0; 4],
            children: [None, None, None, None],
        }
    }

    pub fn find_child(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        for i in 0..self.header.num_children as usize {
            if self.keys[i] == key_byte {
                return self.children[i].as_ref();
            }
        }
        None
    }

    pub fn find_child_mut(&mut self, key_byte: u8) -> Option<&mut Box<Node<K, V>>> {
        for i in 0..self.header.num_children as usize {
            if self.keys[i] == key_byte {
                return self.children[i].as_mut();
            }
        }
        None
    }

    pub fn add_child(&mut self, key_byte: u8, child: Box<Node<K, V>>) -> bool {
        if self.header.num_children >= 4 {
            return false; // Need to grow to Node16
        }

        let idx = self.header.num_children as usize;
        self.keys[idx] = key_byte;
        self.children[idx] = Some(child);
        self.header.num_children += 1;
        true
    }

    pub fn is_full(&self) -> bool {
        self.header.num_children >= 4
    }
}

/// Node16: 16 children (sorted array, binary search or SIMD)
#[derive(Clone)]
pub struct Node16<K, V> {
    pub header: NodeHeader,
    pub keys: [u8; 16],
    pub children: [Option<Box<Node<K, V>>>; 16],
}

impl<V> Node16<K, V> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader::new(),
            keys: [0; 16],
            children: std::array::from_fn(|_| None),
        }
    }

    pub fn from_node4(mut n4: Node4<K, V>) -> Self {
        let mut n16 = Self::new();
        n16.header = n4.header;
        
        for i in 0..n4.header.num_children as usize {
            n16.keys[i] = n4.keys[i];
            n16.children[i] = n4.children[i].take();
        }
        
        n16
    }

    pub fn find_child(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse2") {
                return unsafe { self.find_child_simd(key_byte) };
            }
        }
        
        // Fallback: linear search
        self.find_child_linear(key_byte)
    }

    /// SIMD-accelerated search for x86_64 with SSE2
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    unsafe fn find_child_simd(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        let n = self.header.num_children as usize;
        if n == 0 {
            return None;
        }

        // Load 16 keys into SIMD register
        let keys_ptr = self.keys.as_ptr();
        let keys_vec = _mm_loadu_si128(keys_ptr as *const __m128i);
        
        // Broadcast search key to all 16 bytes
        let search_key = _mm_set1_epi8(key_byte as i8);
        
        // Compare all 16 bytes in parallel
        let cmp = _mm_cmpeq_epi8(keys_vec, search_key);
        
        // Extract comparison result as bitmask
        let mask = _mm_movemask_epi8(cmp) as u16;
        
        if mask == 0 {
            return None; // No match
        }
        
        // Find first set bit (first match)
        let idx = mask.trailing_zeros() as usize;
        
        // Validate index is within num_children
        if idx < n {
            self.children[idx].as_ref()
        } else {
            None
        }
    }

    /// Linear search fallback (non-SIMD architectures)
    fn find_child_linear(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        for i in 0..self.header.num_children as usize {
            if self.keys[i] == key_byte {
                return self.children[i].as_ref();
            }
        }
        None
    }

    pub fn find_child_mut(&mut self, key_byte: u8) -> Option<&mut Box<Node<K, V>>> {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse2") {
                let idx = unsafe { self.find_index_simd(key_byte) };
                if let Some(i) = idx {
                    return self.children[i].as_mut();
                }
                return None;
            }
        }
        
        // Fallback: linear search
        for i in 0..self.header.num_children as usize {
            if self.keys[i] == key_byte {
                return self.children[i].as_mut();
            }
        }
        None
    }

    /// SIMD index finder for mutable access
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    unsafe fn find_index_simd(&self, key_byte: u8) -> Option<usize> {
        let n = self.header.num_children as usize;
        if n == 0 {
            return None;
        }

        let keys_ptr = self.keys.as_ptr();
        let keys_vec = _mm_loadu_si128(keys_ptr as *const __m128i);
        let search_key = _mm_set1_epi8(key_byte as i8);
        let cmp = _mm_cmpeq_epi8(keys_vec, search_key);
        let mask = _mm_movemask_epi8(cmp) as u16;
        
        if mask == 0 {
            return None;
        }
        
        let idx = mask.trailing_zeros() as usize;
        if idx < n {
            Some(idx)
        } else {
            None
        }
    }

    pub fn add_child(&mut self, key_byte: u8, child: Box<Node<K, V>>) -> bool {
        if self.header.num_children >= 16 {
            return false; // Need to grow to Node48
        }

        let idx = self.header.num_children as usize;
        self.keys[idx] = key_byte;
        self.children[idx] = Some(child);
        self.header.num_children += 1;
        true
    }

    pub fn is_full(&self) -> bool {
        self.header.num_children >= 16
    }
}

/// Node48: 48 children (index array + child array for cache efficiency)
#[derive(Clone)]
pub struct Node48<K, V> {
    pub header: NodeHeader,
    /// Index array: 256 entries mapping byte -> child index (255 = empty)
    pub index: [u8; 256],
    /// Child array: 48 slots
    pub children: [Option<Box<Node<K, V>>>; 48],
}

impl<V> Node48<K, V> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader::new(),
            index: [255; 256],
            children: std::array::from_fn(|_| None),
        }
    }

    pub fn from_node16(mut n16: Node16<K, V>) -> Self {
        let mut n48 = Self::new();
        n48.header = n16.header;
        
        for i in 0..n16.header.num_children as usize {
            let key_byte = n16.keys[i];
            n48.index[key_byte as usize] = i as u8;
            n48.children[i] = n16.children[i].take();
        }
        
        n48
    }

    pub fn find_child(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        let idx = self.index[key_byte as usize];
        if idx == 255 {
            return None;
        }
        self.children[idx as usize].as_ref()
    }

    pub fn find_child_mut(&mut self, key_byte: u8) -> Option<&mut Box<Node<K, V>>> {
        let idx = self.index[key_byte as usize];
        if idx == 255 {
            return None;
        }
        self.children[idx as usize].as_mut()
    }

    pub fn add_child(&mut self, key_byte: u8, child: Box<Node<K, V>>) -> bool {
        if self.header.num_children >= 48 {
            return false; // Need to grow to Node256
        }

        let slot = self.header.num_children as usize;
        self.index[key_byte as usize] = slot as u8;
        self.children[slot] = Some(child);
        self.header.num_children += 1;
        true
    }

    pub fn is_full(&self) -> bool {
        self.header.num_children >= 48
    }
}

/// Node256: 256 children (direct indexing, maximum density)
#[derive(Clone)]
pub struct Node256<K, V> {
    pub header: NodeHeader,
    pub children: [Option<Box<Node<K, V>>>; 256],
}

impl<V> Node256<K, V> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader::new(),
            children: std::array::from_fn(|_| None),
        }
    }

    pub fn from_node48(mut n48: Node48<K, V>) -> Self {
        let mut n256 = Self::new();
        n256.header = n48.header;
        
        for byte in 0..256 {
            let idx = n48.index[byte];
            if idx != 255 {
                n256.children[byte] = n48.children[idx as usize].take();
            }
        }
        
        n256
    }

    pub fn find_child(&self, key_byte: u8) -> Option<&Box<Node<K, V>>> {
        self.children[key_byte as usize].as_ref()
    }

    pub fn find_child_mut(&mut self, key_byte: u8) -> Option<&mut Box<Node<K, V>>> {
        self.children[key_byte as usize].as_mut()
    }

    pub fn add_child(&mut self, key_byte: u8, child: Box<Node<K, V>>) {
        if self.children[key_byte as usize].is_none() {
            self.header.num_children += 1;
        }
        self.children[key_byte as usize] = Some(child);
    }
}

/// Helper to grow nodes
impl<V> Node<K, V> {
    pub fn grow(self) -> Self {
        match self {
            Node::Node4(n4) => {
                Node::Node16(Box::new(Node16::from_node4(*n4)))
            }
            Node::Node16(n16) => {
                Node::Node48(Box::new(Node48::from_node16(*n16)))
            }
            Node::Node48(n48) => {
                Node::Node256(Box::new(Node256::from_node48(*n48)))
            }
            _ => self, // Node256 and Leaf cannot grow
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    pub fn as_leaf(&self) -> Option<&LeafNode<K, V>> {
        match self {
            Node::Leaf(leaf) => Some(leaf),
            _ => None,
        }
    }
    
    pub fn as_leaf_mut(&mut self) -> Option<&mut LeafNode<K, V>> {
        match self {
            Node::Leaf(leaf) => Some(leaf),
            _ => None,
        }
    }
}
