use std::collections::HashMap;
use std::sync::Arc;

/// Byte-indexed trie node for SCID prefix matching.
/// Stores Arc<[u8]> references to avoid duplicate CID buffers.
#[derive(Default)]
struct CidTrieNode {
    /// The SCID value stored at this node (Some if this is a complete SCID)
    scid: Option<Arc<[u8]>>,
    /// Child nodes indexed by the next byte
    children: HashMap<u8, CidTrieNode>,
}

/// Radix trie for SCID prefix matching in short packets.
///
/// Supports O(k) longest-prefix lookup where k is the DCID length (typically 8-20 bytes),
/// replacing the O(n) linear scan of all connections.
///
/// # Memory Model
/// - Stores Arc<[u8]> references (not clones) to avoid duplicate buffers
/// - Prefix bytes are implicitly shared through trie structure
/// - Memory grows proportionally to sum of SCID lengths, not number of SCIDs
///
/// # Design
/// - Byte-by-byte traversal (no edge compression, SCIDs are fixed 8-20 bytes)
/// - Longest-prefix naturally found by trie traversal (stop at deepest match)
/// - Incremental updates on SCID rotation/retirement (O(k) per operation)
#[derive(Default)]
pub struct CidRadix {
    root: CidTrieNode,
}

impl CidRadix {
    /// Create a new empty radix trie.
    pub fn new() -> Self {
        CidRadix {
            root: CidTrieNode::default(),
        }
    }

    /// Insert an SCID into the trie.
    ///
    /// # Arguments
    /// * `scid` - The SCID (Arc<[u8]>) to insert as a key
    ///
    /// # Complexity
    /// O(k) where k = SCID length (typically 8-20 bytes)
    ///
    /// # Note
    /// If an SCID with the same bytes already exists, it is overwritten.
    /// This is acceptable because we're indexing by SCID content, and duplicate
    /// content SCIDs shouldn't occur in the connections HashMap.
    pub fn insert(&mut self, scid: Arc<[u8]>) {
        let mut node = &mut self.root;

        // Traverse/build trie path byte by byte
        for &byte in scid.iter() {
            node = node.children.entry(byte).or_insert_with(CidTrieNode::default);
        }

        // Store the SCID at the leaf
        node.scid = Some(scid);
    }

    /// Find the longest SCID prefix that matches the given DCID.
    ///
    /// # Arguments
    /// * `dcid` - The Destination Connection ID (from packet header)
    ///
    /// # Returns
    /// The longest matching SCID, or None if no match found
    ///
    /// # Complexity
    /// O(k) where k = DCID length (typically 8-20 bytes)
    ///
    /// # Behavior
    /// - Traverses trie byte-by-byte along DCID
    /// - Tracks the deepest SCID found (longest match)
    /// - Stops when a byte doesn't match any child
    /// - Returns the longest SCID encountered
    ///
    /// # Example
    /// If trie contains SCID [1,2,3,4,5,6,7,8]:
    /// - lookup([1,2,3,4,5,6,7,8,9,10]) returns Some(Arc for [1,2,3,4,5,6,7,8])
    /// - lookup([1,2,3,4]) returns Some(Arc for [1,2,3,4]) if inserted, else None
    /// - lookup([1,2,9]) returns None (no path at byte 9)
    pub fn longest_prefix_match(&self, dcid: &[u8]) -> Option<Arc<[u8]>> {
        if dcid.is_empty() {
            return None;
        }

        let mut node = &self.root;
        let mut best_match: Option<Arc<[u8]>> = None;

        // Traverse trie following DCID bytes
        for &byte in dcid {
            // If current node has a complete SCID, record it as a potential match
            if let Some(ref scid) = node.scid {
                best_match = Some(Arc::clone(scid));
            }

            // Try to move to next trie level
            match node.children.get(&byte) {
                Some(next_node) => {
                    node = next_node;
                }
                None => {
                    // Path ends, return best match found so far
                    return best_match;
                }
            }
        }

        // After consuming all DCID bytes, check the final node
        if let Some(ref scid) = node.scid {
            best_match = Some(Arc::clone(scid));
        }

        best_match
    }

    /// Remove an SCID from the trie.
    ///
    /// # Arguments
    /// * `scid` - The SCID bytes to remove
    ///
    /// # Complexity
    /// O(k) where k = SCID length (typically 8-20 bytes)
    ///
    /// # Behavior
    /// - Traverses to the leaf matching SCID bytes
    /// - Clears the SCID value at that leaf
    /// - Does NOT clean up empty nodes (lazy deletion)
    /// - If SCID not found, silently returns (no error)
    ///
    /// # Note on Memory
    /// Empty trie nodes are left in place (not deleted). This is acceptable because:
    /// - SCID retirements are infrequent (max 60s apart)
    /// - Empty nodes consume minimal memory (~40 bytes each for HashMap)
    /// - Cleanup code would be more complex and not worth the savings
    /// - Only `clear()` is called at shutdown
    pub fn remove(&mut self, scid: &[u8]) {
        let mut node = &mut self.root;

        // Traverse to the leaf
        for &byte in scid {
            match node.children.get_mut(&byte) {
                Some(next_node) => {
                    node = next_node;
                }
                None => {
                    // Path doesn't exist, nothing to remove
                    return;
                }
            }
        }

        // Clear the SCID at the leaf (lazy deletion)
        node.scid = None;
    }

    /// Remove all SCIDs from the trie.
    ///
    /// # Complexity
    /// O(N) where N = total number of nodes (tree is cleared completely)
    ///
    /// # Use Case
    /// Called once during graceful drain shutdown when all connections are being closed.
    pub fn clear(&mut self) {
        self.root = CidTrieNode::default();
    }

    /// Check if the trie is empty (no SCIDs currently indexed).
    ///
    /// # Complexity
    /// O(1)
    ///
    /// # Note
    /// Due to lazy deletion, this checks if any SCID values are stored,
    /// not if all nodes are gone. Empty nodes may persist.
    pub fn is_empty(&self) -> bool {
        self.count_scids() == 0
    }

    /// Count the number of SCIDs currently in the trie.
    ///
    /// # Complexity
    /// O(N) where N = total number of nodes
    ///
    /// # Use Case
    /// Testing and debugging only. Not used in hot path.
    fn count_scids(&self) -> usize {
        fn count_recursive(node: &CidTrieNode) -> usize {
            let mut count = if node.scid.is_some() { 1 } else { 0 };
            for child in node.children.values() {
                count += count_recursive(child);
            }
            count
        }
        count_recursive(&self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scid(bytes: &[u8]) -> Arc<[u8]> {
        Arc::from(bytes)
    }

    #[test]
    fn test_insert_and_exact_lookup() {
        let mut radix = CidRadix::new();
        let scid_bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let scid_arc = scid(&scid_bytes);

        radix.insert(scid_arc.clone());

        // Exact match
        let result = radix.longest_prefix_match(&scid_bytes);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &scid_bytes[..]);
    }

    #[test]
    fn test_prefix_match_longer_dcid() {
        let mut radix = CidRadix::new();
        let scid = scid(&[1u8, 2, 3, 4, 5, 6, 7, 8]);
        radix.insert(scid.clone());

        // DCID is longer (client appended bytes)
        let dcid = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let result = radix.longest_prefix_match(&dcid);

        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &[1u8, 2, 3, 4, 5, 6, 7, 8][..]);
    }

    #[test]
    fn test_no_match_different_prefix() {
        let mut radix = CidRadix::new();
        let scid = scid(&[1u8, 2, 3, 4, 5, 6, 7, 8]);
        radix.insert(scid);

        // Different prefix
        let dcid = [9u8, 2, 3, 4, 5, 6, 7, 8];
        let result = radix.longest_prefix_match(&dcid);

        assert!(result.is_none());
    }

    #[test]
    fn test_longest_prefix_multiple_matches() {
        let mut radix = CidRadix::new();

        // Insert two SCIDs with shared prefix
        let short_scid = scid(&[1u8, 2, 3, 4]);
        let long_scid = scid(&[1u8, 2, 3, 4, 5, 6, 7, 8]);

        radix.insert(short_scid.clone());
        radix.insert(long_scid.clone());

        // DCID matches both, but longest should be returned
        let dcid = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = radix.longest_prefix_match(&dcid);

        assert!(result.is_some());
        // Should return the 8-byte SCID, not the 4-byte one
        let matched = result.unwrap();
        assert_eq!(matched.len(), 8);
        assert_eq!(matched.as_ref(), &[1u8, 2, 3, 4, 5, 6, 7, 8][..]);
    }

    #[test]
    fn test_remove_operation() {
        let mut radix = CidRadix::new();
        let scid = scid(&[1u8, 2, 3, 4, 5, 6, 7, 8]);

        radix.insert(scid.clone());
        assert!(radix.longest_prefix_match(&[1u8, 2, 3, 4, 5, 6, 7, 8]).is_some());

        radix.remove(&[1u8, 2, 3, 4, 5, 6, 7, 8]);
        assert!(radix.longest_prefix_match(&[1u8, 2, 3, 4, 5, 6, 7, 8]).is_none());
    }

    #[test]
    fn test_remove_nonexistent_scid() {
        let mut radix = CidRadix::new();
        let scid = scid(&[1u8, 2, 3, 4, 5, 6, 7, 8]);
        radix.insert(scid);

        // Remove a different SCID (should not error)
        radix.remove(&[9u8, 8, 7, 6, 5, 4, 3, 2]);

        // Original should still be there
        assert!(radix.longest_prefix_match(&[1u8, 2, 3, 4, 5, 6, 7, 8]).is_some());
    }

    #[test]
    fn test_clear_empties_trie() {
        let mut radix = CidRadix::new();
        radix.insert(scid(&[1u8, 2, 3, 4]));
        radix.insert(scid(&[5u8, 6, 7, 8]));
        radix.insert(scid(&[9u8, 10, 11, 12]));

        radix.clear();

        assert!(radix.longest_prefix_match(&[1u8, 2, 3, 4]).is_none());
        assert!(radix.longest_prefix_match(&[5u8, 6, 7, 8]).is_none());
        assert!(radix.is_empty());
    }

    #[test]
    fn test_empty_dcid() {
        let radix = CidRadix::new();
        let result = radix.longest_prefix_match(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_trie_returns_none() {
        let radix = CidRadix::new();
        let result = radix.longest_prefix_match(&[1u8, 2, 3, 4]);
        assert!(result.is_none());
    }

    #[test]
    fn test_arc_refcount_efficiency() {
        let mut radix = CidRadix::new();
        let scid_bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let scid_arc = scid(&scid_bytes);

        // Arc refcount should be 1 before insert
        assert_eq!(Arc::strong_count(&scid_arc), 1);

        radix.insert(scid_arc.clone());

        // After insert, refcount should be 2 (original + trie)
        assert_eq!(Arc::strong_count(&scid_arc), 2);
    }

    #[test]
    fn test_realistic_scid_scenario() {
        let mut radix = CidRadix::new();

        // Simulate multiple connections with realistic 16-byte SCIDs
        let scid1 = scid(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let scid2 = scid(&[1, 2, 3, 4, 5, 6, 7, 8, 20, 21, 22, 23, 24, 25, 26, 27]);
        let scid3 = scid(&[30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45]);

        radix.insert(scid1.clone());
        radix.insert(scid2.clone());
        radix.insert(scid3.clone());

        // Client sends packet with DCID = SCID1 + extra bytes
        let dcid1 = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 99, 100];
        let result = radix.longest_prefix_match(&dcid1);
        assert_eq!(result.unwrap().len(), 16);

        // Partial prefix shouldn't match
        let partial = [1u8, 2, 3, 4];
        let result = radix.longest_prefix_match(&partial);
        assert!(result.is_none()); // Partial prefix without full 4-byte match

        // But if we had a 4-byte SCID, it would match
        let short_scid = scid(&[1u8, 2, 3, 4]);
        radix.insert(short_scid);
        let result = radix.longest_prefix_match(&partial);
        assert!(result.is_some());
    }
}
