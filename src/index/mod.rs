// Index layer — all indexes return (NodeId, score) or RoaringBitmap for fast fusion.
//
// Full-text:  FST dictionary + Roaring inverted index + BM25 scoring (CJK bigram)
// Vector:     HNSW graph + SIMD distance (Cosine/Euclidean/DotProduct)
// Property:   BTreeMap-based eq/range index → RoaringBitmap for predicate pushdown

pub mod fulltext;
pub mod vector;
pub mod property;

use crate::types::NodeId;
use roaring::RoaringBitmap;

/// Common interface: every index returns scored candidates or filtered bitmaps.
pub trait IndexSearch {
    fn search(&self, query: &str) -> Vec<(NodeId, f32)>;
    fn search_bitmap(&self, query: &str) -> RoaringBitmap;
}
