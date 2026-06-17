// Index layer — all indexes return (NodeId, score) or RoaringBitmap for fast fusion.
//
// Full-text:  FST dictionary + Roaring inverted index + BM25 scoring (CJK bigram)
// Vector:     HNSW graph + SIMD distance (Cosine/Euclidean/DotProduct)
// Property:   BTreeMap-based eq/range index → RoaringBitmap for predicate pushdown

pub mod fulltext;
pub mod vector;
pub mod property;
