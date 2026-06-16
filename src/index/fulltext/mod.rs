// CJK Full-Text Index
//
// Architecture:
//   CjkAnalyzer → tokenize text into bigrams + single chars
//   FST (Finite State Transducer) → compact term dictionary
//   RoaringBitmap → inverted postings list
//   BM25 scoring → tf-idf variant with doc length normalization

mod analyzer;
mod fst_dict;
mod scorer;

pub use analyzer::CjkAnalyzer;
pub use scorer::bm25_score;

use crate::types::{NodeId, Score};
use crate::index::IndexSearch;
use fst::Map as FstMap;
use roaring::RoaringBitmap;
use dashmap::DashMap;

pub struct FullTextIndex {
    name: String,
    /// Labels covered by this index
    labels: Vec<String>,
    /// Properties indexed
    properties: Vec<String>,
    /// FST: term → ordinal (compact trie)
    fst: Option<FstMap<Vec<u8>>>,
    /// Postings[ordinal] = bitmap of NodeId
    postings: Vec<RoaringBitmap>,
    /// Postings positions for phrase queries
    positions: Vec<DashMap<NodeId, Vec<u32>>>,
    /// Per-document term count (for BM25)
    doc_lengths: DashMap<NodeId, u32>,
    total_docs: atomic::AtomicU64,
    avg_doc_length: atomic::AtomicF64,
}

impl FullTextIndex {
    pub fn new(name: &str, labels: Vec<String>, properties: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            labels,
            properties,
            fst: None,
            postings: Vec::new(),
            positions: Vec::new(),
            doc_lengths: DashMap::new(),
            total_docs: atomic::AtomicU64::new(0),
            avg_doc_length: atomic::AtomicF64::new(0.0),
        }
    }

    pub fn search(&self, query: &str) -> Vec<(NodeId, Score)> {
        let tokens = CjkAnalyzer::tokenize(query);
        let mut results: dashmap::DashMap<NodeId, Score> = dashmap::DashMap::new();

        if let Some(ref fst) = self.fst {
            for token in &tokens {
                if let Some(ordinal) = fst.get(token.as_bytes()) {
                    let posting = &self.postings[ordinal as usize];
                    let df = posting.len() as f64;
                    let total = self.total_docs.load(atomic::Ordering::Relaxed) as f64;
                    let avg_dl = self.avg_doc_length.load(atomic::Ordering::Relaxed);

                    for node_id in posting.iter() {
                        let tf = self.count_tf(ordinal as usize, node_id);
                        let dl = self.doc_lengths.get(&node_id)
                            .map(|r| *r as f32).unwrap_or(1.0);
                        let score = bm25_score(tf, df, dl, avg_dl, total);
                        *results.entry(node_id).or_default() += score;
                    }
                }
            }
        }

        let mut sorted: Vec<_> = results.into_iter().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sorted
    }

    fn count_tf(&self, ordinal: usize, node_id: NodeId) -> f32 {
        self.positions.get(ordinal)
            .and_then(|map| map.get(&node_id).map(|v| v.len() as f32))
            .unwrap_or(1.0)
    }
}
