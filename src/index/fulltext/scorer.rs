// BM25 scoring function.
//
// Parameters:
//   k1 = 1.2 — term frequency saturation
//   b  = 0.75 — length normalization
//
// Formula:
//   score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avg_dl))
//   IDF = ln((N - df + 0.5) / (df + 0.5) + 1)

#[inline]
pub fn bm25_score(tf: f32, df: f64, doc_len: f32, avg_doc_len: f64, total_docs: f64) -> f32 {
    let k1 = 1.2f32;
    let b = 0.75f32;

    // IDF
    let df_f32 = df as f32;
    let total_f32 = total_docs as f32;
    let idf = ((total_f32 - df_f32 + 0.5) / (df_f32 + 0.5) + 1.0).ln();

    // Normalized TF
    let tf_norm = (tf * (k1 + 1.0))
        / (tf + k1 * (1.0 - b + b * doc_len / avg_doc_len as f32));

    idf * tf_norm
}
