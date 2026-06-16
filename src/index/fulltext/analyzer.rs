// CJK Bigram Analyzer — no dictionary required, works for Chinese/Japanese/Korean.
//
// Strategy:
//   CJK chars → overlapping bigrams + single chars (boundary coverage)
//   Latin chars → lowercase word chunks
//   Numbers → numeric tokens
//   Everything else → skipped

#[derive(Debug, Clone)]
pub struct Token {
    pub term: String,
    pub position: u32,
}

pub struct CjkAnalyzer;

impl CjkAnalyzer {
    pub fn tokenize(text: &str) -> Vec<Token> {
        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return Vec::new();
        }

        let mut tokens = Vec::new();
        let mut i = 0;
        let mut pos = 0u32;

        while i < chars.len() {
            let c = chars[i];

            if c is_cjk(c) {
                // Overlapping bigram
                if i + 1 < chars.len() && is_cjk(chars[i + 1]) {
                    let bigram: String = chars[i..=i + 1].iter().collect();
                    tokens.push(Token { term: bigram, position: pos });
                    pos += 1;
                }
                // Single char (for boundary/stop-word resilience)
                tokens.push(Token {
                    term: c.to_string(),
                    position: pos,
                });
                pos += 1;
                i += 1;
            } else if c.is_alphabetic() {
                let start = i;
                while i < chars.len() && chars[i].is_alphabetic() {
                    i += 1;
                }
                let word: String = chars[start..i]
                    .iter()
                    .flat_map(|ch| ch.to_lowercase())
                    .collect();
                tokens.push(Token { term: word, position: pos });
                pos += 1;
            } else if c.is_numeric() {
                let start = i;
                while i < chars.len() && chars[i].is_numeric() {
                    i += 1;
                }
                tokens.push(Token {
                    term: chars[start..i].iter().collect(),
                    position: pos,
                });
                pos += 1;
            } else {
                i += 1;
            }
        }
        tokens
    }
}

#[inline]
fn is_cjk(c: char) -> bool {
    matches!(
        c,
        '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}' // CJK Extension A
        | '\u{F900}'..='\u{FAFF}' // CJK Compatibility
        | '\u{3040}'..='\u{309F}' // Hiragana
        | '\u{30A0}'..='\u{30FF}' // Katakana
        | '\u{AC00}'..='\u{D7AF}' // Hangul
    )
}
