//! Lossless segment planning over Listener's durable 16 kHz PCM recording.

pub const SAMPLE_RATE: u64 = 16_000;
pub const HARD_CUT_SAMPLES: u64 = 350 * SAMPLE_RATE;
pub const PAUSE_SEARCH_START_SAMPLES: u64 = 330 * SAMPLE_RATE;
pub const DEFAULT_OVERLAP_SAMPLES: u64 = SAMPLE_RATE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSampleRange { start: u64, end: u64 }
impl SegmentSampleRange {
    pub fn new(start: u64, end: u64) -> Option<Self> { (start < end).then_some(Self { start, end }) }
    pub fn start(&self) -> u64 { self.start }
    pub fn end(&self) -> u64 { self.end }
}

/// Plans a cut at a detected stable pause; otherwise it hard-cuts exactly at
/// 5:50. The returned next range overlaps but the non-overlapped master ranges
/// remain contiguous and no sample is discarded.
pub fn plan_next_segment(start: u64, available_end: u64, pause_at: Option<u64>, overlap: u64) -> Option<(SegmentSampleRange, u64)> {
    let hard_end = start.checked_add(HARD_CUT_SAMPLES)?;
    if available_end < hard_end { return None; }
    let pause = pause_at.filter(|pause| *pause >= start + PAUSE_SEARCH_START_SAMPLES && *pause <= hard_end);
    let end = pause.unwrap_or(hard_end);
    let segment = SegmentSampleRange::new(start, end)?;
    Some((segment, end.saturating_sub(overlap.min(end - start))))
}

/// Reassembles chunk text conservatively. Only an exact normalized overlap of
/// two or more tokens is removed; uncertain material remains visible.
pub fn stitch_transcripts(transcripts: &[String]) -> String {
    let mut assembled: Vec<String> = Vec::new();
    for transcript in transcripts {
        let tokens: Vec<String> = transcript.split_whitespace().map(str::to_owned).collect();
        let maximum = assembled.len().min(tokens.len());
        let duplicate = (2..=maximum).rev().find(|length| {
            assembled[assembled.len() - length..].iter().map(|token| normalize(token)).eq(tokens[..*length].iter().map(|token| normalize(token)))
        }).unwrap_or(0);
        assembled.extend(tokens.into_iter().skip(duplicate));
    }
    assembled.join(" ")
}

fn normalize(token: &str) -> String {
    token.chars().filter(|character| character.is_alphanumeric()).flat_map(char::to_lowercase).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uses_pause_then_preserves_overlap() {
        let pause = PAUSE_SEARCH_START_SAMPLES + SAMPLE_RATE;
        let (segment, next) = plan_next_segment(0, HARD_CUT_SAMPLES, Some(pause), DEFAULT_OVERLAP_SAMPLES).unwrap();
        assert_eq!(segment.end(), pause);
        assert_eq!(next, pause - DEFAULT_OVERLAP_SAMPLES);
    }
    #[test]
    fn hard_cuts_at_five_fifty() {
        let (segment, _) = plan_next_segment(0, HARD_CUT_SAMPLES, None, DEFAULT_OVERLAP_SAMPLES).unwrap();
        assert_eq!(segment.end(), HARD_CUT_SAMPLES);
    }
    #[test]
    fn only_removes_strong_exact_seams() {
        assert_eq!(stitch_transcripts(&["one two three".into(), "two three four".into()]), "one two three four");
        assert_eq!(stitch_transcripts(&["one two".into(), "two differs".into()]), "one two two differs");
    }
}
