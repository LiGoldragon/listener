//! Lossless segment planning over Listener's durable 16 kHz PCM recording.

pub const SAMPLE_RATE: u64 = 16_000;
pub const HARD_CUT_SAMPLES: u64 = 350 * SAMPLE_RATE;
pub const PAUSE_SEARCH_START_SAMPLES: u64 = 330 * SAMPLE_RATE;
pub const DEFAULT_OVERLAP_SAMPLES: u64 = SAMPLE_RATE;
const PAUSE_MINIMUM_SAMPLES: u64 = SAMPLE_RATE / 2;
const SILENCE_AMPLITUDE: i16 = 400;

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

/// Plans contiguous master chunks from raw mono PCM sample indices. A stable
/// low-amplitude pause near the 5:50 boundary wins; otherwise the hard cut is
/// exact. The final shorter tail is retained, and every following chunk starts
/// one second before the prior master end for provider seam context.
pub fn plan_raw_pcm_segments(pcm_s16le: &[u8]) -> Vec<SegmentSampleRange> {
    let samples = pcm_s16le.len() / 2;
    let available_end = u64::try_from(samples).unwrap_or(u64::MAX);
    let mut segments = Vec::new();
    let mut start = 0;
    while let Some((segment, next)) = plan_next_segment(
        start,
        available_end,
        stable_pause_at(pcm_s16le, start, available_end),
        DEFAULT_OVERLAP_SAMPLES,
    ) {
        segments.push(segment);
        start = next;
    }
    if start < available_end {
        if let Some(tail) = SegmentSampleRange::new(start, available_end) { segments.push(tail); }
    }
    segments
}

/// Returns only ranges that have reached a stable pause or the hard 5:50 cut.
/// The shorter live tail intentionally remains raw-log authority until a later
/// commit closes another range or Stop asks for the final assembly.
pub fn plan_closed_raw_pcm_segments(pcm_s16le: &[u8]) -> Vec<SegmentSampleRange> {
    let available_end = u64::try_from(pcm_s16le.len() / 2).unwrap_or(u64::MAX);
    let mut segments = Vec::new();
    let mut start = 0;
    while let Some((segment, next)) = plan_next_segment(
        start,
        available_end,
        stable_pause_at(pcm_s16le, start, available_end),
        DEFAULT_OVERLAP_SAMPLES,
    ) {
        segments.push(segment);
        start = next;
    }
    segments
}

fn stable_pause_at(pcm_s16le: &[u8], start: u64, available_end: u64) -> Option<u64> {
    let begin = start.checked_add(PAUSE_SEARCH_START_SAMPLES)?;
    let limit = start.checked_add(HARD_CUT_SAMPLES)?.min(available_end);
    let mut run_start = None;
    for index in begin..limit {
        let offset = usize::try_from(index.checked_mul(2)?).ok()?;
        let sample = i16::from_le_bytes([*pcm_s16le.get(offset)?, *pcm_s16le.get(offset + 1)?]);
        if sample.unsigned_abs() <= SILENCE_AMPLITUDE as u16 {
            run_start.get_or_insert(index);
            if index + 1 - run_start? >= PAUSE_MINIMUM_SAMPLES { return Some(run_start?); }
        } else {
            run_start = None;
        }
    }
    None
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
    #[test]
    fn raw_pcm_planning_keeps_tail_and_uses_a_stable_pause() {
        let samples = usize::try_from(HARD_CUT_SAMPLES + SAMPLE_RATE).unwrap();
        let mut pcm = vec![1_000_i16; samples];
        let pause = usize::try_from(PAUSE_SEARCH_START_SAMPLES + SAMPLE_RATE).unwrap();
        pcm[pause..pause + usize::try_from(PAUSE_MINIMUM_SAMPLES).unwrap()].fill(0);
        let bytes: Vec<u8> = pcm.into_iter().flat_map(i16::to_le_bytes).collect();
        let planned = plan_raw_pcm_segments(&bytes);
        assert_eq!(planned[0].end(), u64::try_from(pause).unwrap());
        assert_eq!(planned[1].start(), u64::try_from(pause).unwrap() - DEFAULT_OVERLAP_SAMPLES);
        assert_eq!(planned.last().unwrap().end(), u64::try_from(samples).unwrap());
    }

    #[test]
    fn closed_planning_omits_the_live_tail_until_stop() {
        let samples = usize::try_from(HARD_CUT_SAMPLES + SAMPLE_RATE / 2).unwrap();
        let pcm: Vec<u8> = vec![1_000_i16; samples]
            .into_iter()
            .flat_map(i16::to_le_bytes)
            .collect();
        assert_eq!(
            plan_closed_raw_pcm_segments(&pcm),
            vec![SegmentSampleRange::new(0, HARD_CUT_SAMPLES).unwrap()],
        );
    }
}
