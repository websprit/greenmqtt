#[cfg(feature = "fuzzing")]
use super::{connect_packet, connect_packet_v5, malformed_remaining_length_packet};

#[cfg(feature = "fuzzing")]
#[test]
fn fuzz_parse_packet_frame_accepts_empty_input() {
    crate::mqtt::fuzz_parse_packet_frame(&[]);
}

#[cfg(feature = "fuzzing")]
#[test]
fn fuzz_parse_packet_frame_accepts_valid_connect_frames() {
    crate::mqtt::fuzz_parse_packet_frame(&connect_packet("fuzz-v4"));
    crate::mqtt::fuzz_parse_packet_frame(&connect_packet_v5("fuzz-v5"));
}

#[cfg(feature = "fuzzing")]
#[test]
fn fuzz_parse_packet_frame_accepts_malformed_remaining_length_frames() {
    crate::mqtt::fuzz_parse_packet_frame(&malformed_remaining_length_packet());
}
