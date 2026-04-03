#![no_main]

use greenmqtt_broker::mqtt::fuzz_parse_packet_frame;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    fuzz_parse_packet_frame(data);
});
