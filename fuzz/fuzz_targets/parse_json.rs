#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_cdc_decoder::value::parse_json;

fuzz_target!(|text: &str| {
    let Some(value) = parse_json(text) else {
        return;
    };
    for rendered in [
        serde_json::to_string(&value).unwrap(),
        serde_json::to_string_pretty(&value).unwrap(),
    ] {
        assert_eq!(parse_json(&rendered).as_ref(), Some(&value), "{rendered}");
    }
});
