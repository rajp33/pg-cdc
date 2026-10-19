#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_cdc_decoder::value::{TypeKind, map_value};

fuzz_target!(|input: (u8, &str)| {
    let (selector, text) = input;
    let scalar = match selector % 8 {
        0 => TypeKind::Bool,
        1 => TypeKind::Integer,
        2 => TypeKind::Float,
        3 => TypeKind::Numeric,
        4 => TypeKind::Json,
        5 => TypeKind::Timestamp,
        6 => TypeKind::Timestamptz,
        _ => TypeKind::Text,
    };
    let kind = if selector & 0x80 == 0 {
        scalar
    } else {
        let delimiter = if selector & 0x40 == 0 { ',' } else { ';' };
        TypeKind::Array { element: Box::new(scalar), delimiter }
    };
    let _ = map_value(&kind, text);
});
