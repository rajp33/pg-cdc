mod array;
mod datetime;
mod number;

use serde_json::{Number, Value};

pub use datetime::format_commit_ts;

const MAX_JSON_NUMBER_EXPANSION: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeKind {
    Bool,
    Integer,
    Float,
    Numeric,
    Json,
    Timestamp,
    Timestamptz,
    Text,
    Array {
        element: Box<TypeKind>,
        delimiter: char,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapped {
    pub value: Value,
    pub fallback: bool,
}

pub fn builtin_kind(oid: u32) -> Option<TypeKind> {
    let element = match oid {
        1000 => 16,
        1005 => 21,
        1007 => 23,
        1016 => 20,
        1028 => 26,
        1021 => 700,
        1022 => 701,
        1231 => 1700,
        199 => 114,
        3807 => 3802,
        1115 => 1114,
        1185 => 1184,
        1001 => 17,
        1002 => 18,
        1003 => 19,
        1009 => 25,
        1014 => 1042,
        1015 => 1043,
        1182 => 1082,
        _ => return builtin_scalar(oid),
    };
    builtin_scalar(element).map(|element| TypeKind::Array {
        element: Box::new(element),
        delimiter: ',',
    })
}

fn builtin_scalar(oid: u32) -> Option<TypeKind> {
    match oid {
        16 => Some(TypeKind::Bool),
        20 | 21 | 23 | 26 => Some(TypeKind::Integer),
        700 | 701 => Some(TypeKind::Float),
        1700 => Some(TypeKind::Numeric),
        114 | 3802 => Some(TypeKind::Json),
        1114 => Some(TypeKind::Timestamp),
        1184 => Some(TypeKind::Timestamptz),
        17 | 18 | 19 | 25 | 1042 | 1043 | 1082 => Some(TypeKind::Text),
        _ => None,
    }
}

pub fn map_value(kind: &TypeKind, text: &str) -> Mapped {
    let mut fallback = false;
    let value = map_into(kind, text, &mut fallback);
    Mapped { value, fallback }
}

fn map_into(kind: &TypeKind, text: &str, fallback: &mut bool) -> Value {
    let typed = match kind {
        TypeKind::Array { element, delimiter } => {
            array::parse(text, *delimiter).map(|node| array_value(node, element, fallback))
        }
        scalar => map_scalar(scalar, text),
    };
    typed.unwrap_or_else(|| {
        *fallback = true;
        Value::String(text.to_owned())
    })
}

fn array_value(node: array::Node, element: &TypeKind, fallback: &mut bool) -> Value {
    match node {
        array::Node::Null => Value::Null,
        array::Node::Element(text) => map_into(element, &text, fallback),
        array::Node::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| array_value(item, element, fallback))
                .collect(),
        ),
    }
}

fn map_scalar(kind: &TypeKind, text: &str) -> Option<Value> {
    match kind {
        TypeKind::Bool => match text {
            "t" => Some(Value::Bool(true)),
            "f" => Some(Value::Bool(false)),
            _ => None,
        },
        TypeKind::Integer => integer(text),
        TypeKind::Float | TypeKind::Numeric => {
            non_finite(text).or_else(|| number::to_json_number(text))
        }
        TypeKind::Json => json(text),
        TypeKind::Timestamp => datetime::timestamp(text).map(Value::String),
        TypeKind::Timestamptz => datetime::timestamptz(text).map(Value::String),
        TypeKind::Text | TypeKind::Array { .. } => Some(Value::String(text.to_owned())),
    }
}

fn non_finite(text: &str) -> Option<Value> {
    matches!(text, "NaN" | "Infinity" | "-Infinity").then(|| Value::String(text.to_owned()))
}

fn integer(text: &str) -> Option<Value> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    serde_json::from_str::<Number>(text).ok().map(Value::Number)
}

fn json(text: &str) -> Option<Value> {
    let mut budget = MAX_JSON_NUMBER_EXPANSION;
    normalize_numbers(serde_json::from_str(text).ok()?, &mut budget)
}

fn normalize_numbers(value: Value, budget: &mut usize) -> Option<Value> {
    Some(match value {
        Value::Number(number) => number::to_json_number_within(&number.to_string(), budget)?,
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| normalize_numbers(item, budget))
                .collect::<Option<_>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| normalize_numbers(value, budget).map(|value| (key, value)))
                .collect::<Option<_>>()?,
        ),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn mapped(kind: &TypeKind, text: &str) -> Value {
        let result = map_value(kind, text);
        assert!(!result.fallback, "{text:?} as {kind:?} fell back");
        result.value
    }

    fn array_of(element: TypeKind) -> TypeKind {
        TypeKind::Array {
            element: Box::new(element),
            delimiter: ',',
        }
    }

    #[test]
    fn maps_booleans_and_integers() {
        assert_eq!(mapped(&TypeKind::Bool, "t"), json!(true));
        assert_eq!(mapped(&TypeKind::Bool, "f"), json!(false));
        assert_eq!(
            mapped(&TypeKind::Integer, "-9223372036854775808").to_string(),
            "-9223372036854775808"
        );
        assert_eq!(
            mapped(&TypeKind::Integer, "4294967295").to_string(),
            "4294967295"
        );
    }

    #[test]
    fn unparsable_typed_values_fall_back_to_their_text() {
        for (kind, text) in [
            (TypeKind::Bool, "yes"),
            (TypeKind::Integer, "12a"),
            (TypeKind::Integer, "007"),
            (TypeKind::Float, "1.2.3"),
            (TypeKind::Numeric, "1e"),
            (TypeKind::Json, "{"),
            (TypeKind::Timestamp, "noon"),
            (TypeKind::Timestamptz, "2026-10-14 12:00:00"),
        ] {
            let result = map_value(&kind, text);
            assert!(result.fallback, "{text:?} as {kind:?}");
            assert_eq!(result.value, Value::String(text.to_owned()));
        }
    }

    #[test]
    fn floats_render_as_to_jsonb_plain_decimals() {
        let hundred_zeros = format!("1{}", "0".repeat(100));
        let tiny = format!("0.{}5", "0".repeat(323));
        for (text, expected) in [
            ("1e+100", hundred_zeros.as_str()),
            ("1.5e-07", "0.00000015"),
            ("-1.5e-07", "-0.00000015"),
            ("1.2345678901234568e+20", "123456789012345680000"),
            (
                "3.4028234663852886e+38",
                "340282346638528860000000000000000000000",
            ),
            ("0.1", "0.1"),
            ("-0", "0"),
            ("1e-05", "0.00001"),
            ("5e-324", tiny.as_str()),
            ("42", "42"),
        ] {
            assert_eq!(
                mapped(&TypeKind::Float, text).to_string(),
                expected,
                "{text}"
            );
        }
    }

    #[test]
    fn non_finite_floats_and_numerics_are_strings() {
        for kind in [TypeKind::Float, TypeKind::Numeric] {
            for text in ["NaN", "Infinity", "-Infinity"] {
                assert_eq!(mapped(&kind, text), json!(text));
            }
        }
    }

    #[test]
    fn numerics_keep_exact_digits() {
        assert_eq!(mapped(&TypeKind::Numeric, "1.50").to_string(), "1.50");
        assert_eq!(
            mapped(&TypeKind::Numeric, "-12.3456").to_string(),
            "-12.3456"
        );
        assert_eq!(
            mapped(&TypeKind::Numeric, "0.000000000001").to_string(),
            "0.000000000001"
        );
        assert_eq!(
            mapped(&TypeKind::Numeric, "1000000000000000000000000000000").to_string(),
            "1000000000000000000000000000000"
        );
    }

    #[test]
    fn json_is_embedded_with_to_jsonb_numbers() {
        let value = mapped(
            &TypeKind::Json,
            r#"{"b": 1, "a": 1e2, "a": 3, "c": [-0, 1.0E1, "x"]}"#,
        );
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"b":1,"a":3,"c":[0,10,"x"]}"#
        );
        assert_eq!(mapped(&TypeKind::Json, "null"), Value::Null);
        assert_eq!(mapped(&TypeKind::Json, r#"{"x": 100}"#), json!({"x": 100}));
    }

    #[test]
    fn json_number_expansion_is_bounded_per_value() {
        let literals = vec!["1e131071"; 1000].join(",");
        let text = format!("[{literals}]");
        let result = map_value(&TypeKind::Json, &text);
        assert!(result.fallback);
        assert_eq!(result.value, Value::String(text));

        let modest = format!("[{}]", vec!["1e100"; 100].join(","));
        let result = map_value(&TypeKind::Json, &modest);
        assert!(!result.fallback);
        assert_eq!(result.value.to_string().len(), 100 * 101 + 99 + 2);
    }

    #[test]
    fn deeply_nested_json_falls_back_to_text() {
        let text = format!("{}{}", "[".repeat(200), "]".repeat(200));
        let result = map_value(&TypeKind::Json, &text);
        assert!(result.fallback);
        assert_eq!(result.value, Value::String(text));
    }

    #[test]
    fn timestamps_render_in_xsd_form() {
        for (text, expected) in [
            ("2026-10-14 12:00:00.5", "2026-10-14T12:00:00.5"),
            ("0044-03-15 12:00:00 BC", "0044-03-15T12:00:00 BC"),
            ("-infinity", "-infinity"),
            ("infinity", "infinity"),
        ] {
            assert_eq!(
                mapped(&TypeKind::Timestamp, text),
                json!(expected),
                "{text}"
            );
        }
        for (text, expected) in [
            (
                "2026-10-14 12:00:00.123456+00",
                "2026-10-14T12:00:00.123456+00:00",
            ),
            ("2026-10-14 12:00:00+00", "2026-10-14T12:00:00+00:00"),
            ("0044-03-15 12:00:00+00 BC", "0044-03-15T12:00:00+00:00 BC"),
            ("10000-01-01 00:00:00+00", "10000-01-01T00:00:00+00:00"),
            ("2026-10-14 12:00:00+05:30", "2026-10-14T12:00:00+05:30"),
            (
                "1900-01-01 00:00:00-00:01:15",
                "1900-01-01T00:00:00-00:01:15",
            ),
            ("infinity", "infinity"),
        ] {
            assert_eq!(
                mapped(&TypeKind::Timestamptz, text),
                json!(expected),
                "{text}"
            );
        }
    }

    #[test]
    fn text_kinds_are_strings_verbatim() {
        for text in [
            "",
            "\\x0102",
            "ab  ",
            "2026-10-14",
            "0044-03-15 BC",
            "(1,\"x y\")",
            "emoji 🐘",
        ] {
            assert_eq!(mapped(&TypeKind::Text, text), json!(text));
        }
    }

    #[test]
    fn arrays_nest_by_dimension_and_map_elements() {
        assert_eq!(
            mapped(&array_of(TypeKind::Integer), "{1,NULL,3}"),
            json!([1, null, 3])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Integer), "[0:1]={1,2}"),
            json!([1, 2])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Integer), "{{1,2},{3,4}}"),
            json!([[1, 2], [3, 4]])
        );
        assert_eq!(mapped(&array_of(TypeKind::Integer), "{}"), json!([]));
        assert_eq!(
            mapped(&array_of(TypeKind::Text), r#"{{a,"b c"},{"NULL",NULL}}"#),
            json!([["a", "b c"], ["NULL", null]])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Text), r#"{"q\"uote","back\\slash",""}"#),
            json!(["q\"uote", "back\\slash", ""])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Text), r#"{"\\x01","\\x"}"#),
            json!(["\\x01", "\\x"])
        );
        assert_eq!(
            mapped(
                &array_of(TypeKind::Timestamptz),
                r#"{"2026-10-14 12:00:00+00"}"#
            ),
            json!(["2026-10-14T12:00:00+00:00"])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Json), r#"{"{\"a\": 1}"}"#),
            json!([{"a": 1}])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Float), "{NaN,1.5}"),
            json!(["NaN", 1.5])
        );
        assert_eq!(
            mapped(&array_of(TypeKind::Bool), "{t,f}"),
            json!([true, false])
        );
        let boxes = TypeKind::Array {
            element: Box::new(TypeKind::Text),
            delimiter: ';',
        };
        assert_eq!(
            mapped(&boxes, "{(1,1),(0,0);(2,2),(1,1)}"),
            json!(["(1,1),(0,0)", "(2,2),(1,1)"])
        );
    }

    #[test]
    fn malformed_arrays_fall_back_to_text() {
        for text in [
            "{1,2",
            "{1,2}x",
            "1,2",
            r#"{"a}"#,
            "{,}",
            "{{{{{{{1}}}}}}}",
            "[1:2={1,2}",
            "{a\"b}",
        ] {
            let result = map_value(&array_of(TypeKind::Text), text);
            assert!(result.fallback, "{text:?}");
            assert_eq!(result.value, Value::String(text.to_owned()));
        }
    }

    #[test]
    fn array_element_fallback_marks_the_value() {
        let result = map_value(&array_of(TypeKind::Integer), "{1,x}");
        assert!(result.fallback);
        assert_eq!(result.value, json!([1, "x"]));
    }

    #[test]
    fn builtin_kinds_cover_the_mapped_types() {
        assert_eq!(builtin_kind(26), Some(TypeKind::Integer));
        assert_eq!(builtin_kind(1184), Some(TypeKind::Timestamptz));
        assert_eq!(builtin_kind(1082), Some(TypeKind::Text));
        assert_eq!(builtin_kind(1009), Some(array_of(TypeKind::Text)));
        assert_eq!(builtin_kind(3807), Some(array_of(TypeKind::Json)));
        assert_eq!(builtin_kind(2950), None);
        assert_eq!(builtin_kind(16_389), None);
    }

    #[test]
    fn commit_timestamps_use_to_jsonb_form() {
        assert_eq!(
            format_commit_ts(845_294_400_123_456),
            "2026-10-14T12:00:00.123456+00:00"
        );
        assert_eq!(
            format_commit_ts(845_294_400_500_000),
            "2026-10-14T12:00:00.5+00:00"
        );
        assert_eq!(
            format_commit_ts(845_285_183_069_961),
            "2026-10-14T09:26:23.069961+00:00"
        );
        assert_eq!(
            format_commit_ts(762_480_000_000_010),
            "2024-02-29T00:00:00.00001+00:00"
        );
        assert_eq!(format_commit_ts(0), "2000-01-01T00:00:00+00:00");
        assert_eq!(format_commit_ts(-1), "1999-12-31T23:59:59.999999+00:00");
    }

    fn render_pg_array(elements: &[Option<String>]) -> String {
        let rendered: Vec<String> = elements
            .iter()
            .map(|element| match element {
                None => "NULL".to_owned(),
                Some(text) => {
                    let needs_quotes = text.is_empty()
                        || text.eq_ignore_ascii_case("NULL")
                        || text.chars().any(|c| {
                            matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_ascii_whitespace()
                        });
                    if needs_quotes {
                        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
                    } else {
                        text.clone()
                    }
                }
            })
            .collect();
        format!("{{{}}}", rendered.join(","))
    }

    fn scalar_kind() -> impl Strategy<Value = TypeKind> {
        prop_oneof![
            Just(TypeKind::Bool),
            Just(TypeKind::Integer),
            Just(TypeKind::Float),
            Just(TypeKind::Numeric),
            Just(TypeKind::Json),
            Just(TypeKind::Timestamp),
            Just(TypeKind::Timestamptz),
            Just(TypeKind::Text),
        ]
    }

    proptest! {
        #[test]
        fn d2_map_value_is_total(kind in scalar_kind(), text in ".*", as_array in any::<bool>(), semicolon in any::<bool>()) {
            let scalar = kind.clone();
            let kind = if as_array {
                TypeKind::Array { element: Box::new(kind), delimiter: if semicolon { ';' } else { ',' } }
            } else {
                kind
            };
            let result = map_value(&kind, &text);
            if !as_array && result.fallback {
                prop_assert_eq!(result.value, Value::String(text.clone()));
            }
            if !as_array && scalar == TypeKind::Text {
                prop_assert!(!result.fallback);
                prop_assert_eq!(map_value(&TypeKind::Text, &text).value, Value::String(text));
            }
        }

        #[test]
        fn float_normalization_preserves_the_value(x in any::<f64>().prop_filter("finite", |x| x.is_finite())) {
            let mapped = map_value(&TypeKind::Float, &format!("{x:e}"));
            prop_assert!(!mapped.fallback);
            let parsed: f64 = mapped.value.to_string().parse().unwrap();
            prop_assert_eq!(parsed.to_bits(), (x + 0.0).to_bits());
        }

        #[test]
        fn text_arrays_round_trip(elements in proptest::collection::vec(proptest::option::of(".*"), 0..8)) {
            let expected = Value::Array(
                elements.iter().map(|element| element.clone().map_or(Value::Null, Value::String)).collect(),
            );
            let result = map_value(&array_of(TypeKind::Text), &render_pg_array(&elements));
            prop_assert!(!result.fallback);
            prop_assert_eq!(result.value, expected);
        }
    }
}
