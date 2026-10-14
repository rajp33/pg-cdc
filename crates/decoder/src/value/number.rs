use serde_json::{Number, Value};

const MAX_INTEGER_DIGITS: i64 = 131_072;
const MAX_SCALE: i64 = 16_383;

pub(super) fn to_json_number_within(text: &str, budget: &mut usize) -> Option<Value> {
    let plain = plain_decimal(text)?;
    *budget = budget.checked_sub(plain.len().saturating_sub(text.len()))?;
    parse_plain(&plain)
}

fn parse_plain(plain: &str) -> Option<Value> {
    serde_json::from_str::<Number>(plain)
        .ok()
        .map(Value::Number)
}

fn plain_decimal(text: &str) -> Option<String> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if int_part.is_empty()
        || !all_digits(int_part)
        || !all_digits(frac_part)
        || mantissa.ends_with('.')
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let digit_count = i64::try_from(digits.len()).ok()?;
    let point = i64::try_from(int_part.len()).ok()?.checked_add(exponent)?;
    let scale = i64::try_from(frac_part.len())
        .ok()?
        .checked_sub(exponent)?
        .max(0);
    if point > MAX_INTEGER_DIGITS || scale > MAX_SCALE {
        return None;
    }
    let (integer, fraction) = if point <= 0 {
        let zeros = "0".repeat(usize::try_from(-point).ok()?);
        ("0".to_owned(), format!("{zeros}{digits}"))
    } else if point >= digit_count {
        let zeros = "0".repeat(usize::try_from(point - digit_count).ok()?);
        (format!("{digits}{zeros}"), String::new())
    } else {
        let (integer, fraction) = digits.split_at_checked(usize::try_from(point).ok()?)?;
        (integer.to_owned(), fraction.to_owned())
    };
    let trimmed = integer.trim_start_matches('0');
    let integer = if trimmed.is_empty() { "0" } else { trimmed };
    let sign = if negative && digits.bytes().any(|byte| byte != b'0') {
        "-"
    } else {
        ""
    };
    let dot = if fraction.is_empty() { "" } else { "." };
    Some(format!("{sign}{integer}{dot}{fraction}"))
}
