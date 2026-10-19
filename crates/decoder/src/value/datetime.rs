const MICROS_PER_DAY: i64 = 86_400_000_000;
const DAYS_FROM_1970_TO_2000: i64 = 10_957;

pub(super) fn timestamp(text: &str) -> Option<String> {
    if is_infinite(text) {
        return Some(text.to_owned());
    }
    let (date, time) = text.split_once(' ')?;
    if date.is_empty() || time.is_empty() {
        return None;
    }
    Some(format!("{date}T{time}"))
}

pub(super) fn timestamptz(text: &str) -> Option<String> {
    if is_infinite(text) {
        return Some(text.to_owned());
    }
    let (body, era) = match text.strip_suffix(" BC") {
        Some(body) => (body, " BC"),
        None => (text, ""),
    };
    let (date, time) = body.split_once(' ')?;
    let offset_at = time.rfind(['+', '-'])?;
    let (clock, offset) = time.split_at_checked(offset_at)?;
    if date.is_empty() || clock.is_empty() {
        return None;
    }
    let offset = match offset.len() {
        3 => format!("{offset}:00"),
        6 | 9 => offset.to_owned(),
        _ => return None,
    };
    Some(format!("{date}T{clock}{offset}{era}"))
}

fn is_infinite(text: &str) -> bool {
    text == "infinity" || text == "-infinity"
}

pub fn format_commit_ts(micros_since_2000: i64) -> String {
    let days = micros_since_2000.div_euclid(MICROS_PER_DAY) + DAYS_FROM_1970_TO_2000;
    let micros_of_day = micros_since_2000.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let seconds = micros_of_day / 1_000_000;
    let fraction = micros_of_day % 1_000_000;
    let (hour, minute, second) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if fraction != 0 {
        let digits = format!("{fraction:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out.push_str("+00:00");
    out
}

fn civil_from_days(days_since_1970: i64) -> (i64, i64, i64) {
    let shifted = days_since_1970 + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}
