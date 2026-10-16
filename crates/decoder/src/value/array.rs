use std::iter::Peekable;
use std::str::Chars;

const MAX_DIMENSIONS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Node {
    Null,
    Element(String),
    Array(Vec<Node>),
}

pub(super) fn parse(text: &str, delimiter: char) -> Option<Node> {
    let body = strip_dimensions(text)?;
    let mut chars = body.chars().peekable();
    let node = parse_array(&mut chars, delimiter, 1)?;
    chars.next().is_none().then_some(node)
}

fn strip_dimensions(text: &str) -> Option<&str> {
    if !text.starts_with('[') {
        return Some(text);
    }
    let (dimensions, rest) = text.split_once('=')?;
    let well_formed = dimensions.ends_with(']')
        && dimensions
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'[' | b']' | b':' | b'-'));
    well_formed.then_some(rest)
}

fn parse_array(chars: &mut Peekable<Chars<'_>>, delimiter: char, depth: usize) -> Option<Node> {
    if depth > MAX_DIMENSIONS || chars.next()? != '{' {
        return None;
    }
    let mut items = Vec::new();
    if chars.peek() == Some(&'}') {
        chars.next();
        return Some(Node::Array(items));
    }
    loop {
        let item = match chars.peek()? {
            '{' => parse_array(chars, delimiter, depth + 1)?,
            '"' => {
                chars.next();
                Node::Element(parse_quoted(chars)?)
            }
            _ => parse_unquoted(chars, delimiter)?,
        };
        items.push(item);
        match chars.next()? {
            '}' => return Some(Node::Array(items)),
            next if next == delimiter => {}
            _ => return None,
        }
    }
}

fn parse_quoted(chars: &mut Peekable<Chars<'_>>) -> Option<String> {
    let mut out = String::new();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => out.push(chars.next()?),
            other => out.push(other),
        }
    }
}

fn parse_unquoted(chars: &mut Peekable<Chars<'_>>, delimiter: char) -> Option<Node> {
    let mut out = String::new();
    while let Some(&next) = chars.peek() {
        if next == delimiter || next == '}' {
            break;
        }
        if matches!(next, '{' | '"') {
            return None;
        }
        chars.next();
        if next == '\\' {
            out.push(chars.next()?);
        } else {
            out.push(next);
        }
    }
    if out.is_empty() {
        None
    } else if out.eq_ignore_ascii_case("NULL") {
        Some(Node::Null)
    } else {
        Some(Node::Element(out))
    }
}
