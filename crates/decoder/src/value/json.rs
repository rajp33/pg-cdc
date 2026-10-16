use serde_json::{Map, Number, Value};

const MAX_DEPTH: usize = 128;

pub fn parse_json(text: &str) -> Option<Value> {
    let mut parser = Parser { text, at: 0 };
    let value = parser.value(MAX_DEPTH)?;
    parser.skip_whitespace();
    (parser.at == text.len()).then_some(value)
}

struct Parser<'a> {
    text: &'a str,
    at: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> bool {
        self.skip_whitespace();
        let found = self.peek() == Some(byte);
        if found {
            self.at += 1;
        }
        found
    }

    fn value(&mut self, depth: usize) -> Option<Value> {
        self.skip_whitespace();
        match self.peek()? {
            b'{' => self.object(depth.checked_sub(1)?),
            b'[' => self.array(depth.checked_sub(1)?),
            b'"' => self.string().map(Value::String),
            b't' => self.word("true", Value::Bool(true)),
            b'f' => self.word("false", Value::Bool(false)),
            b'n' => self.word("null", Value::Null),
            _ => self.number(),
        }
    }

    fn word(&mut self, word: &str, value: Value) -> Option<Value> {
        let end = self.at.checked_add(word.len())?;
        if self.text.get(self.at..end)? != word {
            return None;
        }
        self.at = end;
        Some(value)
    }

    fn number(&mut self) -> Option<Value> {
        let start = self.at;
        while matches!(
            self.peek(),
            Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
        ) {
            self.at += 1;
        }
        serde_json::from_str::<Number>(self.text.get(start..self.at)?)
            .ok()
            .map(Value::Number)
    }

    fn string(&mut self) -> Option<String> {
        let start = self.at;
        self.at += 1;
        loop {
            match self.peek()? {
                b'"' => break,
                b'\\' => self.at += 2,
                _ => self.at += 1,
            }
        }
        self.at += 1;
        serde_json::from_str(self.text.get(start..self.at)?).ok()
    }

    fn object(&mut self, depth: usize) -> Option<Value> {
        self.at += 1;
        let mut map = Map::new();
        if self.eat(b'}') {
            return Some(Value::Object(map));
        }
        loop {
            self.skip_whitespace();
            if self.peek()? != b'"' {
                return None;
            }
            let key = self.string()?;
            if !self.eat(b':') {
                return None;
            }
            let value = self.value(depth)?;
            map.insert(key, value);
            if self.eat(b'}') {
                return Some(Value::Object(map));
            }
            if !self.eat(b',') {
                return None;
            }
        }
    }

    fn array(&mut self, depth: usize) -> Option<Value> {
        self.at += 1;
        let mut items = Vec::new();
        if self.eat(b']') {
            return Some(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            if self.eat(b']') {
                return Some(Value::Array(items));
            }
            if !self.eat(b',') {
                return None;
            }
        }
    }
}
