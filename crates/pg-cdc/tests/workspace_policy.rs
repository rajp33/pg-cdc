use std::fs;
use std::path::Path;

#[test]
fn every_member_crate_inherits_workspace_policy() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut checked = 0;
    for entry in fs::read_dir(&crates_dir).unwrap() {
        let manifest = entry.unwrap().path().join("Cargo.toml");
        if !manifest.exists() {
            continue;
        }
        let text = fs::read_to_string(&manifest).unwrap();
        for required in [
            "[lints]\nworkspace = true",
            "license.workspace = true",
            "edition.workspace = true",
            "rust-version.workspace = true",
        ] {
            assert!(
                text.contains(required),
                "{} must contain {required:?}",
                manifest.display()
            );
        }
        checked += 1;
    }
    assert!(
        checked >= 1,
        "no member crates found in {}",
        crates_dir.display()
    );
}

fn macro_calls<'a>(source: &'a str, name: &str) -> Vec<(usize, &'a str)> {
    let mut calls = Vec::new();
    let pattern = format!("{name}!(");
    let mut from = 0;
    while let Some(found) = source[from..].find(&pattern) {
        let start = from + found + pattern.len();
        let bytes = source.as_bytes();
        let (mut depth, mut at, mut quoted) = (1_usize, start, false);
        while depth > 0 {
            let Some(&byte) = bytes.get(at) else {
                break;
            };
            match (byte, quoted) {
                (b'\\', true) => at += 1,
                (b'"', _) => quoted = !quoted,
                (b'(', false) => depth += 1,
                (b')', false) => depth -= 1,
                _ => {}
            }
            at += 1;
        }
        let line = source[..start].lines().count();
        calls.push((line, &source[start..at.saturating_sub(1)]));
        from = at;
    }
    calls
}

fn last_literal(call: &str) -> Option<&str> {
    let end = call.rfind('"')?;
    let start = call[..end].rfind('"')?;
    Some(&call[start + 1..end])
}

fn rust_sources(dir: &Path, found: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn warn_and_error_messages_are_literals_so_error_reports_never_carry_values() {
    let mut sources = Vec::new();
    rust_sources(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );
    let mut interpolated = Vec::new();
    for path in &sources {
        let text = fs::read_to_string(path).unwrap();
        for name in ["warn", "error"] {
            for (line, call) in macro_calls(&text, name) {
                if let Some(message) = last_literal(call)
                    && message.replace("{{", "").contains('{')
                {
                    interpolated.push(format!("{}:{line}: {message}", path.display()));
                }
            }
        }
    }
    assert!(sources.len() > 10, "{sources:?}");
    assert!(interpolated.is_empty(), "{interpolated:#?}");
}
