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
