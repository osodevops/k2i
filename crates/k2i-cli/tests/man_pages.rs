use std::collections::BTreeSet;
use std::fs;
use std::process::Command;

const EXPECTED_PAGES: &[&str] = &[
    "k2i.1",
    "k2i-help.1",
    "k2i-ingest.1",
    "k2i-status.1",
    "k2i-maintenance.1",
    "k2i-maintenance-help.1",
    "k2i-maintenance-compact.1",
    "k2i-maintenance-expire-snapshots.1",
    "k2i-maintenance-clean-orphans.1",
    "k2i-table.1",
    "k2i-table-help.1",
    "k2i-table-register.1",
    "k2i-table-reset.1",
    "k2i-dev.1",
    "k2i-validate.1",
    "k2i-completions.1",
    "k2i-completions-help.1",
    "k2i-completions-bash.1",
    "k2i-completions-zsh.1",
    "k2i-completions-fish.1",
    "k2i-completions-power-shell.1",
    "k2i-completions-man.1",
];

#[test]
fn completions_man_emits_recursive_pages() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_k2i"))
        .args(["completions", "man", "--output-dir"])
        .arg(temp_dir.path())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "man generation failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let mut generated_pages = BTreeSet::new();
    let mut combined = String::new();
    for entry in fs::read_dir(temp_dir.path()).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name().to_string_lossy().to_string();
        let contents = fs::read_to_string(entry.path()).unwrap();
        assert!(
            contents.len() > 100,
            "{file_name} should contain real man-page content"
        );
        generated_pages.insert(file_name);
        combined.push_str(&contents);
        combined.push('\n');
    }

    for page in EXPECTED_PAGES {
        assert!(
            generated_pages.contains(*page),
            "expected generated man page {page}"
        );
    }

    let normalized = combined.replace("\\-", "-");
    for token in normalized.split(|ch: char| ch.is_whitespace() || ch == ',' || ch == '.') {
        if let Some(reference) = token.strip_suffix("(1)") {
            if reference.starts_with("k2i") {
                let page = format!("{reference}.1");
                assert!(
                    generated_pages.contains(&page),
                    "man page reference {reference}(1) should have generated page {page}"
                );
            }
        }
    }
}
