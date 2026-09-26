use std::{fs, path::Path};

const SOURCE_EXTENSIONS: &[&str] = &[
    "rs", "ts", "js", "go", "dart", "gleam", "py", "cpp", "cc", "cxx", "h", "hpp",
    "java", "erl", "ex", "exs", "ml", "mli", "cs", "fs", "fsx", "sh", "ps1",
];

fn collect_source(path: &Path, out: &mut String, files: &mut usize) {
    let Ok(meta) = fs::metadata(path) else { return; };
    if meta.is_dir() {
        let name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
        if matches!(name, "target" | "build" | "dist" | "node_modules" | ".git") { return; }
        let Ok(entries) = fs::read_dir(path) else { return; };
        for entry in entries.flatten() { collect_source(&entry.path(), out, files); }
        return;
    }
    let Some(ext) = path.extension().and_then(|v| v.to_str()) else { return; };
    if !SOURCE_EXTENSIONS.contains(&ext) { return; }
    let Ok(text) = fs::read_to_string(path) else { return; };
    *files += 1;
    out.push_str(&text);
    out.push('\n');
}

fn normalized_authority_surface(source: &str) -> String {
    source.chars().filter(|ch| ch.is_ascii_alphanumeric()).flat_map(|ch| ch.to_lowercase()).collect()
}

#[test]
fn every_client_implementation_preserves_the_fencing_token_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("clients");
    let mut checked = 0usize;
    let mut no_source = Vec::new();
    let mut missing_fencing = Vec::new();

    for entry in fs::read_dir(&root).expect("clients directory must exist") {
        let entry = entry.expect("client directory entry must be readable");
        let path = entry.path();
        if !path.is_dir() { continue; }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let mut source = String::new();
        let mut files = 0usize;
        collect_source(&path, &mut source, &mut files);
        if files == 0 {
            no_source.push(name);
            continue;
        }
        checked += 1;
        if !normalized_authority_surface(&source).contains("fencingtoken") {
            missing_fencing.push(name);
        }
    }

    no_source.sort();
    missing_fencing.sort();
    assert!(no_source.is_empty(), "client directories with no implementation source: {}", no_source.join(", "));
    assert!(missing_fencing.is_empty(), "clients that do not expose/preserve a fencing-token field: {}", missing_fencing.join(", "));
    assert!(checked >= 10, "expected a broad polyglot client matrix; checked only {checked}");
    eprintln!("formal client fencing surface OK: {checked} client implementations checked");
}
