use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const NODE_VERSION: &str = "v24.16.0";
const NPM_VERSION: &str = "11.13.0";

fn main() {
    println!("cargo:rerun-if-changed=.tool-versions");
    for input in [
        "console/package.json",
        "console/package-lock.json",
        "console/index.html",
        "console/src",
        "console/public",
        "console/tsconfig.json",
        "console/tsconfig.app.json",
        "console/tsconfig.node.json",
        "console/vite.config.ts",
        "console/tailwind.config.js",
        "console/postcss.config.js",
    ] {
        println!("cargo:rerun-if-changed={input}");
    }
    println!("cargo:rerun-if-env-changed=PATH");

    require_version("node", "--version", NODE_VERSION);
    require_version("npm", "--version", NPM_VERSION);

    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let source = root.join("console");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let workspace = out.join("console-build");

    if workspace.exists() {
        fs::remove_dir_all(&workspace).expect("remove previous Console build workspace");
    }
    copy_tree(&source, &workspace).expect("copy Console sources into Cargo output");

    run(&workspace, "npm", &["ci", "--no-audit", "--no-fund"]);
    run(&workspace, "npm", &["run", "build"]);

    let dist = workspace.join("dist");
    if !dist.join("index.html").is_file() {
        panic!(
            "Console build completed without {}",
            dist.join("index.html").display()
        );
    }

    let mut assets = Vec::new();
    collect_files(&dist, &mut assets).expect("enumerate built Console assets");
    assets.sort_by(|left, right| {
        relative_asset_path(&dist, left).cmp(&relative_asset_path(&dist, right))
    });

    let generated = generate_asset_module(&dist, &assets);
    fs::write(out.join("console_assets.rs"), generated)
        .expect("write generated Console asset module");
}

fn require_version(command: &str, flag: &str, expected: &str) {
    let output = Command::new(command)
        .arg(flag)
        .output()
        .unwrap_or_else(|error| panic!("{command} is required to build Tickr Lite: {error}"));
    if !output.status.success() {
        panic!("{command} {flag} failed with {}", output.status);
    }
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if actual != expected {
        panic!("Tickr Lite requires {command} {expected}, found {actual}");
    }
}

fn run(directory: &Path, command: &str, args: &[&str]) {
    let status = Command::new(command)
        .args(args)
        .current_dir(directory)
        .status()
        .unwrap_or_else(|error| panic!("failed to run {command}: {error}"));
    if !status.success() {
        panic!("{command} {} failed with {status}", args.join(" "));
    }
}

fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        if matches!(name.to_str(), Some("node_modules" | "dist")) {
            continue;
        }
        let source_path = entry.path();
        let destination_path = destination.join(name);
        if entry.file_type()?.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn collect_files(directory: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        if entry.file_type()?.is_dir() {
            collect_files(&entry.path(), files)?;
        } else {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn relative_asset_path(root: &Path, asset: &Path) -> String {
    asset
        .strip_prefix(root)
        .expect("asset is under Console dist")
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(OsStr::to_str) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("json" | "map") => "application/json",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn generate_asset_module(root: &Path, assets: &[PathBuf]) -> String {
    let mut generated = String::from(
        "pub(crate) fn resolve(path: &str) -> Option<tickr_api::http::routes::ConsoleAsset> {\n    match path.trim_start_matches('/') {\n",
    );
    for asset in assets {
        let relative = relative_asset_path(root, asset);
        let relative_literal = format!("{relative:?}");
        let absolute_literal = format!("{:?}", asset.to_string_lossy());
        let content_type_literal = format!("{:?}", content_type(asset));
        generated.push_str(&format!(
            "        {relative_literal} => Some(tickr_api::http::routes::ConsoleAsset::new(include_bytes!({absolute_literal}), {content_type_literal})),\n"
        ));
    }
    generated.push_str("        _ => None,\n    }\n}\n");
    generated
}
