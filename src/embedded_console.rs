include!(concat!(env!("OUT_DIR"), "/console_assets.rs"));

#[cfg(test)]
mod tests {
    use super::resolve;

    #[test]
    fn release_binary_contains_console_entrypoint_and_favicon() {
        let index = resolve("index.html").expect("embedded Console index");
        assert_eq!(index.content_type(), "text/html; charset=utf-8");
        assert!(index.bytes().starts_with(b"<!doctype html>"));

        let favicon = resolve("favicon.svg").expect("embedded Tickr favicon");
        assert_eq!(favicon.content_type(), "image/svg+xml");
        assert!(favicon.bytes().windows(4).any(|window| window == b"6.55"));
    }
}
