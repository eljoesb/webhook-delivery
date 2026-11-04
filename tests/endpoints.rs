use webhook_delivery::endpoints::{new_secret, validate_name, validate_url};

#[test]
fn name_is_trimmed_and_required() {
    assert_eq!(validate_name("  billing  ").unwrap(), "billing");
    assert!(validate_name("   ").is_err());
}

#[test]
fn url_must_be_absolute_http() {
    assert!(validate_url("http://127.0.0.1:4000/hook").is_ok());
    assert!(validate_url("https://example.com").is_ok());
    assert!(validate_url("ftp://example.com").is_err());
    assert!(validate_url("not a url").is_err());
    assert!(validate_url("http://").is_err());
}

#[test]
fn secret_is_64_hex_chars_and_unique() {
    let first = new_secret();
    let second = new_secret();
    assert_eq!(first.len(), 64);
    assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(first, second);
}
