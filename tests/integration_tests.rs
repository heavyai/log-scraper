use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

extern crate omnisci_log_scraper;
use omnisci_log_scraper::log_parser;

#[test]
fn test_log_file_parse() {
    let mut test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_path.push("tests/test_log_file.log");
    let f = File::open(test_path.as_path()).unwrap();
    let mut buf_reader = BufReader::new(f);
    let lines = log_parser::parse_log_file(&mut buf_reader);
    assert_eq!(lines.len(), 12);
}
