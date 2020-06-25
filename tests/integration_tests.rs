/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
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
