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
 
use std::io::Error;

extern crate omnisci_log_scraper;
use omnisci_log_scraper::log_parser as olog;

#[test]
fn test_sql_execute() -> Result<(), Error> {
    let text1 = r#"2020-07-15T08:27:53.512388 I 16 DBHandler.cpp:964 stdlog sql_execute 1 1769 omnisci admin 751-UnTT {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT count(*) from omnisci_states;","tcp:localhost:47712","1764","1768"}"#;
    let mut rec = olog::LogLine::new(text1)?;
    rec.parse_msg();
    assert_eq!(rec.fileline, "DBHandler.cpp:964");
    assert_eq!(rec.username.unwrap(), "admin");
    assert_eq!(rec.event.unwrap(), "sql_execute");
    assert_eq!(rec.query.unwrap(), "SELECT count(*) from omnisci_states;");
    Ok(())
}
