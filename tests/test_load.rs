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
 
extern crate omnisci_log_scraper;
use omnisci_log_scraper::log_parser as olog;

#[test]
fn test_load() -> olog::SResult<()> {

    let db = "omnisci://admin:HyperInteractive@localhost:6274/omnisci";

    let mut con = omnisci::client::connect_url(db)?;

    let res = con.sql_execute(String::from("drop table if exists omnisci_log_scraper"), false)?;
    println!("{:?}", res);

    olog::transform_logs(
        "tests/gold/omnisci_server.INFO",
        None,
        &vec!(),
        &olog::OutputType::Load,
        Some(db),
        Some("db"),
    )?;

    // print(pd.read_sql("""copy omnisci_log_scraper from '/src/target/test/omnisci_server.INFO.csv' with (header='true', max_reject=0, threads=1)""", con))

    let res = con.sql_execute(String::from("select count(*) from omnisci_log_scraper"), false)?;
    println!("{:?}", res);

    let res = con.sql_execute(String::from("copy (select * from omnisci_log_scraper) to '/src/target/test/copy_to_omnisci_log_scraper.csv'"), false)?;
    println!("{:?}", res);
    
    Ok(())
}
