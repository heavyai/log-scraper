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

use std::path::Path;
use std::fs::File;
use std::io::{self, BufRead};
use std::io::prelude::*;

const DB_URL: &str = "omnisci://admin:HyperInteractive@localhost:46274/omnisci";


#[test]
#[ignore]
fn test_load() -> olog::SResult<()> {
    let nonce = "test_load";

    let mut con = omnisci::client::connect_url(DB_URL)?;

    let res = con.sql_execute(String::from("drop table if exists omnisci_log_scraper"), false, nonce.to_string())?;
    println!("{:?}", res);

    olog::transform_logs(
        "tests/gold/omnisci_server.INFO",
        None,
        &vec!(),
        &olog::OutputType::Load,
        Some(DB_URL),
        Some("test_load"),
        false,
    )?;

    let res = con.sql_execute(String::from("select count(*) from omnisci_log_scraper where hostname = 'test_load'"), false, nonce.to_string())?;
    println!("{:?}", res);

    let res = con.sql_execute(String::from("copy (select * from omnisci_log_scraper where hostname = 'test_load') to '/src/target/test2/copy_to_omnisci_log_scraper.csv' with (header='true')"), false, nonce.to_string())?;
    println!("{:?}", res);

    let gold_file = std::fs::read_to_string("tests/gold/copy_to_omnisci_log_scraper.csv")?;
    let test_file = std::fs::read_to_string("target/test2/copy_to_omnisci_log_scraper.csv")?;
    
    let gold_lines: Vec<&str> = gold_file.split_terminator('\n').collect();
    let test_lines: Vec<&str> = test_file.split_terminator('\n').collect();
    assert_eq!(gold_lines.len(), test_lines.len());
    for i in 0..gold_lines.iter().len() {
        assert_eq!(gold_lines[i], test_lines[i]);
    }

    Ok(())
}

#[test]
#[ignore]
fn test_copy_from() -> olog::SResult<()> {
    let nonce = "test_copy_from";

    let mut con = omnisci::client::connect_url(DB_URL)?;

    olog::transform_logs(
        "tests/gold/omnisci_server.INFO",
        Some("tests/gold/omnisci_server.INFO.csv"),
        &vec!(),
        &olog::OutputType::CSV,
        None,
        Some("db"),
        false,
    )?;

    let res = con.sql_execute(String::from("drop table if exists omnisci_log_scraper"), false, nonce.to_string())?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from(olog::CREATE_TABLE), false, nonce.to_string())?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from(
        "copy omnisci_log_scraper from '/src/target/test/omnisci_server.INFO.csv' with (header='true', max_reject=0, threads=1)"),
        false, nonce.to_string())?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from("select count(*) from omnisci_log_scraper where hostname = 'db'"), false, nonce.to_string())?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from(
        "copy (select * from omnisci_log_scraper where hostname = 'db') to '/src/target/test/copy_to_omnisci_log_scraper.csv' with (header='true')"),
        false, nonce.to_string())?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());
    
    let gold_file = std::fs::read_to_string("tests/gold/copy_to_omnisci_log_scraper.csv")?;
    let test_file = std::fs::read_to_string("target/test/copy_to_omnisci_log_scraper.csv")?;

    let gold_lines: Vec<&str> = gold_file.split_terminator('\n').collect();
    let test_lines: Vec<&str> = test_file.split_terminator('\n').collect();
    assert_eq!(gold_lines.len(), test_lines.len());
    for i in 0..gold_lines.iter().len() {
        assert_eq!(gold_lines[i], test_lines[i]);
    }

    Ok(())
}

#[test]
#[ignore]
fn gen_logs() -> olog::SResult<()> {
    if ! Path::new("tests/gold/omnisci_server.INFO").exists() {
        work_to_generate_logs()?;
        copy_server_log()?;
    }
    Ok(())
}

fn work_to_generate_logs() -> olog::SResult<()> {
    let nonce = r#"{"chartId":"work_to_generate_logs","dashboardId":100}"#;

    let mut con = omnisci::client::connect_url(DB_URL)?;

    let res = con.sql_execute(String::from("SELECT count(*) from omnisci_states;"), false, nonce.to_string())?;
    println!("{:?}", res);

    let res = con.sql_execute(String::from("select count(*)
    from omnisci_states as s"), false, nonce.to_string())?;
    println!("{:?}", res);

    let vega = r#"
{"width":1002,"height":726,"viewRenderOptions":{"premultipliedAlpha":false},"data":[{"name":"pointmap","sql":"SELECT conv_4326_900913_x(st_xmin(omnisci_geo)) AS x, conv_4326_900913_y(st_ymin(omnisci_geo )) AS y FROM omnisci_states WHERE ((st_xmin(omnisci_geo) is not null\n          AND st_ymin(omnisci_geo ) is not null\n          AND st_xmin(omnisci_geo) >= -178.12315200000032 AND st_xmin(omnisci_geo) <= -67.26987899999968 AND st_ymin(omnisci_geo ) >= -0.8144879012842097 AND st_ymin(omnisci_geo ) <= 61.96302517868901)) LIMIT 10000000","enableHitTesting":false}],"scales":[{"name":"x","type":"linear","domain":[-19828578.576412328,-7488448.674977641],"range":"width"},{"name":"y","type":"linear","domain":[-90671.43229163112,8850380.771762503],"range":"height"},{"name":"pointmap_fillColor","type":"linear","domain":[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1],"range":["rgba(17,95,154,0.475)","rgba(25,132,197,0.5471153846153846)","rgba(34,167,240,0.6192307692307691)","rgba(72,181,196,0.6913461538461538)","rgba(118,198,143,0.7634615384615384)","rgba(166,215,91,0.835576923076923)","rgba(201,229,47,0.85)","rgba(208,238,17,0.85)","rgba(208,244,0,0.85)"],"accumulator":"density","minDensityCnt":"-2ndStdDev","maxDensityCnt":"2ndStdDev","clamp":true}],"projections":[],"marks":[{"type":"symbol","from":{"data":"pointmap"},"properties":{"xc":{"scale":"x","field":"x"},"yc":{"scale":"y","field":"y"},"fillColor":{"scale":"pointmap_fillColor","value":0},"shape":"circle","width":5,"height":5}}]}
    "#.trim();
    match con.render_vega(0, String::from(vega), 0, nonce.to_string()) {
        Ok(res) => println!("{:?}", res),
        // gpu not enabled. we want the error in the DB log
        Err(e) => println!("{:}", e),
    };

    con.disconnect()?;

    Ok(())
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn copy_server_log() -> olog::SResult<()> {
    let output_path = Path::new("target/test/omnisci_server.INFO");
    let mut output = File::create(&output_path)?;

    // Normalize timestamps in log file, so diff is minimized
    if let Ok(lines) = read_lines("target/omnisci-test-db/data/mapd_log/omnisci_server.INFO") {
        let mut c = 0;
        for line in lines {
            if let Ok(line) = line {
                if line.starts_with("20") && line.find(' ') == Some(26) {                    
                    c += 1;
                    output.write_all(format!("2020-07-01T00:00:00.{:06} ", c).as_bytes())?;
                    output.write_all(line[27..].as_bytes())?;
                }
                else {
                    output.write_all(line.as_bytes())?;
                };
                output.write_all(b"\n")?;
            }
        }
    }
    std::fs::copy("target/test/omnisci_server.INFO", "tests/gold/omnisci_server.INFO")?;

    Ok(())
}
