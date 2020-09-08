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

// #[test]
fn test_load() -> olog::SResult<()> {

    if ! Path::new("target/test/omnisci_server.INFO").exists() {
        work_to_generate_logs()?;
    }

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
        Some("test_load"),
    )?;

    let res = con.sql_execute(String::from("select count(*) from omnisci_log_scraper where hostname = 'test_load'"), false)?;
    println!("{:?}", res);

    let res = con.sql_execute(String::from("copy (select * from omnisci_log_scraper where hostname = 'test_load') to '/src/target/test2/copy_to_omnisci_log_scraper.csv' with (header='true')"), false)?;
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
fn test_copy_from() -> olog::SResult<()> {

    if ! Path::new("target/test/omnisci_server.INFO").exists() {
        work_to_generate_logs()?;
    }

    let db = "omnisci://admin:HyperInteractive@localhost:6274/omnisci";

    let mut con = omnisci::client::connect_url(db)?;

    olog::transform_logs(
        "tests/gold/omnisci_server.INFO",
        Some("tests/gold/omnisci_server.INFO.csv"),
        &vec!(),
        &olog::OutputType::CSV,
        None,
        Some("db"),
    )?;

    let res = con.sql_execute(String::from("drop table if exists omnisci_log_scraper"), false)?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from(olog::CREATE_TABLE), false)?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from("copy omnisci_log_scraper from '/src/target/test/omnisci_server.INFO.csv' with (header='true', max_reject=0, threads=1)"), false)?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from("select count(*) from omnisci_log_scraper where hostname = 'db'"), false)?;
    println!("{:?}", res);
    assert_eq!(true, res.success.unwrap());

    let res = con.sql_execute(String::from("copy (select * from omnisci_log_scraper where hostname = 'db') to '/src/target/test/copy_to_omnisci_log_scraper.csv' with (header='true')"), false)?;
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

fn work_to_generate_logs() -> olog::SResult<()> {

    let db = "omnisci://admin:HyperInteractive@localhost:6274/omnisci";

    let mut con = omnisci::client::connect_url(db)?;

    let res = con.sql_execute(String::from("SELECT count(*) from omnisci_states;"), false)?;
    println!("{:?}", res);

    let res = con.sql_execute(String::from("select count(*)
    from omnisci_states as s"), false)?;
    println!("{:?}", res);

    // # vega='{"widget_id","compression_level","vega_json","nonce","client"} {"8702332822301651","3","{""width"":1002,""height"":726,""viewRenderOptions"":{""premultipliedAlpha"":false},""data"":[{""name"":""pointmap"",""sql"":""SELECT conv_4326_900913_x(st_xmin(omnisci_geo)) AS x, conv_4326_900913_y(st_ymin(omnisci_geo )) AS y FROM omnisci_states WHERE ((st_xmin(omnisci_geo) is not null\n          AND st_ymin(omnisci_geo ) is not null\n          AND st_xmin(omnisci_geo) >= -178.12315200000032 AND st_xmin(omnisci_geo) <= -67.26987899999968 AND st_ymin(omnisci_geo ) >= -0.8144879012842097 AND st_ymin(omnisci_geo ) <= 61.96302517868901)) LIMIT 10000000"",""enableHitTesting"":false}],""scales"":[{""name"":""x"",""type"":""linear"",""domain"":[-19828578.576412328,-7488448.674977641],""range"":""width""},{""name"":""y"",""type"":""linear"",""domain"":[-90671.43229163112,8850380.771762503],""range"":""height""},{""name"":""pointmap_fillColor"",""type"":""linear"",""domain"":[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1],""range"":[""rgba(17,95,154,0.475)"",""rgba(25,132,197,0.5471153846153846)"",""rgba(34,167,240,0.6192307692307691)"",""rgba(72,181,196,0.6913461538461538)"",""rgba(118,198,143,0.7634615384615384)"",""rgba(166,215,91,0.835576923076923)"",""rgba(201,229,47,0.85)"",""rgba(208,238,17,0.85)"",""rgba(208,244,0,0.85)""],""accumulator"":""density"",""minDensityCnt"":""-2ndStdDev"",""maxDensityCnt"":""2ndStdDev"",""clamp"":true}],""projections"":[],""marks"":[{""type"":""symbol"",""from"":{""data"":""pointmap""},""properties"":{""xc"":{""scale"":""x"",""field"":""x""},""yc"":{""scale"":""y"",""field"":""y""},""fillColor"":{""scale"":""pointmap_fillColor"",""value"":0},""shape"":""circle"",""width"":5,""height"":5}}]}","11","http:10.109.0.9"}'
    // # try:
    // #     print(con.render_vega(vega))
    // # except:
    // #     # gpu not enabled
    // #     # we want the error in the log
    // #     pass

    // // Normalize timestamps in log file, so diff is minimized
    // with open('/omnisci-storage/data/mapd_log/omnisci_server.INFO') as src:
    //     # with open('/src/tests/gold/omnisci_server.INFO') as src:
    //     with open('/src/target/test/omnisci_server.INFO', 'w') as tgt:
    //         i = 0
    //         for line in src:
    //             s = line.split(' ', 3)
    //             if len(s) > 2 and len(s[0]) == 26:
    //                 try:
    //                     pd.to_datetime(s[0])
    //                     i += 1
    //                     line = '2020-07-01T00:00:00.{:06} {} {} {}'.format(i, s[1], '16', s[3])
    //                 except:
    //                     pass
    //             tgt.write(line)

    Ok(())
}
