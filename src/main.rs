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

mod log_parser;

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;

extern crate csv;

extern crate chrono;
use chrono::NaiveDateTime;

extern crate regex;

#[macro_use]
extern crate clap;

extern crate pager;
use pager::Pager;

use colored;


#[derive(Debug)]
struct QueryWithTiming<'a> {
    timestamp: NaiveDateTime,
    query: String,
    execution_time: i32,
    total_time: i32,
    sequence: i32,
    session: &'a str,
    database: &'a str,
}

impl QueryWithTiming<'_> {
    pub fn to_vec(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        out.push(self.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string());
        out.push(self.query.clone());
        out.push(self.sequence.to_string());
        out.push(self.session.to_string());
        out.push(self.execution_time.to_string());
        out.push(self.total_time.to_string());
        out.push(self.database.to_string());
        return out;
    }

    pub fn new(log_line: &log_parser::LogLine) -> Option<QueryWithTiming> {
        let msg_elements: Vec<&str> = log_line.msg.split(" ").map(|x| x.trim()).collect();
        if msg_elements.len() < 3 || msg_elements[0] != "stdlog" || msg_elements[1] != "sql_execute" {
            return None
        }
        // stdlog sql_execute 19 911 omnisci admin 410-gxvh {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT COUNT(*) AS n FROM t","http:10.109.0.11","910","911"}
        let sequence: i32 = msg_elements[2].parse().unwrap();
        let database = msg_elements[4];
        let session = msg_elements[6];
        let re = regex::Regex::new(r"(?ms)(?:[^{}]+)\{(.+)\} \{(.+)\}").unwrap();
        let captures = match re.captures(&log_line.msg) {
            None => panic!(format!("{:?}", &log_line.msg)),
            Some(c) => c,
        };
        assert_eq!(captures.len(), 3);
        let keys_str = captures.get(1).unwrap().as_str();
        let values_str = captures.get(2).unwrap().as_str();
        let keys: Vec<&str> = keys_str.split(",").map(|x| x.trim()).collect();
        // Values are trickier, since SQL can have embedded commas. We explicitly split on the pattern "," and rely on the cleanup during array insertion to remove unbalanced quotes.
        let values: Vec<&str> = values_str.split("\",\"").map(|x| x.trim()).collect();
        assert!(
            keys.len() == values.len(),
            format!("\nKeys: {:?}\nValues: {:?}", keys, values)
        );

        let array_iter = keys.iter().zip(values.iter());
        let mut array_data = HashMap::new();
        for (k, v) in array_iter {
            array_data.insert(
                k.trim_start_matches("\"").trim_end_matches("\""),
                v.trim_start_matches("\"").trim_end_matches("\""),
            );
        }
        let query_str: String = array_data.get(&"query_str").unwrap().to_string();
        let execution_time: i32 = match array_data.get(&"execution_time_ms") {
            Some(v) => v.parse().unwrap(),
            None => -1,
        };
        let total_time: i32 = match array_data.get(&"total_time_ms") {
            Some(v) => v.parse().unwrap(),
            None => -1,
        };
        return Some(QueryWithTiming {
            timestamp: log_line.timestamp,
            query: query_str,
            execution_time,
            total_time,
            sequence,
            session,
            database,
        });
    }
}

fn main() -> std::io::Result<()> {
    let mut pager = Pager::new();
    pager.setup();

    let params = clap_app!(myapp =>
        (name: "omnisci-log-scraper")
        (version: "0.1.0")
        (author: "Alex Baden <alex.baden@mapd.com>, Mike Hinchey <mike.hinchey@omnisci.com>")
        (about: "Scrapes OmniSci DB logs for useful data")

        // TODO implement more filter tags: vega, exec, ops, connect, version, failure, error, warning
        (@arg FILTER: -f --filter +takes_value "Filter logs: all, sql")

        // TODO select
        // (@arg SELECT: -s --select +takes_value "Select column sets: all, min, exec, ...")

        // TODO arg input dir

        // TODO arg file index selector: "-1", -5..-1", "..-1"

        // TODO arg output format type: json, load_table, kafka
        (@arg TYPE: -t --type +takes_value "Output format: csv, tsv, terminal")

        (@arg OUTPUT: -o --output +takes_value "Ouput file or DB URL")

        (@arg INPUT: +multiple "Input log files")

        (@arg debug: -d ... "Debugging information")
    ).get_matches();

    let inputs = match params.indices_of("INPUT") {
        None => vec!("data/mapd_log/omnisci_server.INFO".to_string()),
        Some(indices) => {
            let args: Vec<String> = env::args().collect();
            let mut vec = Vec::new();
            for i in indices {
                vec.push(args[i].to_string());
            };
            vec
        },
    };

    let output = params.value_of("OUTPUT");
    let filter = match params.value_of("FILTER") {
        None => "all",
        Some(x) => x,
    };
    let filter: Vec<&str> = filter.split(",").map(|x| x.trim()).collect();

    let format_type = match params.value_of("TYPE") {
        None => if pager.is_on() {
            // since we know we're printing to terminal, force the pager on, so colurs work
            colored::control::set_override(true);
            "terminal"
        } else {
            "csv"
        },
        Some(x) => x,
    };

    for input in inputs {
        match parse_logs(&input, output, &filter, format_type) {
            Ok(_) => continue,
            Err(x) => return Err(x),
        };
    }
    Ok(())
}

fn parse_logs(input: &str, output: Option<&str>, filter: &Vec<&str>, _format: &str) -> std::io::Result<()> {
    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(input)?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut buf_reader = BufReader::new(buf);
    let lines = log_parser::parse_log_file(&mut buf_reader);

    // TODO How do I declare writer for different sources?
    // let mut writer: csv::Writer<&dyn io::Write> = match output {
    //     Some(path) => csv::Writer::from_path(path)?,
    //     None => csv::Writer::from_writer(io::stdout()),
    // }

    match output {
        Some(path) => {
            // TODO tsv output
            println!("output {}", path);
            let mut writer = csv::Writer::from_path(path)?;

            if filter.contains(&"sql") {
                for log_line in lines {
                    match QueryWithTiming::new(&log_line) {
                        Some(timing) => writer.write_record(timing.to_vec())?,
                        None => (),
                    }
                }
            } else {
                for log_line in lines {
                    match writer.write_record(log_line.to_vec()) {
                        Ok(_) => continue,
                        // return Ok on error, assumes the user quit the output early, we don't want to print an error
                        Err(_) => return Ok(())
                    }
                }
            }
            writer.flush()?;
        },
        None => {
            if filter.contains(&"sql") {
                let mut writer = csv::WriterBuilder::new()
                    .delimiter(b'\t')
                    .from_writer(io::stdout());
                for log_line in lines {
                    match QueryWithTiming::new(&log_line) {
                        Some(timing) => {
                            writer.write_record(timing.to_vec())?;
                            // TODO if debug: println!("{:?}", timing)
                        }
                        None => (),
                    }
                }
                writer.flush()?;
            } else {
                let stdout = std::io::stdout();
                let mut writer = stdout.lock();
                // TODO if format=terminal https://docs.rs/colored/1.9.3/colored/
                for line in lines {
                    match writer.write_all(&line.print_colorize().into_bytes()) {
                        Ok(_) => continue,
                        // return Ok on error, assumes the user quit the output early, we don't want to print an error
                        Err(_) => return Ok(())
                    };
                }
            }
        },
    };
    Ok(())
}
