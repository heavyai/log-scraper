mod log_parser;

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::BufReader;
use std::io::Cursor;

extern crate csv;

extern crate chrono;
use chrono::NaiveDateTime;

extern crate regex;

#[macro_use]
extern crate clap;

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
    let params = clap_app!(myapp =>
        (name: "omnisci-log-scraper")
        (version: "0.1.0")
        (author: "Alex Baden <alex.baden@mapd.com>")
        (about: "Scrapes OmniSci DB logs for useful data")

        (@arg FILTER: -f --filter +takes_value "Filter logs: ")

        (@arg OUTPUT: -o --output +takes_value "Ouput file or DB URL")

        (@arg LOG_FILE: "Input log files")

        (@arg debug: -d ... "Sets the level of debugging information")
    ).get_matches();

    let args: Vec<String> = env::args().collect();
    let input = match args.len() {
        1 => "data/mapd_log/omnisci_server.INFO",
        _ => &args[1],
    };
    let output = params.value_of("output");

    match params.value_of("filter").unwrap_or("sql") {
        "sql" => sql_logs(input, output),
        // TODO "all" => { println!("all"); Ok(()) }
        _ => panic!("filter not recognized")
    }
}

fn sql_logs(input: &str, output: Option<&str>) -> std::io::Result<()> {
    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(input)?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut buf_reader = BufReader::new(buf);
    let lines = log_parser::parse_log_file(&mut buf_reader);
    match output {
        Some(output) => {
            let mut writer = csv::Writer::from_path(&output)?;
            for log_line in lines {
                match QueryWithTiming::new(&log_line) {
                    Some(timing) => {
                        writer.write_record(timing.to_vec())?;
                    }
                    None => (),
                }
            }
            writer.flush()?;
        },
        None => {
            for log_line in lines {
                match QueryWithTiming::new(&log_line) {
                    Some(timing) => println!("{:?}", timing),
                    None => (),
                }
            }
        }
    }
    Ok(())
}
