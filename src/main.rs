use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::Cursor;
use std::io::{Error, ErrorKind};

extern crate chrono;
use chrono::NaiveDateTime;

extern crate regex;

#[derive(Debug)]
enum LogType {
    INFO,
    ERROR,
    WARNING,
    FATAL,
    OTHER,
}

#[derive(Debug)]
struct QueryWithTiming<'a> {
    query: String,
    execution_time: i32,
    total_time: i32,
    sequence: i32,
    session: &'a str,
}

#[derive(Debug)]
struct LogLine {
    timestamp: NaiveDateTime,
    log_type: LogType,
    msg: String,
}

impl LogLine {
    pub fn new(line_raw: &str) -> Result<LogLine, Error> {
        let line_vec: Vec<&str> = line_raw.split(" ").map(|x| x.trim()).collect();
        if line_vec.len() < 3 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Line is too short to parse: \"{}\"", line_raw),
            ));
        }
        let timestamp = match NaiveDateTime::parse_from_str(line_vec[0], "%Y-%m-%dT%H:%M:%S%.f") {
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to parse timestamp: \"{}\" ({})", line_vec[0], e),
                ))
            }
            Ok(t) => t,
        };
        let log_type = match line_vec[1] {
            "I" => LogType::INFO,
            "E" => LogType::ERROR,
            "W" => LogType::WARNING,
            "F" => LogType::FATAL,
            _ => LogType::OTHER,
        };
        let msg = line_vec[2..].join(" ");
        return Ok(LogLine {
            timestamp,
            log_type,
            msg,
        });
    }

    pub fn append_msg(&mut self, line_raw: &str) {
        self.msg.push_str(line_raw);
    }

    pub fn parse_query_timing(&self) -> Option<QueryWithTiming> {
        let msg_elements: Vec<&str> = self.msg.split(" ").map(|x| x.trim()).collect();
        let mut iter = msg_elements.iter();
        match iter.find_map(|&x| match x {
            "stdlog_begin" => Some(false),
            "sql_execute" => Some(true),
            _ => None,
        }) {
            None => return None,
            Some(false) => return None,
            Some(true) => (),
        };
        // 53988 DBHandler.cpp:1039 stdlog sql_execute 19 911 omnisci admin 410-gxvh {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT COUNT(*) AS n FROM t","http:10.109.0.11","910","911"}
        let sequence: i32 = msg_elements[4].parse().unwrap();
        let session = msg_elements[8];
        let re = regex::Regex::new(r"^(?:[^{}]+)\{(.+)\} \{(.+)\}$").unwrap();
        // remove an extra line breaks
        let mut msg_cleaned = String::from(&self.msg);
        msg_cleaned.retain(|c| c != '\n');
        let captures = match re.captures(&msg_cleaned) {
            None => panic!(format!("{:?}", &self.msg)),
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
            query: query_str,
            execution_time,
            total_time,
            sequence,
            session,
        });
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Not enough args!");
        std::process::exit(9);
    }

    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(&args[1])?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut buf_reader = BufReader::new(buf);
    let mut lines = Vec::<LogLine>::new();
    loop {
        let mut line = String::new();
        let len = match buf_reader.read_line(&mut line) {
            Ok(l) => l,
            Err(e) => panic!(format!("Failed to parse line from file: {}", e)),
        };
        if len == 0 {
            break;
        }
        match LogLine::new(&line) {
            Err(e) => {
                if lines.len() > 0 {
                    lines.last_mut().unwrap().append_msg(&line)
                } else {
                    panic!("Failed to process line: {}\n{}", line, e)
                }
            }
            Ok(l) => lines.push(l),
        }
    }
    for log_line in lines {
        match log_line.parse_query_timing() {
            Some(timing) => println!("Timing: {:?}", timing),
            None => (),
        }
    }
    Ok(())
}
