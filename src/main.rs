use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::{Error, ErrorKind};
use std::env;

extern crate chrono;
use chrono::{NaiveDateTime};

#[derive(Debug)]
enum LogType {
    INFO,
    ERROR,
    WARNING,
    FATAL,
    OTHER,
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
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Not enough args!");
        std::process::exit(9);
    }

    let file = File::open(&args[1])?;
    let mut buf_reader = BufReader::new(file);
    let mut lines = Vec::<LogLine>::new();
    loop {
        let mut line = String::new();
        let len = buf_reader.read_line(&mut line)?;
        if len == 0 {
            break;
        }
        match LogLine::new(&line) {
            Err(e) => {
                if lines.len() > 0 {
                    lines.last_mut().unwrap().append_msg(&line)
                } else {
                    panic!("{}", e)
                }
            }
            Ok(l) => lines.push(l),
        }
        println!("{:?}", lines.last());
    }
    Ok(())
}
