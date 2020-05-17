use std::io::BufRead;
use std::io::{Error, ErrorKind};

extern crate chrono;
use chrono::NaiveDateTime;

#[derive(Debug)]
pub enum LogType {
    INFO,
    ERROR,
    WARNING,
    FATAL,
    OTHER,
}

#[derive(Debug)]
pub struct LogLine {
    pub timestamp: NaiveDateTime,
    pub log_type: LogType,
    pub msg: String,
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

pub fn parse_log_file<R: BufRead>(buf_reader: &mut R) -> Vec<LogLine> {
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
    return lines;
}
