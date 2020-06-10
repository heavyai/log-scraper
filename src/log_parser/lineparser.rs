use std::io::BufRead;
use std::io::{Error, ErrorKind};
use std::fmt;

extern crate chrono;
use chrono::NaiveDateTime;

#[derive(Debug)]
pub enum Severity {
    INFO,
    ERROR,
    WARNING,
    FATAL,
    DEBUG,
    OTHER,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug)]
pub struct LogLine {
    pub timestamp: NaiveDateTime,
    pub severity: Severity,
    pub pid: i32,
    pub fileline: String,
    pub msg: String,
}

impl LogLine {
    pub fn to_vec(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        out.push(self.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string());
        out.push(self.severity.to_string());
        out.push(self.msg.clone());
        out.push(self.fileline.clone());
        out.push(self.pid.to_string());
        return out;
    }

    pub fn new(line_raw: &str) -> Result<LogLine, Error> {
        let parts: Vec<&str> = line_raw.split(" ").map(|x| x.trim()).collect();

        if parts[0].len() < 26 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Line does not start with timestamp: \"{}\"", line_raw),
            ));
        }
        let timestamp = match NaiveDateTime::parse_from_str(parts[0], "%Y-%m-%dT%H:%M:%S%.f") {
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to parse timestamp: \"{}\" ({})", parts[0], e),
                ))
            }
            Ok(t) => t,
        };
        
        let severity = match parts[1] {
            "I" => Severity::INFO,
            "E" => Severity::ERROR,
            "W" => Severity::WARNING,
            "F" => Severity::FATAL,
            "1" => Severity::DEBUG,
            _ => Severity::OTHER,
        };
        let pid: i32 = match parts[2].parse() {
            Ok(n) => n,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to parse pid: \"{}\" ({})", parts[2], e),
                ))
            }
          };
        let fileline = parts[3].to_string();
        let msg = parts[4..].join(" ");
        return Ok(LogLine {
            timestamp,
            severity,
            pid,
            fileline,
            msg,
        });
    }

    pub fn append_msg(&mut self, line_raw: &str) {
        self.msg.push_str("\n");
        self.msg.push_str(&line_raw.trim_end());
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
