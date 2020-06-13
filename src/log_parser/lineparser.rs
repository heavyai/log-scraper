use std::io::BufRead;
use std::io::{Error, ErrorKind};
use std::fmt;

extern crate chrono;
use chrono::NaiveDateTime;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;

extern crate csv;

// https://docs.rs/colored/1.9.3/colored/
use colored::Colorize;


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

    pub fn new(log_line: &LogLine) -> Option<QueryWithTiming> {
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

    pub fn print_colorize(&self) -> String {
        format!("{}|{:5.5}| {} |{}|{}\n",
            self.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string().color("grey"),
            self.severity.to_string().color(
                match &self.severity {
                    Severity::FATAL => "red",
                    Severity::ERROR => "red",
                    Severity::WARNING => "red",
                    Severity::INFO => "blue",
                    Severity::DEBUG => "green",
                    Severity::OTHER => "cyan",
                }
            ),
            self.msg,
            self.fileline.color("grey"),
            self.pid.to_string().color("grey"),
        )
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


pub fn transform_logs(input: &str, output: Option<&str>, filter: &Vec<&str>, _format: &str) -> std::io::Result<()> {
    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(input)?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut buf_reader = BufReader::new(buf);
    let lines = parse_log_file(&mut buf_reader);

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
