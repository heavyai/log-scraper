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
 
use std::io::BufRead;
use std::io::{Error, ErrorKind};
use std::fmt;

extern crate chrono;
use chrono::NaiveDateTime;

use std::fs;
use std::io;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;

extern crate csv;

// https://docs.rs/colored/1.9.3/colored/
use colored::{Colorize, ColoredString};

use serde::Serialize;


#[derive(Debug, Clone)]
#[derive(Serialize)]
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


mod serde_date_format {
    use chrono::{NaiveDateTime};
    use serde::{self, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.f";

    pub fn serialize<S>(
        date: &NaiveDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
}


mod serde_vec_format {
    use serde::{self, Serializer};

    pub fn serialize<S>(
        strings: &Option<Vec<String>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where S: Serializer,
    {
        match strings {
            None => serializer.serialize_str(""),
            Some(strings) => {
                let mut sb = "{".to_string();
                let mut first = true;
                for s in strings {
                    if ! first {
                        sb.push_str(",");
                    } else {
                        first = false;
                    }
                    sb.push_str(&s);
                }
                sb.push_str("}");
                serializer.serialize_str(&sb.as_str())
            }
        }
    }
}


#[derive(Serialize, Debug, Clone)]
pub struct LogLine {

    // every log

    #[serde(with = "serde_date_format")]
    pub logtime: NaiveDateTime,

    pub severity: Severity,
    pub pid: i32,
    pub fileline: String,

    // stdlog
    pub event: Option<String>,
    pub sequence: Option<i32>,
    pub dur_ms: Option<i32>,
    pub session: Option<String>,
    pub dbname: Option<String>,
    pub username: Option<String>,
    // sql_execute
    pub operation: Option<String>,
    pub execution_time: Option<i32>,
    pub total_time: Option<i32>,
    pub query: Option<String>,
    pub client: Option<String>,

    pub msg: String,

    #[serde(with = "serde_vec_format")]
    pub name_values: Option<Vec<String>>,
}

enum LogEntry {
    Unknown(String),
    LogLine(LogLine),
    EOF,
}

trait MyColorize {
    fn color(&self, color: &str) -> ColoredString;
}

impl MyColorize for Option<i32> {
    fn color(&self, color: &str) -> ColoredString {
        match self {
            Some(x) => x.to_string(),
            None => "".to_string(),
        }.color(color)
    }
}

impl MyColorize for Option<String> {
    fn color(&self, color: &str) -> ColoredString {
        match self {
            Some(x) => x.to_string(),
            None => "".to_string(),
        }.color(color)
    }
}

impl LogLine {

    pub fn print_colorize_header() -> String {
        format!("{}|{}|{}|{}|{}|{}|{}|{}| {} |{}|{}|{}|{}|{}\n",
            "logtime".color("grey"),
            "severity".color("blue"),
            "event".color("grey"),
            "sequence".color("grey"),
            "dur_ms".color("green"),
            "execution_ms".color("green"),
            "total_ms".color("yellow"),

            "query".color("blue"),
            "msg",
            "fileline".color("grey"),
            "pid".color("grey"),
            "session".color("grey"),
            "dbname".color("yellow"),
            "username".color("grey"),
        )
    }

    pub fn print_colorize(&self) -> String {
        format!("{}|{:5.5}|{}|{}|{}|{}|{}|{}| {} |{}|{}|{}|{}|{}\n",
            self.logtime.format("%m-%d %H:%M:%S%.f").to_string().color("grey"),
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
            self.event.color("grey"),
            self.sequence.color("grey"),
            self.dur_ms.color("green"),
            self.execution_time.color("green"),
            self.total_time.color("yellow"),

            self.query.color("blue"),
            self.msg,
            self.fileline.color("grey"),
            self.pid.to_string().color("grey"),
            self.session.color("grey"),
            self.dbname.color("yellow"),
            self.username.color("grey"),
        )
    }

    // Note, the functions called in parse_msg progressively parse out more values.
    // They quit/return if something is wrong, so the full msg text remains.
    pub fn parse_msg(self: &mut LogLine) {
        self.stdlog();

        // TODO limit query and msg to length 32767
    }

    fn stdlog(self: &mut LogLine) {
        let msg_elements: Vec<&str> = self.msg.splitn(8, " ").map(|x| x.trim()).collect();
        if msg_elements.len() < 7 || (msg_elements[0] != "stdlog" && msg_elements[0] != "stdlog_begin") {
            return
        }
        // stdlog sql_execute 19 911 omnisci admin 410-gxvh {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT COUNT(*) AS n FROM t","http:10.109.0.11","910","911"}
        self.event = Some(match msg_elements[0] {
            "stdlog_begin" => format!("{}_begin", msg_elements[1]),
            "stdlog" => msg_elements[1].to_string(),
            x => format!("{}_{}", msg_elements[1], x),
        });
        self.sequence = Some(msg_elements[2].parse().unwrap());
        self.dur_ms = Some(msg_elements[3].parse().unwrap());
        self.dbname = Some(msg_elements[4].to_string());
        self.username = Some(msg_elements[5].to_string());
        self.session = Some(msg_elements[6].to_string());

        if msg_elements.len() == 8 {
            let remainder = msg_elements[7].to_string();
            self.parse_key_value_arrays(remainder);
        }
    }

    fn parse_key_value_arrays(self: &mut LogLine, remainder: String) {
        let (keys_str, values_str) = match remainder.find('}') {
            None => return,
            Some(i) => (
                String::from(remainder[1 .. i].to_string()),
                String::from(remainder[i+3 .. remainder.len()-1].to_string()),
            )
        };

        let delim = "\",\"";
        let keys: Vec<&str> = keys_str.split(delim).map(|x| x.trim()).collect();

        let mut values: Vec<String> = Vec::new();
        for val in values_str.split("\",\"") {
            let val = val.trim().replace("\"\"", "\"");
            if val.starts_with('"') && values.len() > 0 {
                let mut last = values.pop().unwrap().clone().to_string();
                last.push_str(delim);
                last.push_str(val.as_str());
                values.push(last);
            }
            else {
                values.push(val.to_string());
            }
        }

        if keys.len() != values.len() {
            panic!("{:?} {:?}", keys, values)
            // return
        }

        let array_iter = keys.iter().zip(values.iter());
        let mut unknown_values: Vec<String> = Vec::new();
        for (k, v) in array_iter {
            let key = k.trim_start_matches("\"").trim_end_matches("\"");
            let val = v.trim_start_matches("\"").trim_end_matches("\"");

            if key == "query_str" {
                self.query = Some(val.to_string())
            }
            else if key == "vega_json" {
                self.query = Some(val.to_string())
            }
            else if key == "client" {
                self.client = Some(val.to_string())
            }
            else if key == "execution_time_ms" {
                self.execution_time = match val.parse() {
                    Err(_) => None,
                    Ok(v) => Some(v),
                }
            }
            else if key == "total_time_ms" {
                self.total_time = match val.parse() {
                    Err(_) => None,
                    Ok(v) => Some(v),
                }
            }
            else {
                unknown_values.push(key.to_string());
                unknown_values.push(val.to_string());
            }
        }

        self.operation = match &self.query {
            None => None,
            Some(q) => {
                match q.find(char::is_whitespace) {
                    None => None,
                    Some(i) => {
                        let mut r = String::from(q[..i].to_string());
                        r.make_ascii_uppercase();
                        Some(r)
                    },
                }
            },
        };
        // all values have been used, so do not keep redundant msg
        self.msg = "".to_string();
        if ! unknown_values.is_empty() {
            self.name_values = Some(unknown_values)
        }
    }

    pub fn new(line_raw: &str) -> Result<LogLine, Error> {
        let parts: Vec<&str> = line_raw.split(" ").map(|x| x.trim()).collect();

        if parts[0].len() < 26 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Line does not start with timestamp: \"{}\"", line_raw),
            ));
        }
        let logtime = match NaiveDateTime::parse_from_str(parts[0], "%Y-%m-%dT%H:%M:%S%.f") {
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
        let result = LogLine{
            logtime,
            severity,
            pid,
            fileline,
            msg,
            query: None,
            operation: None,
            event: None,
            execution_time: None,
            total_time: None,
            sequence: None,
            dur_ms: None,
            session: None,
            dbname: None,
            username: None,
            client: None,
            name_values: None,
        };
        return Ok(result)
    }

    pub fn append_msg(&mut self, line_raw: &str) {
        self.msg.push_str("\n");
        self.msg.push_str(&line_raw.trim_end());
    }
}

impl LogEntry {
    fn readline<'a, R: BufRead>(reader: &'a mut R) -> Result<LogEntry, Error> {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("Failed to read: {}", e))),
            Ok(len) => match len {
                0 => return Ok(LogEntry::EOF),
                _ => match LogLine::new(&line) {
                    Err(_) => return Ok(LogEntry::Unknown(line)),
                    Ok(log) => return Ok(LogEntry::LogLine(log)),
                },
            }
        }
    }
}

// We'll use this iterator to parse the lines as the input is read
// https://doc.rust-lang.org/core/iter/index.html#implementing-iterator
pub struct ParsingLine<'a, R: BufRead> {
    reader: &'a mut R,
    ahead: Option<LogLine>,
}

impl<'a, R: BufRead> ParsingLine<'a, R> {
    pub fn new(reader: &'a mut R) -> ParsingLine<'a, R> {
        ParsingLine {
            ahead: None,
            reader: reader,
        }
    }
}

impl<'a, R: BufRead> Iterator for ParsingLine<'a, R> {
    type Item = Result<LogLine, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match LogEntry::readline(self.reader) {
                Err(e) => return Some(Err(e)),
                Ok(log) => match log {
                    LogEntry::EOF => {

                        // TODO Can we act like tail -f by continuing to loop instead of return None?
                        //      This would let the user refresh the pager app.

                        match &self.ahead {
                            None => return None,
                            Some(log) => {
                                let mut ok = log.clone();
                                self.ahead = None;
                                ok.parse_msg();
                                return Some(Ok(ok))
                            },
                        }
                    },
                    LogEntry::Unknown(text) => {
                        match &self.ahead {
                            None => return None,
                            Some(log) => {
                                let mut ok = log.clone();
                                ok.append_msg(&text);
                                self.ahead = Some(ok)
                            },
                        }
                    },
                    LogEntry::LogLine(log) => {
                        match &self.ahead {
                            None => {
                                self.ahead = Some(log);
                            },
                            Some(ahead) => {
                                let mut ok = ahead.clone();
                                self.ahead = Some(log);
                                ok.parse_msg();
                                return Some(Ok(ok))
                            },
                        }
                    },
                },
            }
        }
    }
}

pub enum OutputType {
    CSV,
    TSV,
    Terminal,
    SQL,
}

impl OutputType {
    pub fn new(name: &str) -> OutputType {
        match &name {
            &"csv" => OutputType::CSV,
            &"tsv" => OutputType::TSV,
            &"terminal" => OutputType::Terminal,
            &"sql" => OutputType::SQL,
            _ => panic!(format!("Unknown OutputType: '{}'", name))
        }
    }
}

trait LogWriter {
    fn write(&mut self, log: &LogLine) -> std::io::Result<()>;
}

struct CsvFileLogWriter {
    writer: csv::Writer<std::fs::File>,
}

impl LogWriter for CsvFileLogWriter {
    fn write(&mut self, log: &LogLine) -> std::io::Result<()> {
        match self.writer.serialize(log) {
            Ok(_) => return Ok(()),
            // return Ok on error, assumes the user quit the output early, we don't want to print an error
            Err(_) => return Ok(())
        }
    }
}

struct CsvOutLogWriter {
    writer: csv::Writer<io::Stdout>,
}

impl LogWriter for CsvOutLogWriter {
    fn write(&mut self, log: &LogLine) -> std::io::Result<()> {
        match self.writer.serialize(log) {
            Ok(_) => return Ok(()),
            // return Ok on error, assumes the user quit the output early, we don't want to print an error
            Err(_) => return Ok(())
        }
    }
}

struct TerminalWriter {
    writer: io::Stdout,
}

impl TerminalWriter {
    fn new() -> TerminalWriter {
        let mut w = TerminalWriter{ writer: io::stdout() };
        match w.writer.write_all(&LogLine::print_colorize_header().into_bytes()) {
            _ => w,
        }
    }
}

impl LogWriter for TerminalWriter {
    fn write(&mut self, log: &LogLine) -> std::io::Result<()> {
        match self.writer.write_all(&log.print_colorize().into_bytes()) {
            Ok(_) => return Ok(()),
            // return Ok on error, assumes the user quit the output early, we don't want to print an error
            Err(_) => return Ok(())
        }
    }
}

struct SqlLogWriter {
}

impl LogWriter for SqlLogWriter {
    fn write(&mut self, log: &LogLine) -> std::io::Result<()> {
        // io::stdout().write(log.query)?;
        match &log.event {
            Some(event) => {
                if event == "sql_execute" {
                    match &log.query {
                        None => Ok(()),
                        Some(x) => {
                            if x.ends_with(";") {
                                println!("{}\n", x);
                            } else {
                                println!("{};\n", x);
                            }
                            Ok(())
                        }
                    }
                }
                else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

fn new_log_writer(filter: &Vec<&str>, output: Option<&str>, output_type: &OutputType) -> Result<Box<dyn LogWriter>, Error> {
    match output {
        Some(path) => match csv::Writer::from_path(path) {
            Ok(x) => if filter.contains(&"sql") {
                // TODO write only sql fields
                Ok(Box::new(CsvFileLogWriter{ writer: x}))
            } else {
                Ok(Box::new(CsvFileLogWriter{ writer: x}))
            },
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("Failed to read: {}", e))),
        },
        None => match output_type {
            OutputType::Terminal => Ok(Box::new(TerminalWriter::new())),
            OutputType::CSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(io::stdout())
                })),
            OutputType::TSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    .delimiter(b'\t')
                    .has_headers(false)
                    .from_writer(io::stdout())
                })),
            OutputType::SQL => Ok(Box::new(SqlLogWriter{})),
        }
    }
}

pub fn transform_logs(input: &str, output: Option<&str>, filter: &Vec<&str>, output_type: &OutputType) -> std::io::Result<()> {
    // println!("filter {:?} output {:?}", filter, output);

    let query_operations = vec!("SELECT", "WITH");

    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(input)?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut reader = BufReader::new(buf);
    let mut writer = new_log_writer(filter, output, &output_type)?;

    for entry in ParsingLine::new(&mut reader) {
        match entry {
            Err(e) => return Err(e),
            Ok(log) => {
                if filter.contains(&"sql") {
                    match log.query {
                        None => (),
                        Some(_) => writer.write(&log)?
                    }
                } else if filter.contains(&"select") {
                    match log.query {
                        None => (),
                        Some(_) => match &log.operation {
                            None => (),
                            Some(op) => if query_operations.contains(&&op[0..]) {
                                writer.write(&log)?
                            } else {
                                ()
                            }
                        }
                    }
                } else {
                    writer.write(&log)?
                }
            },
        }
    };
    Ok(())
}
