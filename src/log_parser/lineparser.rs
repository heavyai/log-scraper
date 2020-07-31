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
use std::error::Error as StdError;

extern crate chrono;
use chrono::NaiveDateTime;

use std::fs;
use std::path::Path;
use std::io;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;

use regex;

extern crate csv;

// https://docs.rs/colored/1.9.3/colored/
use colored::{Colorize, ColoredString};

use serde::Serialize;

use omnisci;
use omnisci::omnisci::TColumn;


// standard result with error boxed so original errors are preserved
// https://doc.rust-lang.org/stable/rust-by-example/error/multiple_error_types/boxing_errors.html
pub type SResult<T> = std::result::Result<T, Box<dyn StdError>>;


const STRING_DICT_MAX_LEN: usize = 32767;

#[derive(Debug, Clone)]
#[derive(Serialize)]
pub enum Severity {
    INFO,
    ERROR,
    WARNING,
    FATAL,
    DEBUG,
    OTHER,
    INPUT,
    AUTH,
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
    pub threadid: Option<i32>,
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

    pub hostname: Option<String>,
    pub logfile: Option<String>,

    // msg_norm is a simple way to match logs by stripping numbers
    pub msg_norm: Option<String>,
}


pub const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS omnisci_log_scraper (
    logtime TIMESTAMP(6),
    severity TEXT ENCODING DICT(8),
    pid INTEGER,
    threadid INTEGER,
    fileline TEXT ENCODING DICT(16),
    event TEXT ENCODING DICT(8),
    dur_ms BIGINT,
    sequence INTEGER,
    session TEXT,
    dbname TEXT ENCODING DICT(16),
    username TEXT ENCODING DICT(16),
    operation TEXT ENCODING DICT(16),
    execution_ms BIGINT,
    total_ms BIGINT,
    query TEXT,
    client TEXT,
    msg TEXT,
    name_values TEXT[],
    hostname TEXT,
    logfile TEXT
    msg_norm TEXT,
) with (max_rows=640000000);
";


enum LogEntry {
    Unknown(String),
    LogLine(LogLine),
    EOF,
}


trait MyColorize {
    fn color(&self, color: &str) -> ColoredString;
}

impl MyColorize for i32 {
    fn color(&self, color: &str) -> ColoredString {
        self.to_string().color(color)
    }
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
        format!("{}|{}|{}|{}|{}|{}|{}|{}| {} |{}|{}|{}|{}|{}|{}\n",
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
            "threadid".color("grey"),
            "session".color("grey"),
            "dbname".color("yellow"),
            "username".color("grey"),
        )
    }

    pub fn print_colorize(&self) -> String {
        format!("{}|{:5.5}|{}|{}|{}|{}|{}|{}| {} |{}|{}|{}|{}|{}|{}\n",
            self.logtime.format("%m-%d %H:%M:%S%.f").to_string().color("grey"),
            self.severity.to_string().color(
                match &self.severity {
                    Severity::FATAL => "red",
                    Severity::ERROR => "red",
                    Severity::WARNING => "red",
                    Severity::INFO => "blue",
                    Severity::DEBUG => "green",
                    Severity::OTHER => "cyan",
                    Severity::INPUT => "grey",
                    Severity::AUTH => "magenta",
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
            self.pid.color("grey"),
            self.threadid.color("grey"),
            self.session.color("grey"),
            self.dbname.color("yellow"),
            self.username.color("grey"),
        )
    }

    // Note, the functions called in parse_msg progressively parse out more values.
    // They quit/return if something is wrong, so the full msg text remains.
    pub fn parse_msg(self: &mut LogLine) {
        if self.stdlog() {
        } else {
            self.msg_norm();
        }
        self.change_severity();
        self.truncate_strings();
    }

    fn msg_norm(self: &mut LogLine) {
        let re_numbers = regex::Regex::new(r"\d+").unwrap();
        let re_singlequoted = regex::Regex::new(r"'.*'").unwrap();

        if self.msg.len() > 0 {
            let norm = re_numbers.replace_all(self.msg.as_ref(), "");
            let norm = re_singlequoted.replace_all(norm.as_ref(), "");
            let mut norm = norm.to_string();
            if norm.len() > 50 {
                norm = norm[..50].to_string();
            }
            self.msg_norm = Some(norm);
        }
    }

    fn truncate_strings(self: &mut LogLine) {
        match &self.query {
            None => (),
            Some(v) =>
            if v.len() >= STRING_DICT_MAX_LEN {
                let a = v[..STRING_DICT_MAX_LEN].to_string();
                if self.msg.is_empty() {
                    let b = if a.len() >= STRING_DICT_MAX_LEN * 2 {
                        a[STRING_DICT_MAX_LEN .. STRING_DICT_MAX_LEN * 2].to_string()
                    } else {
                        a[STRING_DICT_MAX_LEN ..].to_string()
                    };
                    self.msg = b;
                }
                self.query = Some(a);
            }
        };
        if self.msg.len() >= STRING_DICT_MAX_LEN {
            self.msg = self.msg[..STRING_DICT_MAX_LEN].to_string();
        }
    }

    fn change_severity(self: &mut LogLine) {
        // IMO these logs have the wrong severity, so remedying that
        match self.severity {
            Severity::INFO => {
                if self.msg.starts_with("Caught an out-of-gpu-memory error") {
                    self.severity = Severity::ERROR
                }
                else if self.msg.starts_with("ALLOCATION failed to find") {
                    self.severity = Severity::WARNING
                }
                else if self.msg.starts_with("ALLOCATION Attempted slab") {
                    self.severity = Severity::WARNING
                }
                else if self.msg.starts_with("Query ran out of GPU memory, attempting punt to CPU") {
                    self.severity = Severity::WARNING
                }
                else if self.msg.starts_with("Interrupt signal") {
                    // the server is going to be killed, this should be logged FATAL
                    self.severity = Severity::FATAL
                }
                else if self.msg.starts_with("heartbeat thread exiting") {
                    self.severity = Severity::FATAL
                }
                else if self.msg.starts_with("Loader truncated due to reject count") {
                    self.severity = Severity::ERROR
                }
                else if match &self.event {
                    None => false,
                    Some(event) => (event == "connect" || event == "connect_begin" || event == "disconnect" || event == "disconnect_begin")
                } {
                    self.severity = Severity::AUTH
                }
            },
            Severity::WARNING => {
                if self.msg.starts_with("Local login failed") {
                    self.severity = Severity::AUTH
                }
            }
            Severity::ERROR => {
                // INPUT and AUTH are made-up severities
                // INPUT errors are already useful to the user/client, less often to the devops admin
                if self.msg.starts_with("Exception: Parse failed:") {
                    self.severity = Severity::INPUT
                }
                else if self.msg.starts_with("Syntax error at:") {
                    self.severity = Severity::INPUT
                }
                else if self.msg.starts_with("Object with name") {
                    self.severity = Severity::INPUT
                }
                else if self.msg.starts_with("Exception: Exception occurred: org.apache.calcite.runtime.CalciteContextException:") {
                    self.severity = Severity::INPUT
                }
                // AUTH errors should be called out distinctly from software errors
                else if self.msg.starts_with("Authentication failure") {
                    self.severity = Severity::AUTH
                }
                else if self.msg.starts_with("Session not valid.") {
                    self.severity = Severity::AUTH
                }
                else if self.msg.starts_with("Unauthorized Access:") {
                    self.severity = Severity::AUTH
                }
            }
            _ => (),
        };
    }

    fn stdlog(self: &mut LogLine) -> bool {
        let msg_elements: Vec<&str> = self.msg.splitn(8, " ").map(|x| x.trim()).collect();
        if msg_elements.len() < 7 || (msg_elements[0] != "stdlog" && msg_elements[0] != "stdlog_begin") {
            return false
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
        return true
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
                        match &self.event {
                            None => None,
                            Some(event) => if event == "sql_execute" || event == "sql_execute_begin" {
                                let mut r = String::from(q[..i].to_string());
                                r.make_ascii_uppercase();
                                if r == String::from("WITH") {
                                    Some(String::from("SELECT"))
                                } else {
                                    Some(r)
                                }
                            } else {
                                None
                            }
                        }
                    },
                }
            },
        };
        // all values have been used, so do not keep redundant msg
        self.msg = "".to_string();
        if ! unknown_values.is_empty() {
            self.name_values = Some(unknown_values);
        }
    }

    pub fn new(line_raw: &str) -> Result<LogLine, Error> {
        let parts: Vec<&str> = line_raw.split(" ").map(|x| x.trim()).collect();

        let i = 0;
        if parts[i].len() < 26 {
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
        
        let i = i + 1;
        let severity = match parts[i] {
            "I" => Severity::INFO,
            "E" => Severity::ERROR,
            "W" => Severity::WARNING,
            "F" => Severity::FATAL,
            "1" => Severity::DEBUG,
            _ => Severity::OTHER,
        };
        let i = i + 1;
        let pid: i32 = match parts[i].parse() {
            Ok(n) => n,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to parse pid: \"{}\" ({})", parts[2], e),
                ))
            }
        };
        let mut i = i + 1;
        let threadid: Option<i32> = match parts[i].parse() {
            Ok(n) => Some(n),
            Err(_) => {
                i -= 1;
                None
            }
        };
        let i = i + 1;
        let fileline = parts[i].to_string();
        let i = i + 1;
        let msg = parts[i..].join(" ");
        let result = LogLine{
            logtime,
            severity,
            pid,
            threadid,
            fileline,
            msg,
            msg_norm: None,
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
            hostname: None,
            logfile: None,
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

#[derive(Debug)]
pub enum OutputType {
    CSV,
    TSV,
    Terminal,
    SQL,
    Execute,
    Load,
}

impl fmt::Display for OutputType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl OutputType {
    pub fn new(name: &str) -> OutputType {
        match &name {
            &"csv" => OutputType::CSV,
            &"tsv" => OutputType::TSV,
            &"terminal" => OutputType::Terminal,
            &"sql" => OutputType::SQL,
            &"execute" => OutputType::Execute,
            &"load" => OutputType::Load,
            _ => panic!(format!("Unknown OutputType: '{}'", name))
        }
    }
}

trait LogWriter {
    fn write(&mut self, log: &LogLine) -> SResult<()>;
    fn close(&mut self) -> SResult<()> { Ok(()) }
}

struct CsvFileLogWriter {
    writer: csv::Writer<std::fs::File>,
}

impl LogWriter for CsvFileLogWriter {
    fn write(&mut self, log: &LogLine) -> SResult<()> {
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
    fn write(&mut self, log: &LogLine) -> SResult<()> {
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
    fn write(&mut self, log: &LogLine) -> SResult<()> {
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
    fn write(&mut self, log: &LogLine) -> SResult<()> {
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


struct LogExecutor {
    con: Box<dyn omnisci::client::OmniSciConnection>,
}

impl LogExecutor {
    fn new(db: &str) -> SResult<LogExecutor> {
        let con = omnisci::client::connect_url(db)?;
        return Ok(LogExecutor{con})
    }
}

impl LogWriter for LogExecutor {
    fn write(&mut self, log: &LogLine) -> SResult<()> {
        match &log.event {
            Some(event) => {
                if event == "sql_execute" {
                    match &log.query {
                        None => Ok(()),
                        Some(x) => {
                            println!("sql_execute {}", x);
                            match self.con.sql_execute(x.to_string(), true) {
                                Err(e) => Err(Box::new(e)),
                                Ok(r) => {
                                    match r.success {
                                        None => Err(Box::new(Error::new(ErrorKind::Other, "success=None"))),
                                        Some(x) => if x {
                                            println!("success={:?}, total_time_ms={:?}, execution_time_ms={:?}, query_type={:?}",
                                                r.success, r.total_time_ms, r. execution_time_ms, r.query_type);
                                            Ok(())
                                        } else {
                                            Err(Box::new(Error::new(ErrorKind::Other, "success=false")))
                                        }
                                    }
                                }
                            }
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


struct LogLoader {
    con: Box<dyn omnisci::client::OmniSciConnection>,
    buffer: Vec<LogLine>,
    buf_size: usize,
}

impl LogLoader {
    fn new(db: &str) -> SResult<LogLoader> {
        let mut con = omnisci::client::connect_url(db)?;

        match con.sql_execute(String::from(CREATE_TABLE), false) {
            Err(e) => return Err(Box::new(e)),
            Ok(res) => println!("{:?}", res),
        };

        match con.sql_execute(String::from("select count(*) from omnisci_log_scraper"), false) {
            Err(e) => return Err(Box::new(e)),
            Ok(res) => println!("{:?}", res),
        };
        return Ok(LogLoader{con, buffer: vec![], buf_size: 10000})
    }

    fn to_tcolumns(lines: &Vec<LogLine>) -> Vec<TColumn> {
        vec![
            TColumn::from(lines.iter().map(|val| val.logtime.timestamp()).collect::<Vec<i64>>()),
            TColumn::from(lines.iter().map(|val| val.severity.to_string()).collect::<Vec<String>>()),
            TColumn::from(lines.iter().map(|val| val.pid as i64).collect::<Vec<i64>>()),
            TColumn::from(lines.iter().map(|val| val.fileline.to_string()).collect::<Vec<String>>()),
            TColumn::from(lines.iter().map(|val| &val.event).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| val.sequence).collect::<Vec<Option<i32>>>()),
            TColumn::from(lines.iter().map(|val| val.dur_ms).collect::<Vec<Option<i32>>>()),
            TColumn::from(lines.iter().map(|val| &val.session).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| &val.dbname).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| &val.username).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| &val.operation).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| val.execution_time).collect::<Vec<Option<i32>>>()),
            TColumn::from(lines.iter().map(|val| val.total_time).collect::<Vec<Option<i32>>>()),
            TColumn::from(lines.iter().map(|val| &val.query).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| &val.client).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| val.msg.to_string()).collect::<Vec<String>>()),
            TColumn::from(&lines.iter().map(|val| &val.name_values).collect()),
            TColumn::from(lines.iter().map(|val| &val.hostname).collect::<Vec<&Option<String>>>()),
            TColumn::from(lines.iter().map(|val| &val.logfile).collect::<Vec<&Option<String>>>()),
        ]
    }
}

impl LogWriter for LogLoader {
    fn write(&mut self, log: &LogLine) -> SResult<()> {
        if self.buffer.len() < self.buf_size {
            self.buffer.push(log.clone());
            Ok(())
        } else {
            let data = LogLoader::to_tcolumns(&self.buffer);
            self.buffer.clear();
            match self.con.load_table_binary_columnar(&"omnisci_log_scraper".to_string(), data) {
                Ok(ok) => Ok(ok),
                Err(e) => Err(Box::new(e)),
            }
        }
    }

    fn close(&mut self) -> SResult<()> {
        let data = LogLoader::to_tcolumns(&self.buffer);
        self.buffer.clear();
        match self.con.load_table_binary_columnar(&"omnisci_log_scraper".to_string(), data) {
            Ok(ok) => {
                match self.con.sql_execute(String::from("select count(*) from omnisci_log_scraper"), false) {
                    Err(e) => return Err(Box::new(e)),
                    Ok(res) => println!("{:?}", res),
                };
                Ok(ok)
            },
            Err(e) => Err(Box::new(e)),
        }
    }
}

fn output_filename(input: &str, output: &str, extension: &str) -> String {
    let output_path = Path::new(output);
    if output_path.is_dir() {
        // TODO there must be a better way to use the Path api to constuct a new path
        // https://doc.rust-lang.org/std/path/struct.Path.html
        let input_path = Path::new(input);
        let name = input_path.file_name().unwrap().to_str().unwrap();
        format!("{}/{}.{}", output, name, extension)
    } else {
        String::from(output)
    }
}

fn new_log_writer(input: &str, filter: &Vec<&str>, output: Option<&str>, output_type: &OutputType, db: Option<&str>) -> SResult<Box<dyn LogWriter>> {
    match output {
        Some(path) => match output_type {
            OutputType::Terminal => Ok(Box::new(TerminalWriter::new())),
            OutputType::CSV => {
                let x = csv::Writer::from_path(output_filename(input, path, "csv"))?;
                if filter.contains(&"sql") {
                    // TODO write only sql fields
                    Ok(Box::new(CsvFileLogWriter{ writer: x}))
                } else {
                    Ok(Box::new(CsvFileLogWriter{ writer: x}))
                }
            },
            _ => panic!(format!("Output type not supported yet, {}", output_type)), // TODO
        },
        None => match output_type {
            OutputType::Terminal => Ok(Box::new(TerminalWriter::new())),
            OutputType::CSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    // .has_headers(false)
                    .from_writer(io::stdout())
                })),
            OutputType::TSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    .delimiter(b'\t')
                    // .has_headers(false)
                    .from_writer(io::stdout())
                })),
            OutputType::SQL => Ok(Box::new(SqlLogWriter{})),
            OutputType::Execute => match db {
                None => panic!("EXECUTE requires DB URL"),
                Some(db) => Ok(Box::new(LogExecutor::new(db)?)),
            },
            OutputType::Load => match db {
                None => panic!("LOAD requires DB URL"),
                Some(db) => Ok(Box::new(LogLoader::new(db)?)),
            },
        }
    }
}

pub fn transform_logs(input: &str,
        output: Option<&str>,
        filter: &Vec<&str>,
        output_type: &OutputType,
        db: Option<&str>,
        hostname: Option<&str>,
        ) -> SResult<()> {
    // println!("filter {:?} output {:?}", filter, output);

    let query_operations = vec!("SELECT", "WITH");

    let file_contents_utf8 = String::from_utf8_lossy(&fs::read(input)?).into_owned();
    let buf = Cursor::new(&file_contents_utf8);
    let mut reader = BufReader::new(buf);

    let mut writer = new_log_writer(input, filter, output, &output_type, db)?;
    let hostname: Option<String> = match hostname {
        None => None,
        Some(x) => Some(x.to_string())
    };

    for entry in ParsingLine::new(&mut reader) {
        match entry {
            Err(e) => return Err(Box::new(e)),
            Ok(mut log) => {
                // TODO use lifetime to avoid copying these for every line
                log.hostname = hostname.clone();
                log.logfile = Some(input.to_string());
                
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
    writer.close()
}
