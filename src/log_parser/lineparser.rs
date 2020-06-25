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


mod my_date_format {
    use chrono::{NaiveDateTime};
    use serde::{self, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.f";

    pub fn serialize<S>(
        date: &NaiveDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct LogLine {

    // every log

    #[serde(with = "my_date_format")]
    pub timestamp: NaiveDateTime,

    pub severity: Severity,
    pub pid: i32,
    pub fileline: String,
    pub msg: String,

    // stdlog
    pub query: Option<String>,
    pub operation: Option<String>,
    pub execution_time: Option<i32>,
    pub total_time: Option<i32>,
    pub sequence: Option<i32>,
    pub session: Option<String>,
    pub database: Option<String>,
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

    pub fn print_colorize(&self) -> String {
        format!("{}|{:5.5}|{}|{}|{}|{}|{}|{}| {} |{}|{}\n",
            self.timestamp.format("%m-%d %H:%M:%S%.f").to_string().color("grey"),
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
            self.sequence.color("grey"),
            self.session.color("grey"),
            self.database.color("yellow"),
            self.execution_time.color("green"),
            self.total_time.color("yellow"),

            self.query.color("blue"),
            self.msg,
            self.fileline.color("grey"),
            self.pid.to_string().color("grey"),
        )
    }

    pub fn stdlog(self: &mut LogLine) {
        let msg_elements: Vec<&str> = self.msg.split(" ").map(|x| x.trim()).collect();
        if msg_elements.len() < 3 || msg_elements[0] != "stdlog" || msg_elements[1] != "sql_execute" {
            return
        }
        // stdlog sql_execute 19 911 omnisci admin 410-gxvh {"query_str","client","execution_time_ms","total_time_ms"} {"SELECT COUNT(*) AS n FROM t","http:10.109.0.11","910","911"}
        self.sequence = Some(msg_elements[2].parse().unwrap());
        self.database = Some(msg_elements[4].to_string());
        self.session = Some(msg_elements[6].to_string());

        let re = regex::Regex::new(r"(?ms)(?:[^{}]+)\{(.+)\} \{(.+)\}").unwrap();

        let captures = match re.captures(&self.msg) {
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
        self.query = Some(array_data.get(&"query_str").unwrap().to_string());
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
        self.execution_time = match array_data.get(&"execution_time_ms") {
            Some(v) => match v.parse() {
                Err(_) => None,
                Ok(v) => Some(v),
            },
            None => None,
        };
        self.total_time = match array_data.get(&"total_time_ms") {
            Some(v) => match v.parse() {
                Err(_) => None,
                Ok(v) => Some(v),
            },
            None => None,
        };
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
        let result = LogLine{
            timestamp,
            severity,
            pid,
            fileline,
            msg,
            query: None,
            operation: None,
            execution_time: None,
            total_time: None,
            sequence: None,
            session: None,
            database: None,
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
                                ok.stdlog();
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
                                ok.stdlog();
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

impl LogWriter for TerminalWriter {
    // TODO??? let mut writer = stdout.lock();
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
        match &log.query {
            None => Ok(()),
            Some(x) => {
                println!("{};\n", x);
                Ok(())
            }
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
            OutputType::Terminal => Ok(Box::new(TerminalWriter{ writer: io::stdout() })),
            OutputType::CSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    .from_writer(io::stdout())
                })),
            OutputType::TSV => Ok(Box::new(CsvOutLogWriter{
                writer: csv::WriterBuilder::new()
                    .delimiter(b'\t')
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
                } else if filter.contains(&"sqlquery") {
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
