# OmniSci Log Scraper

A collection of utilities and libraries for parsing, collating and converting OmniSciDB logs into a variety of different representations and formats. 

## Building

To build, make sure you first [install rust](https://www.rust-lang.org/tools/install). Then, clone the repo and run:
```
cargo build --release
```

The binary will be in `target/release`. 

## Usage

The program currently accepts two arguments. The first argument is the path to the log file you want to parse. The second is an optional output file to write to, currently in csv format only. The timing information for each SQL query will be written to the CSV file, along with the query, session ID, and timestamp. 