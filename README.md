# OmniSci Log Scraper

A collection of utilities and libraries for parsing, collating and converting
OmniSciDB logs into a variety of different representations and formats.

## Building

To build, make sure you first [install rust](https://www.rust-lang.org/tools/install).
The rustup install can be run with `make dev`.

Then, clone the repo and run:

```
cargo build --release
```

The binary will be in `target/release`. 

Alternatively, build the Linux binary using Docker:
```
cd docker
./buildbinary.sh
```

## Usage

```
omnisci-log-scraper 0.1.0
Alex Baden <alex.baden@mapd.com>, Mike Hinchey <mike.hinchey@omnisci.com>
Scrapes OmniSci DB logs for useful data

USAGE:
    omnisci-log-scraper [FLAGS] [OPTIONS] [INPUT]...

FLAGS:
    -d               Debugging information
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --filter <FILTER>    Filter logs: all, sql
    -o, --output <OUTPUT>    Ouput file or DB URL
    -t, --type <TYPE>        Output format: csv, tsv, terminal

ARGS:
    <INPUT>...    Input log files
```
