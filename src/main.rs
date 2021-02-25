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

mod log_parser;

mod kafka;

use std::env;

extern crate regex;

#[macro_use]
extern crate clap;

extern crate pager;
use pager::Pager;

use colored;

// #[macro_use]
extern crate log;


#[tokio::main]
async fn main() -> log_parser::SResult<()> {
    let params = clap_app!(myapp =>
        (name: crate_name!())
        (version: crate_version!())
        (author: "https://github.com/omnisci/log-scraper and https://community.omnisci.com/")
        (about: crate_description!())

        // TODO implement more filter tags: vega, exec, ops, connect, version, failure, error, warning
        (@arg FILTER: -f --filter +takes_value "Filter logs: all, sql, select")

        // TODO select
        // (@arg SELECT: -s --select +takes_value "Select column sets: all, min, exec, ...")

        // TODO arg input dir

        // TODO arg file index selector: "-1", -5..-1", "..-1"

        // TODO arg output format type: json, load_table, kafka
        (@arg TYPE: -t --type +takes_value "Output format: csv, json, tsv, terminal, sql, execute, load (default: terminal)")

        (@arg OUTPUT: -o --output +takes_value "Ouput file, or if a dir, then output files as OUTPUT/INPUT.csv")

        (@arg HOSTNAME: --hostname +takes_value "Hostname to set for the hostname column (optional)")

        (@arg DB: --db +takes_value "OmniSci DB URL, like: omnisci://admin:HyperInteractive@localhost:6274/omnisci")

        (@arg DRYRUN: --dryrun "Do not execute anything")

        (@arg CREATE_TABLE: --createtable "Create table")

        (@arg INPUT: +multiple "Input log files")

        // (@arg debug: -d ... "Debugging information")

        (@arg follow: --follow "Wait forever for appended data")

        (@arg BROKERS: --brokers +takes_value "Kafka broker list in kafka format")
        (@arg GROUP_ID: --groupid +takes_value "Kafka Consumer group id")
        (@arg TOPICS: --topics +takes_value +multiple "Kafka topics")

        (after_help: "EXAMPLES:
    omnisci-log-scraper /var/lib/omnisci/data/mapd_log/omnisci_server.INFO
    omnisci-log-scraper -t csv /var/lib/omnisci/data/mapd_log/omnisci_server.INFO.*.log > log.csv
    omnisci-log-scraper -f select -t sql /var/lib/omnisci/data/mapd_log/omnisci_server.INFO | omnisql")
    ).get_matches();

    env_logger::init();

    if params.is_present("DRYRUN") {
        if params.is_present("CREATE_TABLE") {
            // None => panic!("CREATE_TABLE not implemented yet"),
            println!("{}", log_parser::CREATE_TABLE);
        }
        // TODO continue doing dryrun
        return Ok(())
    }

    let inputs = match params.indices_of("INPUT") {
        None => vec!("data/mapd_log/omnisci_server.INFO".to_string()),
        Some(indices) => {
            let args: Vec<String> = env::args().collect();
            let mut vec = Vec::new();
            for i in indices {
                vec.push(args[i].to_string());
            };
            vec
        },
    };

    let output = params.value_of("OUTPUT");
    let db = params.value_of("DB");
    let hostname = params.value_of("HOSTNAME");

    let filter = match params.value_of("FILTER") {
        None => "all",
        Some(x) => x,
    };
    let filter: Vec<&str> = filter.split(",").map(|x| x.trim()).collect();

    let follow = params.is_present("follow");

    let output_type = match params.value_of("TYPE") {
        None => log_parser::OutputType::Terminal,
        Some(x) => log_parser::OutputType::new(x),
    };

    // TODO if OUTPUT file is set, disable terminal, default to csv
    match output_type {
        log_parser::OutputType::Terminal => if ! follow {
            let mut pager = Pager::new();
            pager.setup();
            if pager.is_on() {
                // since we know we're printing to terminal, force the pager on, so colors work
                colored::control::set_override(true);
            }
        },
        _ => (),
    }

    if params.is_present("BROKERS") {
        kafka::consume_logs_main(
            params.value_of("BROKERS").unwrap(),
            params.value_of("GROUP_ID").unwrap(),
            &params.values_of("TOPICS").unwrap().collect::<Vec<&str>>(),
            output, &output_type, db, hostname,
        ).await?;
    }
    else {
        for input in inputs {
            match log_parser::transform_logs(&input, output, &filter, &output_type, db, hostname, follow) {
                Ok(_) => continue,
                Err(x) => return Err(x),
            };
        }
    }
    Ok(())
}
