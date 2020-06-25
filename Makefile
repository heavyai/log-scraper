#
# Simple Makefile to call the rust and cargo commands
#

SHELL = /bin/sh
.DEFAULT_GOAL=all

-include .env

deps:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

build:
	cargo build

run:
	cargo run tests/test_log_file.log

test:
	cargo test
	
	mkdir -p target/test
	
	# write various types of output to files in target/test
	cargo run tests/omnisci_server.INFO -t csv > target/test/omnisci_server.INFO.csv
	cargo run tests/omnisci_server.INFO -t sql > target/test/omnisci_server.INFO.sql
	cargo run tests/omnisci_server.INFO -t terminal > target/test/omnisci_server.INFO.terminal.txt

	# then compare them to expected output
	diff tests/gold/omnisci_server.INFO.csv target/test/omnisci_server.INFO.csv
	diff tests/gold/omnisci_server.INFO.sql target/test/omnisci_server.INFO.sql
	diff tests/gold/omnisci_server.INFO.terminal.txt target/test/omnisci_server.INFO.terminal.txt

# Run this after validating the changes to output in target/test are expected
test_update_gold:
	cp -R target/test/*.* tests/gold/

install: test
	cargo install --path .

release:
	cargo build --release

all: test

.PHONY: dev build run test all install release
