#
# Simple Makefile to call the rust and cargo commands
#

SHELL = /bin/sh
.DEFAULT_GOAL=all

OMNISCI_VERSION = v5.3.0
DB_CONTAINER = omnisci-test-db

-include .env

deps:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
.PHONY: deps

build:
	cargo build
.PHONY: build

run:
	cargo run tests/omnisci_server.INFO
.PHONY: run

test:
	cargo test

	mkdir -p target/test
	
	# write various types of output to files in target/test
	cargo run -- -t csv tests/omnisci_server.INFO > target/test/omnisci_server.INFO.csv
	cargo run tests/omnisci_server.INFO -t sql > target/test/omnisci_server.INFO.sql
	cargo run tests/omnisci_server.INFO -f select -t sql > target/test/omnisci_server.INFO-select.sql
	cargo run tests/omnisci_server.INFO -t terminal > target/test/omnisci_server.INFO.terminal.txt

	# then compare them to expected output
	diff tests/gold/omnisci_server.INFO.csv target/test/omnisci_server.INFO.csv
	diff tests/gold/omnisci_server.INFO.sql target/test/omnisci_server.INFO.sql
	diff tests/gold/omnisci_server.INFO-select.sql target/test/omnisci_server.INFO-select.sql
	diff tests/gold/omnisci_server.INFO.terminal.txt target/test/omnisci_server.INFO.terminal.txt
.PHONY: test

# Run this after validating the changes to output in target/test are expected
test_update_gold:
	cp -R target/test/*.* tests/gold/
.PHONY: test_update_gold

install: test
	cargo install --path .
.PHONY: install

release:
	cargo build --release
.PHONY: release

all: test
.PHONY: all

#
# Docker
#

up:
	mkdir -p /tmp/${DB_CONTAINER}
	docker run --name ${DB_CONTAINER} -d --rm \
		-v ${PWD}:/src \
		-v /tmp/${DB_CONTAINER}:/omnisci-storage \
		-p 6273-6274:6273-6274 \
		omnisci/core-os-cpu:${OMNISCI_VERSION}
.PHONY: up

down:
	docker rm -f ${DB_CONTAINER}
.PHONY: down

test_docker:
	tests/test_load_db.sh
.PHONY: test_docker
