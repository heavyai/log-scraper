#
# Simple Makefile to call the rust and cargo commands
#

SHELL = /bin/sh
.DEFAULT_GOAL=all

# OMNISCI_VERSION = v5.3.1
# OMNISCI_IMAGE = omnisci/core-os-cpu:${OMNISCI_VERSION}
OMNISCI_IMAGE = omnisci-log-scraper-build
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
	cargo run -- -t csv tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.csv
	cargo run -- -t sql tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.sql
	cargo run -- -f select -t sql tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO-select.sql
	cargo run -- -t terminal tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.terminal.txt

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

deps.docker:
	# cd docker && ./buildbinary.sh
	docker build -q -t omnisci-log-scraper-build docker
.PHONY: deps.docker

build.docker: deps.docker
	docker run -i -v ${PWD}:/src -w /src omnisci-log-scraper-build \
		/root/.cargo/bin/cargo build --release --target-dir target/ubuntu
	echo "See target/ubuntu/release/ for the omnisci-log-scraper binary"
.PHONY: build.docker

up: deps.docker
	mkdir -p /tmp/${DB_CONTAINER}
	docker run --name ${DB_CONTAINER} \
		-d --rm \
		-v ${PWD}:/src \
		-v /tmp/${DB_CONTAINER}:/omnisci-storage \
		-p 6273-6274:6273-6274 \
		${OMNISCI_IMAGE} \
		/omnisci/startomnisci --non-interactive --data /omnisci-storage/data --verbose=true --log-severity=DEBUG4
.PHONY: up

stop:
	docker stop ${DB_CONTAINER}
.PHONY: stop

down:
	docker stop ${DB_CONTAINER} || echo "container not running"
	# docker rm -f ${DB_CONTAINER}
.PHONY: down

test_load:
	cargo run -- -t csv tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.csv
	docker exec -i ${DB_CONTAINER} python3 /src/tests/test_load_db.py
	diff tests/gold/copy_to_omnisci_log_rust.csv target/test/copy_to_omnisci_log_rust.csv
.PHONY: test_load

test_run: down up
	sleep 12
	rm -f target/test/omnisci_server.INFO
	docker exec -i ${DB_CONTAINER} python3 /src/tests/work_to_generate_log.py
.PHONY: test_run
