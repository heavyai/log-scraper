# Copyright 2020 OmniSci, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Makefile to call the basic rust, cargo and docker commands
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

test_lib:
	mkdir -p target/test
	mkdir -p target/test2
	cargo test
	# diff -d tests/gold/copy_to_omnisci_log_scraper.csv target/test/copy_to_omnisci_log_scraper.csv
	# diff -d tests/gold/copy_to_omnisci_log_scraper.csv target/test2/copy_from_to_omnisci_log_scraper.csv
.PHONY: test_lib

test_gold:
	mkdir -p target/test
	
	# write various types of output to files in target/test
	cargo run -- -t csv --hostname db --output target/test tests/gold/omnisci_server.INFO
	cargo run -- -t sql tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.sql
	cargo run -- -f select -t sql tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO-select.sql
	cargo run -- -t terminal tests/gold/omnisci_server.INFO > target/test/omnisci_server.INFO.terminal.txt

	# then compare them to expected output
	diff tests/gold/omnisci_server.INFO.csv target/test/omnisci_server.INFO.csv
	diff tests/gold/omnisci_server.INFO.sql target/test/omnisci_server.INFO.sql
	diff tests/gold/omnisci_server.INFO-select.sql target/test/omnisci_server.INFO-select.sql
	diff tests/gold/omnisci_server.INFO.terminal.txt target/test/omnisci_server.INFO.terminal.txt
.PHONY: test_gold

test: test_lib test_gold
.PHONY: test

test_ignored:
	cargo test -- --ignored
.PHONY: test_ignored

test_all: test test_ignored
.PHONY: test_all

# Run this after validating the changes to output in target/test are expected
test_update_gold:
	cp -R target/test/*.* tests/gold/
.PHONY: test_update_gold

install:
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
	docker build -t omnisci-log-scraper-build docker
.PHONY: deps.docker

build.docker: deps.docker
	docker run -i -v ${PWD}:/src -w /src omnisci-log-scraper-build \
		/root/.cargo/bin/cargo build --release --target-dir target/ubuntu
	echo "See target/ubuntu/release/ for the omnisci-log-scraper binary"
.PHONY: build.docker

up: deps.docker
	mkdir -p target/${DB_CONTAINER}
	docker run --name ${DB_CONTAINER} \
		-d --rm \
		-v ${PWD}:/src \
		-v ${PWD}/target/${DB_CONTAINER}:/omnisci-storage \
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
