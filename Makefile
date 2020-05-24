#
# Simple Makefile to call the rust and cargo commands
#

SHELL = /bin/sh
.DEFAULT_GOAL=all

-include .env

build:
	cargo build

run:
	cargo run tests/test_log_file.log

test:
	cargo test

all: test

.PHONY: build run test all
