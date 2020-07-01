#!/bin/bash -Eeux

OMNISCI_VERSION=v5.3.0
DB_CONTAINER=omnisci-test-db

OMNISQL="docker exec -i ${DB_CONTAINER} /omnisci/bin/omnisql -q -u admin -p HyperInteractive --db omnisci"
# STREAMIMPORTER="docker exec -i ${DB_CONTAINER} /omnisci/bin/StreamImporter -u admin -p HyperInteractive --database omnisci --print_error --quoted true"

echo "drop table if exists omnisci_log_rust;" | ${OMNISQL}
cat create_table-omnisci_log_rust.sql | ${OMNISQL}

cargo run tests/omnisci_server.INFO -t csv > target/test/omnisci_server.INFO.csv
echo "copy omnisci_log_rust from '/src/target/test/omnisci_server.INFO.csv' with (header='false', max_reject=0, threads=1);" | ${OMNISQL}

# StreamImporter is line oriented, so rejects multi-line records
# TODO in lineparser.rs, replace \n with space?
# cargo run tests/omnisci_server.INFO -t csv | ${STREAMIMPORTER} --table omnisci_log_rust

echo "select count(*) from omnisci_log_rust limit 100;" | ${OMNISQL}
echo "copy (select * from omnisci_log_rust) to '/src/target/test/copy_to_omnisci_log_rust.csv';" | ${OMNISQL}

diff tests/gold/copy_to_omnisci_log_rust.csv target/test/copy_to_omnisci_log_rust.csv
