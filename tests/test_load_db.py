import pymapd
import pandas as pd

con = pymapd.connect('omnisci://admin:HyperInteractive@localhost:6274/omnisci')

# OMNISQL="/omnisci/bin/omnisql -q -u admin -p HyperInteractive --db omnisci"
# STREAMIMPORTER="/omnisci/bin/StreamImporter -u admin -p HyperInteractive --database omnisci --print_error --quoted true"

print(pd.read_sql('drop table if exists omnisci_log_rust', con))
with open('/src/create_table-omnisci_log_rust.sql') as f:
    create_table = f.read()
print(pd.read_sql(create_table, con))

print(pd.read_sql("""copy omnisci_log_rust from '/src/target/test/omnisci_server.INFO.csv' with (header='false', max_reject=0, threads=1)""", con))

# StreamImporter is line oriented, so rejects multi-line records
# TODO in lineparser.rs, replace \n with space?
# cargo run tests/omnisci_server.INFO -t csv | ${STREAMIMPORTER} --table omnisci_log_rust

print(pd.read_sql('select count(*) from omnisci_log_rust', con))
print(pd.read_sql("""copy (select * from omnisci_log_rust) to '/src/target/test/copy_to_omnisci_log_rust.csv'""", con))
