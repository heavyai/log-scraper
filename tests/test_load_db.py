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

import pymapd
import pandas as pd

con = pymapd.connect('omnisci://admin:HyperInteractive@localhost:6274/omnisci')

# OMNISQL="/omnisci/bin/omnisql -q -u admin -p HyperInteractive --db omnisci"
# STREAMIMPORTER="/omnisci/bin/StreamImporter -u admin -p HyperInteractive --database omnisci --print_error --quoted true"

print(pd.read_sql('drop table if exists omnisci_log_scraper', con))
with open('/src/target/omnisci_log_scraper.sql') as f:
    create_table = f.read()
print(pd.read_sql(create_table, con))

print(pd.read_sql("""copy omnisci_log_scraper from '/src/target/test/omnisci_server.INFO.csv' with (header='true', max_reject=0, threads=1)""", con))

# StreamImporter is line oriented, so rejects multi-line records
# TODO in lineparser.rs, replace \n with space?
# cargo run tests/omnisci_server.INFO -t csv | ${STREAMIMPORTER} --table omnisci_log_scraper

print(pd.read_sql('select count(*) from omnisci_log_scraper', con))
print(pd.read_sql("""copy (select * from omnisci_log_scraper) to '/src/target/test/copy_to_omnisci_log_scraper.csv'""", con))
