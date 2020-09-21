##
#  Copyright 2020 OmniSci, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

##
# These functions use ibis and pandas to analyze the omnisci_log_scraper table that is created by the omnisci-log-scraper tool.
##

import os
import pandas as pd
import logging
import ibis


log = logging.getLogger('omnisci_log_scraper')


def log_scraper_starts_and_fails(expr):
    b = expr.filter((expr.severity == 'FATAL')
        | ((expr.severity == 'INFO') & (expr.msg.contains('OmniSci Server 5')))
        )
    b = b.sort_by('logtime')
    b = b.select(['logtime', 'msg', 'logfile'])
    return b


def log_scraper_incomplete_queries(expr):
    endings = expr.filter(expr.event.isin(['sql_execute', 'render_vega'])).select(['sequence'])
    incomplete = expr\
        .filter(expr.event.isin(['sql_execute_begin', 'render_vega_begin'])\
               & expr.sequence.notin(endings.sequence))\
        .sort_by(ibis.desc('logtime'))
    return incomplete.select(['logtime', 'event', 'query', 'sequence'])



# csv from omnisci-log-scraper
def log_scraper_find_failed_query(csv_file):
    df = pd.read_csv(csv_file, index_col=0, usecols=['logtime', 'severity', 'event', 'sequence', 'session', 'operation', 'query', 'msg'])
    # print(df[df.severity == 'FATAL']['msg'])
    last_finished = df[df.event == 'sql_execute'].tail(1).sequence.values[0]
    # last_finished
    failed = df[((df.event == 'sql_execute_begin') & (df.sequence > last_finished))]
    # print(failed)
    failed_query = failed['query'].values[0]
    # print(failed_query)
    return failed_query


def log_scraper_last_complete_before_failure(expr, limit=100):
    failtimes = expr.filter(expr.severity == 'FATAL').select(['logtime'])
    results = []
    for i, fail in failtimes.execute(limit).iterrows():
        # ceil to avoid ibis warning and dropping microseconds
        logtime = fail.logtime.ceil('s')
        # print(i, fail.logtime, logtime)
        
        last_completion = expr.filter((expr.logtime < logtime)
            & expr.event.isin(['sql_execute', 'render_vega']))\
            .sort_by(ibis.desc('logtime'))\
            .select(['logtime', 'event', 'query', 'sequence', 'logfile'])
        # print(last_completion.compile())
        results.append(last_completion.execute(1))
    df = pd.concat(results)
    df.drop_duplicates(inplace=True)
    return df


def log_scraper_last_complete(expr):
    last_completion = expr.filter(
        expr.event.isin(['sql_execute', 'render_vega']))\
        .sort_by(ibis.desc('logtime'))\
        .select(['logtime', 'event', 'query', 'sequence', 'logfile'])
        # print(last_completion.compile())
    return last_completion.execute(1)


def log_scraper_first_incomplete_before_restart(expr, limit=100):
    restart_times = expr.filter(
        ((expr.severity == 'INFO') & (expr.msg.contains('OmniSci Server 5')))
    ).select(['logtime'])
    results = []
    for i, endtime in restart_times.execute(limit).iterrows():
        # ceil to avoid ibis warning and dropping microseconds
        endtime = endtime.logtime.ceil('s')
        
        last_complete_end = expr.filter((expr.logtime < endtime)
            & expr.event.isin(['sql_execute', 'render_vega']))\
            .sort_by(ibis.desc('logtime'))\
            .limit(1)\
            .select(['sequence'])
        
        last_complete_start = expr\
            .filter(expr.event.isin(['sql_execute_begin', 'render_vega_begin'])
                    & expr.sequence.isin(last_complete_end.sequence)
                    & (expr.logtime < endtime))\
            .sort_by(ibis.desc('logtime'))\
            .select(['logtime', 'sequence'])
        
        for i, last_complete_start in last_complete_start.execute(1).iterrows():
            # floor to avoid ibis warning and dropping microseconds
            last_complete_start_time = last_complete_start.logtime.floor('s')

            incomplete = expr\
                .filter(
                    (expr.logtime < endtime)
                    & (expr.logtime > last_complete_start_time)
                    & (expr.sequence != last_complete_start.sequence)
                    & (expr.event.isin(['sql_execute_begin', 'render_vega_begin'])))\
                .sort_by('logtime')\
                .select(['logtime', 'event', 'query', 'sequence', 'logfile'])
            results.append(incomplete.execute(1))
    if results:
        df = pd.concat(results)
        df.drop_duplicates(inplace=True)
        return df
    else:
        return None


def log_scraper_qmd(expr):
    x = expr.filter(expr.msg.contains('Query Memory Descriptor State'))
    x = x.sort_by(ibis.desc('logtime'))
    x = x.select(['msg'])
    df = x.execute(1)
    return df.msg[0]

