CREATE TABLE omnisci_log_rust (
    logtime timestamp(9),
    severity text,
    pid int,
    fileline text,
    operation text,
    execution_time int,
    total_time int,
    sequence int,
    session text,
    dbname text,
    query text,
    msg text
) with (max_rows=640000000);
