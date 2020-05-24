CREATE TABLE omnisci_log_rust (
    logtime timestamp(9),
    query text,
    sequence int,
    session text,
    execution_time int,
    total_time int,
    database text
);
