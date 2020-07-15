CREATE TABLE omnisci_log_rust (
    logtime TIMESTAMP(6),
    severity TEXT ENCODING DICT(8),
    pid INTEGER,
    fileline TEXT ENCODING DICT(16),
    event TEXT ENCODING DICT(8),
    dur_ms bigint,
    sequence INTEGER,
    session text,
    dbname TEXT ENCODING DICT(16),
    username TEXT ENCODING DICT(16),
    operation TEXT ENCODING DICT(16),
    execution_ms bigint,
    total_ms bigint,
    query text,
    client text,
    msg text,
    name_values TEXT[]
) with (max_rows=640000000);
