DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier} SYNC;

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE:Identifier};
CREATE TABLE data (id UInt64) ENGINE=MergeTree() ORDER BY id;
CREATE MATERIALIZED VIEW mv1 (id UInt64) ENGINE=MergeTree() ORDER BY id AS SELECT id + 1 AS id FROM data;
INSERT INTO data VALUES (0);
SELECT * FROM mv1;
ALTER TABLE mv1 MODIFY COMMENT 'TEST COMMENT';
RENAME TABLE mv1 TO mv2;
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC;

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE:Identifier};
CREATE TABLE data (id UInt64) ENGINE=MergeTree() ORDER BY id;
CREATE MATERIALIZED VIEW mv1 (id UInt64) ENGINE=MergeTree() ORDER BY id AS SELECT id + 2 AS id FROM data;
INSERT INTO data VALUES (0);
SELECT * FROM mv1;
ALTER TABLE mv1 MODIFY COMMENT 'TEST COMMENT';
RENAME DATABASE {CLICKHOUSE_DATABASE:Identifier} TO {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE:Identifier};
CREATE TABLE data (id UInt64) ENGINE=MergeTree() ORDER BY id;
CREATE MATERIALIZED VIEW mv1 (id UInt64) ENGINE=MergeTree() ORDER BY id AS SELECT id + 3 AS id FROM data;
INSERT INTO data VALUES (0);
SELECT * FROM mv1;
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC;