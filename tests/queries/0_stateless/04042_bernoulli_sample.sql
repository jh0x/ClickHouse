-- Tags: no-random-settings

-- Bernoulli sampling for MergeTree tables without SAMPLE BY key

DROP TABLE IF EXISTS t_bernoulli;
DROP TABLE IF EXISTS t_bernoulli_empty;
DROP TABLE IF EXISTS t_bernoulli_memory;
DROP TABLE IF EXISTS t_bernoulli_multi;
DROP TABLE IF EXISTS t_bernoulli_large;

CREATE TABLE t_bernoulli (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli SELECT number FROM numbers(100000);

SELECT 'error without experimental setting';
SELECT count() FROM t_bernoulli SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

SET allow_experimental_bernoulli_sample = 1;

SELECT 'basic sample 0.1';
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample_factor';
SELECT DISTINCT _sample_factor FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'offset without sample by key';
SELECT count() FROM t_bernoulli SAMPLE 0.1 OFFSET 0.5; -- { serverError SAMPLING_NOT_SUPPORTED }
SELECT count() FROM t_bernoulli SAMPLE 50000 OFFSET 10000; -- { serverError SAMPLING_NOT_SUPPORTED }

SELECT 'sample 0 (no-op)';
SELECT count() FROM t_bernoulli SAMPLE 0;

SELECT 'sample 1 (no-op)';
SELECT count() FROM t_bernoulli SAMPLE 1;

SELECT 'absolute sample exceeding table size';
SELECT count() FROM t_bernoulli SAMPLE 200000;

SELECT 'absolute sample 50000';
SELECT count() FROM t_bernoulli SAMPLE 50000 SETTINGS bernoulli_sample_seed = 42;

SELECT 'sample_factor absolute';
SELECT DISTINCT _sample_factor FROM t_bernoulli SAMPLE 50000 SETTINGS bernoulli_sample_seed = 42;

SELECT 'empty table';
CREATE TABLE t_bernoulli_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT count() FROM t_bernoulli_empty SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'parallel replicas sampling_key mode without sample by';
-- In the legacy 'sampling_key' mode, there is no key to split across replicas, so only
-- replica 0 performs Bernoulli sampling while non-first replicas read nothing.
-- With the modern 'read_tasks' mode (default), work is split by mark ranges and each
-- replica applies Bernoulli sampling independently — no such limitation.
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS
    bernoulli_sample_seed = 42,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_mode = 'sampling_key',
    parallel_replicas_count = 2,
    parallel_replica_offset = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS
    bernoulli_sample_seed = 42,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    parallel_replicas_mode = 'sampling_key',
    parallel_replicas_count = 2,
    parallel_replica_offset = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'non-MergeTree engine rejected';
CREATE TABLE t_bernoulli_memory (x UInt64) ENGINE = Memory;
INSERT INTO t_bernoulli_memory SELECT number FROM numbers(100);
SELECT count() FROM t_bernoulli_memory SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

SELECT 'very small probability';
SELECT count() FROM t_bernoulli SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42;

SELECT 'multi-part table';
DROP TABLE IF EXISTS t_bernoulli_multi;
CREATE TABLE t_bernoulli_multi (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_multi SELECT number FROM numbers(50000);
INSERT INTO t_bernoulli_multi SELECT number + 50000 FROM numbers(50000);
INSERT INTO t_bernoulli_multi SELECT number + 100000 FROM numbers(50000);
SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;

SELECT 'thread-count independence';
SELECT
    (SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT count() FROM t_bernoulli SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 4);

SELECT 'multi-part thread-count independence';
SELECT
    (SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT count() FROM t_bernoulli_multi SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42, max_threads = 4);

SELECT 'large table with granule skipping (p=0.001)';
DROP TABLE IF EXISTS t_bernoulli_large;
CREATE TABLE t_bernoulli_large (x UInt64) ENGINE = MergeTree ORDER BY x;
-- 2M rows = ~244 granules at default 8192 granularity.
-- At p=0.001, expected ~2000 hits. Each granule expects ~8 hits,
-- but some will have zero (P ≈ 0.0002), exercising granule skipping.
INSERT INTO t_bernoulli_large SELECT number FROM numbers(2000000);

SELECT 'large count in range';
SELECT count() FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42;

SELECT 'large bit-exact determinism (checksum)';
SELECT
    (SELECT sum(cityHash64(x)) FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42, max_threads = 1)
    =
    (SELECT sum(cityHash64(x)) FROM t_bernoulli_large SAMPLE 0.001 SETTINGS bernoulli_sample_seed = 42, max_threads = 7);

DROP TABLE t_bernoulli_large;

SELECT 'bernoulli with skip index';
DROP TABLE IF EXISTS t_bernoulli_skip;
CREATE TABLE t_bernoulli_skip (x UInt64, y UInt64, INDEX idx_y y TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8192;
INSERT INTO t_bernoulli_skip SELECT number, number FROM numbers(100000);
-- WHERE y >= 50000 eliminates ~half the granules via the skip index.
-- SAMPLE 0.1 with Bernoulli further reduces by ~10x.
SELECT count() FROM t_bernoulli_skip SAMPLE 0.1 WHERE y >= 50000 SETTINGS bernoulli_sample_seed = 42;
DROP TABLE t_bernoulli_skip;

SELECT 'bernoulli with merge table';
DROP TABLE IF EXISTS t_bernoulli_child;
DROP TABLE IF EXISTS t_bernoulli_merge;
CREATE TABLE t_bernoulli_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_child SELECT number FROM numbers(100000);
CREATE TABLE t_bernoulli_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^t_bernoulli_child$');
SELECT count() FROM t_bernoulli_merge SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42;
DROP TABLE t_bernoulli_merge;
DROP TABLE t_bernoulli_child;

SELECT 'prewhere with bernoulli';
SELECT count() FROM t_bernoulli SAMPLE 0.1 PREWHERE x >= 50000 SETTINGS bernoulli_sample_seed = 42;

DROP TABLE t_bernoulli;
DROP TABLE t_bernoulli_empty;
DROP TABLE t_bernoulli_memory;
DROP TABLE t_bernoulli_multi;
