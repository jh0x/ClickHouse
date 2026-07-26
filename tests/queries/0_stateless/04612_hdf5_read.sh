#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_hdf5"

echo "--- Trivial: root-level 1D datasets ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/trivial.h5')"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/trivial.h5') ORDER BY x"

echo "--- Flat group of 1D datasets ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/flat'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat'"

echo "--- Compound dataset ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/compound/data'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/compound/data'"

echo "--- Nested group ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5', 'HDF5') ORDER BY a SETTINGS input_format_hdf5_dataset = '/nested/group'"

echo "--- Various numeric types ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/types'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/types'"

echo "--- Fixed-length strings ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/strings'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/strings'"

echo "--- Column subset of compound dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT value FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/compound/data'"

echo "--- Column subset of flat group ---"
$CLICKHOUSE_LOCAL -q "SELECT y FROM file('$DATA_DIR/test.h5') ORDER BY y SETTINGS input_format_hdf5_dataset = '/flat'"

echo "--- Variable-length strings (UTF-8) ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/vlen_utf8'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY value SETTINGS input_format_hdf5_dataset = '/vlen_utf8'"

echo "--- Variable-length strings (ASCII) ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/vlen_ascii'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_ascii'"

echo "--- Empty dataset ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/empty.h5')"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/empty.h5')"

echo "--- Large dataset spanning multiple batches ---"
$CLICKHOUSE_LOCAL -q "SELECT count(), min(id), max(id), sum(id) FROM file('$DATA_DIR/large.h5')"

echo "--- Schema mismatch: Int32 file read as Int8 ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/schema_mismatch.h5', 'HDF5', 'col Int8')" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: nonexistent dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/nonexistent'" 2>&1 | grep -o 'INCORRECT_DATA'
