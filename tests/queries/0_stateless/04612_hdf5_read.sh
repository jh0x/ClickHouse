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

echo "--- Compressed (deflate) dataset ---"
echo "Schema:"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/compressed.h5') SETTINGS input_format_hdf5_dataset = '/deflate'"
echo "Data:"
$CLICKHOUSE_LOCAL -q "SELECT count(), min(id), max(id), sum(value) FROM file('$DATA_DIR/compressed.h5') SETTINGS input_format_hdf5_dataset = '/deflate'"

echo "--- Hyperslab: start only ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[2:::]'"

echo "--- Hyperslab: start + count + block ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[1::2:1]'"

echo "--- Hyperslab: stride on large dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT count(), min(id), max(id) FROM file('$DATA_DIR/large.h5') SETTINGS input_format_hdf5_dataset = '/[0:2:100:1]'"

echo "--- Hyperslab: negative start ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[-2:::]'"

echo "--- Hyperslab: cross-batch boundary ---"
$CLICKHOUSE_LOCAL -q "SELECT count(), min(id), max(id) FROM file('$DATA_DIR/large.h5') SETTINGS input_format_hdf5_dataset = '/[0:1:68000:1]'"

echo "--- Hyperslab: compound dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/compound/data[0::2:1]'"

echo "--- Hyperslab: empty brackets (identity) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/large.h5') SETTINGS input_format_hdf5_dataset = '/[]'"

echo "--- Hyperslab: all defaults (identity) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/large.h5') SETTINGS input_format_hdf5_dataset = '/[:::]'"

echo "--- Error: hyperslab out of range ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/flat[10:::]'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: malformed hyperslab ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/flat[abc]'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: 2D dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/unsupported.h5') SETTINGS input_format_hdf5_dataset = '/nd/matrix'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: nested compound type ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/unsupported.h5') SETTINGS input_format_hdf5_dataset = '/nested_compound/data'" 2>&1 | grep -o 'INCORRECT_DATA'

echo "--- Error: unsupported type (enum) ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/unsupported.h5') SETTINGS input_format_hdf5_dataset = '/enum/colors'" 2>&1 | grep -o 'INCORRECT_DATA'

echo "--- Error: empty group ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/unsupported.h5') SETTINGS input_format_hdf5_dataset = '/empty_group'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: multi-dimensional hyperslab ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/unsupported.h5') SETTINGS input_format_hdf5_dataset = '/enum/colors[0:::,0:::]'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: column not found in compound ---"
$CLICKHOUSE_LOCAL -q "SELECT nonexistent FROM file('$DATA_DIR/test.h5', 'HDF5', 'nonexistent Int32') SETTINGS input_format_hdf5_dataset = '/compound/data'" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Schema mismatch: Int32 file read as Int8 ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/schema_mismatch.h5', 'HDF5', 'col Int8')" 2>&1 | grep -o 'BAD_ARGUMENTS'

echo "--- Error: nonexistent dataset ---"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_DIR/test.h5') SETTINGS input_format_hdf5_dataset = '/nonexistent'" 2>&1 | grep -o 'INCORRECT_DATA'
