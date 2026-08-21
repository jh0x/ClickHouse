#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the HDF5 format needs libhdf5, which the fast test build leaves out

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The format is experimental, so every invocation has to enable it.
CLICKHOUSE_LOCAL="$CLICKHOUSE_LOCAL --allow_experimental_hdf5_format=1"

DATA_DIR="$CUR_DIR/data_hdf5"
F="$DATA_DIR/test.h5"

echo "=== Layouts ==="

$CLICKHOUSE_LOCAL -q "
SELECT '--- Root-level 1D datasets, schema ---';
DESCRIBE file('$F');
SELECT '--- Root-level 1D datasets ---';
SELECT * FROM file('$F') ORDER BY x;
SELECT '--- Flat group of 1D datasets, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/flat';
SELECT '--- Flat group of 1D datasets ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat';
SELECT '--- Compound dataset, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/compound/data';
SELECT '--- Compound dataset ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound/data';
SELECT '--- Nested group ---';
SELECT * FROM file('$F', 'HDF5') ORDER BY a SETTINGS input_format_hdf5_dataset = '/nested/group';
SELECT '--- Single dataset named with a trailing slash, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/x/';
SELECT '--- Single dataset named with a trailing slash ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/x/';
SELECT '--- Column subset of a compound dataset ---';
SELECT value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound/data';
SELECT '--- Column subset of a flat group ---';
SELECT y FROM file('$F') ORDER BY y SETTINGS input_format_hdf5_dataset = '/flat';
SELECT '--- Empty dataset, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/empty';
SELECT '--- Empty dataset ---';
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/empty';
SELECT '--- Large dataset spanning multiple batches ---';
SELECT count(), min(id), max(id), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large';
"

echo "=== Types ==="

# A variable-length string is the one type the reader does not read straight into the column: it
# reads a pointer per element into an array it sizes per batch, copies the strings out, and hands
# them back to libhdf5. All of that is per batch and per selection, so it needs a dataset that takes
# more than one batch and a selection that is not the identity.
$CLICKHOUSE_LOCAL -q "
SELECT '--- Various numeric types, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/types';
SELECT '--- Various numeric types ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/types';
SELECT '--- Fixed-length strings, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/strings';
SELECT '--- Fixed-length strings ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/strings';
SELECT '--- Variable-length strings, both charsets, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/vlen';
SELECT '--- Variable-length strings, both charsets ---';
SELECT * FROM file('$F') ORDER BY value SETTINGS input_format_hdf5_dataset = '/vlen';
SELECT '--- Variable-length strings across batch boundaries ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/vlen_large', input_format_hdf5_max_block_size = 64;
SELECT count(), sum(length(text)), max(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/vlen_large', input_format_hdf5_max_block_size = 64;
SELECT '--- ... and the rows on either side of a boundary are the right ones ---';
SELECT id, text FROM file('$F') WHERE id IN (1, 64, 65, 128, 129) ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large', input_format_hdf5_max_block_size = 64;
SELECT '--- Variable-length strings under a strided hyperslab ---';
SELECT count(), min(id), max(id), sum(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/vlen_large[0:2:100:1]', optimize_count_from_files = 0;
SELECT '--- Variable-length strings under a blocked hyperslab ---';
SELECT groupArray(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/vlen_large[0:20:5:8]';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/vlen_large[0:20:5:8]', input_format_hdf5_max_block_size = 17;
SELECT count(), sum(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/vlen_large[0:20:5:8]', input_format_hdf5_max_block_size = 17, optimize_count_from_files = 0;
SELECT '--- Variable-length strings in a block wider than a batch, which keeps its offset ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/vlen_large[3:::]', input_format_hdf5_max_block_size = 64;
SELECT count(), min(id), max(id), min(text), sum(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/vlen_large[3:::]', input_format_hdf5_max_block_size = 64, optimize_count_from_files = 0;
SELECT '--- Narrow-precision type: value does not fill its storage ---';
SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/narrow';
SELECT '--- A type whose storage size and native size disagree, schema ---';
DESCRIBE file('$DATA_DIR/unusual_precision.h5');
SELECT '--- A type whose storage size and native size disagree ---';
SELECT * FROM file('$DATA_DIR/unusual_precision.h5') ORDER BY id;
"

echo "=== Compound datasets ==="

# One read per batch fills every column of a compound dataset, through a record the reader lays out
# itself: each member is placed at a computed offset, and every value is copied out of the records a
# record apart. These widths make a member at the wrong offset read its neighbour's bytes - nothing
# is a multiple of anything else, `value` needs eight-byte alignment behind a one-byte and a
# three-byte member, and `text` and `tag` are pointers rather than values.
$CLICKHOUSE_LOCAL -q "
SELECT '--- Members that need padding in the record, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data';
SELECT '--- Members that need padding in the record ---';
SELECT * FROM file('$F') WHERE id IN (1, 2, 64, 65, 200) ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data';
SELECT '--- ... aggregated over every row ---';
SELECT count(), sum(flag), sum(toInt64(small)), sum(value), sum(length(code)), sum(length(text)), sum(length(tag)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', optimize_count_from_files = 0;
SELECT '--- ... across a batch boundary ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', input_format_hdf5_max_block_size = 64;
SELECT count(), sum(length(text)), sum(length(tag)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', input_format_hdf5_max_block_size = 64, optimize_count_from_files = 0;
SELECT '--- ... and the rows on either side of it are the right ones ---';
SELECT id, code, text, tag FROM file('$F') WHERE id IN (64, 65) ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', input_format_hdf5_max_block_size = 64;
SELECT '--- ... a subset of the members, reordered ---';
SELECT * FROM file('$F', 'HDF5', 'value Float64, flag UInt8, text String') ORDER BY value LIMIT 3 SETTINGS input_format_hdf5_dataset = '/compound_mixed/data';
SELECT '--- ... a fixed-length and a variable-length member, reordered ---';
SELECT * FROM file('$F', 'HDF5', 'code FixedString(3), tag String') WHERE tag != '' ORDER BY tag, code LIMIT 3 SETTINGS input_format_hdf5_dataset = '/compound_mixed/data';
SELECT '--- ... a single member, which is read into the column directly ---';
SELECT count(), sum(value) FROM file('$F', 'HDF5', 'value Float64') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', optimize_count_from_files = 0;
SELECT count(), sum(length(text)) FROM file('$F', 'HDF5', 'text String') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', optimize_count_from_files = 0;
SELECT '--- ... under a strided hyperslab ---';
SELECT count(), min(id), max(id), sum(value), sum(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data[0:2:100:1]', optimize_count_from_files = 0;
SELECT '--- ... under a blocked hyperslab ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/compound_mixed/data[0:20:5:8]', input_format_hdf5_max_block_size = 17;
SELECT groupArray(id), sum(value), sum(length(text)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data[0:20:5:8]', input_format_hdf5_max_block_size = 17;
"

echo "=== Filters, chunk cache and byte order ==="

# The filter pipeline is run by libhdf5, so every filter it has built in works. `unknown` is filter
# id 32001 (blosc), which no build of ClickHouse registers; it is recorded as optional, so libhdf5
# stored the data unfiltered and skips it on read instead of looking for a plugin.
#
# libhdf5 keeps decoded chunks per dataset, up to 8 MiB by default, and does not keep a chunk that
# does not fit: it would be decoded again for every block. A filtered dataset whose chunk is larger
# is therefore opened a second time with a cache sized to hold one chunk, and none of that is visible
# in the result. What is pinned here is that the dataset reads correctly however the batches fall -
# the reopen is what would break it.
#
# That cache comes out of one budget for the whole read, so a group cannot multiply it by the number
# of columns: `input_format_hdf5_max_chunk_size` bounds the total. A dataset whose chunk no longer
# fits in what is left keeps the default cache, which costs decoding time and nothing else, so the
# results below are the same at every budget. One chunk of `/big_chunks` is 1200000 * 8 bytes, so a
# budget of 9600000 fits the first dataset by name and leaves nothing for the second.
$CLICKHOUSE_LOCAL -q "
SELECT '--- Deflate, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/deflate';
SELECT '--- Deflate ---';
SELECT count(), min(id), max(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate';
SELECT '--- Shuffle + deflate ---';
SELECT count(), min(id), max(id), sum(id), min(value), max(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/shuffled';
SELECT '--- Shuffle that degenerates to a no-op (1-byte elements, one element per chunk) ---';
SELECT groupArray(i8), groupArray(i64) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/shuffle_edge';
SELECT '--- Filter: fletcher32 ---';
SELECT count(), sum(toInt64(*)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/filters/fletcher32';
SELECT '--- Filter: nbit ---';
SELECT count(), sum(toInt64(*)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/filters/nbit';
SELECT '--- Filter: scaleoffset ---';
SELECT count(), sum(toInt64(*)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/filters/scaleoffset';
SELECT '--- Filter: unknown, recorded as optional and skipped ---';
SELECT count(), sum(toInt64(*)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/filters/unknown';
SELECT '--- ... and all of them together when the group is iterated ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/filters';
SELECT '--- A filtered chunk larger than the default chunk cache, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk';
SELECT '--- A filtered chunk larger than the default chunk cache ---';
SELECT count(), min(id), max(id), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk', optimize_count_from_files = 0;
SELECT '--- ... in many small batches, all of them inside the one chunk ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/big_chunk', input_format_hdf5_max_block_size = 1000;
SELECT count(), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk', input_format_hdf5_max_block_size = 1000, optimize_count_from_files = 0;
SELECT '--- ... under a strided hyperslab ---';
SELECT count(), min(id), max(id), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk[0:1000:100:1]', optimize_count_from_files = 0;
SELECT '--- ... and removing the chunk bound still reads it ---';
SELECT count(), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk', input_format_hdf5_max_chunk_size = 0, optimize_count_from_files = 0;
SELECT '--- A group of two oversized filtered chunks, both within the default budget ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks';
SELECT count(), min(id), max(id), sum(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks', optimize_count_from_files = 0;
SELECT '--- ... with a budget that fits one of the two chunks ---';
SELECT count(), min(id), max(id), sum(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks', input_format_hdf5_max_chunk_size = 9600000, optimize_count_from_files = 0;
SELECT '--- ... in many small batches under that budget ---';
SELECT count(), sum(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks', input_format_hdf5_max_chunk_size = 9600000, input_format_hdf5_max_block_size = 1000, optimize_count_from_files = 0;
SELECT '--- ... under a strided hyperslab and that budget ---';
SELECT count(), min(id), max(id), sum(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks[0:1000:100:1]', input_format_hdf5_max_chunk_size = 9600000, optimize_count_from_files = 0;
SELECT '--- ... reading only the column the budget did not reach ---';
SELECT count(), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks', input_format_hdf5_max_chunk_size = 9600000, optimize_count_from_files = 0;
SELECT '--- ... and removing the bound gives both chunks a cache ---';
SELECT count(), sum(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunks', input_format_hdf5_max_chunk_size = 0, optimize_count_from_files = 0;
SELECT '--- Big-endian contiguous, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/bigendian';
SELECT '--- Big-endian contiguous ---';
SELECT count(), min(id), max(id), sum(id), min(value), max(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/bigendian';
SELECT '--- Big-endian contiguous strided hyperslab ---';
SELECT count(), min(id), max(id), sum(id), min(value), max(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/bigendian[0:2:25:1]';
SELECT '--- Big-endian + shuffle + deflate ---';
SELECT count(), min(id), max(id), sum(id), min(value), max(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/bigendian_compressed';
"

echo "=== Hyperslabs and block size ==="

# The defaults are resolved in the order count = 1, block = (dim - start) / count, stride = block, so
# a spec that names only `count` splits the remaining extent into that many adjacent blocks - it does
# not take `count` single elements one apart. A hyperslab whose block defaults to the whole remaining
# extent used to be returned as one chunk of the entire selection, however large the dataset.
$CLICKHOUSE_LOCAL -q "
SELECT '--- Start only ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[2:::]';
SELECT '--- Start + count + block ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[1::2:1]';
SELECT '--- Stride on a large dataset ---';
SELECT count(), min(id), max(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0:2:100:1]';
SELECT '--- Negative start ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat[-2:::]';
SELECT '--- Cross-batch boundary ---';
SELECT count(), min(id), max(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0:1:68000:1]';
SELECT '--- Compound dataset ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/compound/data[0::2:1]';
SELECT '--- Empty brackets (identity) ---';
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[]';
SELECT '--- All defaults (identity) ---';
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[:::]';
SELECT '--- Adjacent blocks (stride = block) ---';
SELECT groupArray(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0:3:5:3]';
SELECT '--- One block has no stride between blocks, so a stride below it is not an overlap ---';
SELECT groupArray(x) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[1:1:1:3]';
SELECT groupArray(x) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[1::1:3]';
SELECT '--- Count only: blocks cover the extent ---';
SELECT groupArray(x) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[0::2:]';
SELECT '--- Count only, from a nonzero start ---';
SELECT groupArray(x) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[1::2:]';
SELECT '--- Count only, extent not divisible by count, so the tail is dropped ---';
SELECT count(), min(id), max(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0::3:]', optimize_count_from_files = 0;
SELECT '--- A block wider than a batch is split across batches ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/large[0:::]', input_format_hdf5_max_block_size = 65536;
SELECT '--- A block starting past zero is split too, and keeps its offset ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/large[7:::]', input_format_hdf5_max_block_size = 65536;
SELECT count(), min(id), max(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[7:::]', optimize_count_from_files = 0;
SELECT '--- input_format_hdf5_max_block_size sets the number of rows per chunk ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/large', input_format_hdf5_max_block_size = 30000;
SELECT '--- ... and applies to a block wider than a batch as well ---';
SELECT DISTINCT blockSize() AS s FROM file('$F') ORDER BY s SETTINGS input_format_hdf5_dataset = '/large[0:::]', input_format_hdf5_max_block_size = 30000;
"

echo "=== Neither the batch size nor the hyperslab changes what is read ==="

# Every one of these compares a read against the same read done another way, so each prints 1. The
# comparison is a subquery rather than two shell invocations: two processes per pair is what makes a
# test like this expensive, and nothing here needs them to be separate.
$CLICKHOUSE_LOCAL -q "
SELECT '--- A standalone variable-length string dataset, in small batches ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large', input_format_hdf5_max_block_size = 7))
     = (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large'));
SELECT '--- ... under a strided hyperslab ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large[0:2:100:1]', input_format_hdf5_max_block_size = 5))
     = (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large[0:2:100:1]'));
SELECT '--- ... in a block wider than a batch, against the whole dataset ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large[0:::]', input_format_hdf5_max_block_size = 64))
     = (SELECT sipHash64(groupArray(id), groupArray(text)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/vlen_large'));
SELECT '--- A compound dataset, in small batches ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(flag), groupArray(code), groupArray(value), groupArray(small), groupArray(text), groupArray(tag)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data', input_format_hdf5_max_block_size = 7))
     = (SELECT sipHash64(groupArray(id), groupArray(flag), groupArray(code), groupArray(value), groupArray(small), groupArray(text), groupArray(tag)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data'));
SELECT '--- ... under a strided hyperslab ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(flag), groupArray(code), groupArray(value), groupArray(small), groupArray(text), groupArray(tag)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data[0:2:100:1]', input_format_hdf5_max_block_size = 5))
     = (SELECT sipHash64(groupArray(id), groupArray(flag), groupArray(code), groupArray(value), groupArray(small), groupArray(text), groupArray(tag)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/compound_mixed/data[0:2:100:1]'));
SELECT '--- A block covering the whole dataset is the identity selection ---';
SELECT (SELECT sipHash64(groupArray(id)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/large[0:::]'))
     = (SELECT sipHash64(groupArray(id)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/large'));
SELECT '--- A large dataset in batches of 7 ---';
SELECT (SELECT sipHash64(groupArray(id)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/large', input_format_hdf5_max_block_size = 7))
     = (SELECT sipHash64(groupArray(id)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/large'));
SELECT '--- A narrow-precision type decodes to the same values as the standard one ---';
SELECT (SELECT sipHash64(groupArray(id), groupArray(value)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/narrow'))
     = (SELECT sipHash64(groupArray(id), groupArray(value)) FROM (SELECT * FROM file('$F') ORDER BY id SETTINGS input_format_hdf5_dataset = '/plain'));
"

echo "=== Links, and the settings that do not apply to a local file ==="

$CLICKHOUSE_LOCAL -q "
SELECT '--- External link: the rest of its group is readable ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/links/external/own';
SELECT '--- External link in a group that is iterated: skipped, not fatal ---';
SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/links/external';
SELECT '--- Soft link inside the file is followed, schema ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/links/soft';
SELECT '--- Soft link inside the file is followed ---';
SELECT * FROM file('$F') ORDER BY own SETTINGS input_format_hdf5_dataset = '/links/soft';
SELECT '--- Dangling soft link: the rest of its group is readable when named ---';
SELECT * FROM file('$F') ORDER BY own SETTINGS input_format_hdf5_dataset = '/links/dangling/own';
SELECT '--- Dangling soft links in a group that is iterated: skipped, not fatal ---';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/links/dangling';
SELECT '--- ... and the same when the group is read ---';
SELECT * FROM file('$F') ORDER BY own SETTINGS input_format_hdf5_dataset = '/links/dangling';
SELECT '--- The datasets refused by a tight chunk limit read fine at the default one ---';
SELECT count(), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate';
SELECT count(), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/shuffled';
SELECT count(), sum(id) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/shuffled[0:2:30:1]';
SELECT '--- Zero removes the chunk limit ---';
SELECT count(), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate', input_format_hdf5_max_chunk_size = 0;
SELECT '--- A local file is read by path, so the seek settings do not apply to it ---';
SELECT * FROM file('$F') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat', input_format_allow_seeks = 0;
SELECT '--- ... including a compressed dataset ---';
SELECT count(), min(id), max(id), sum(value) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate', input_format_allow_seeks = 0;
"

echo "=== Schema cache ==="

# These queries must run in a single process: the cache distinguishes datasets within one file, which
# is only observable within one session.

echo "--- Different datasets of one file ---"
$CLICKHOUSE_LOCAL -q "
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/flat';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/types';
DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/flat';
"

echo "--- Same schema, different hyperslab ---"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large';
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0:2:100:1]';
SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large';
"

echo "=== Counting without reading ==="

# A query that needs nothing but the count gets it from the dataspace, which is read while the
# datasets are opened, so no chunk is ever decoded. `use_cache_for_count_from_files = 0` keeps the
# count from being answered out of the schema cache instead, and naming the structure keeps schema
# inference - which would also produce the count - from running at all, so what is pinned here is
# the reader's own count. Every case is checked against the same read done row by row.
$CLICKHOUSE_LOCAL -q "
SELECT '--- The whole dataset ---';
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large', use_cache_for_count_from_files = 0;
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large', optimize_count_from_files = 0;
SELECT '--- A strided hyperslab counts the selection, not the dataset ---';
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large[0:2:100:1]', use_cache_for_count_from_files = 0;
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large[0:2:100:1]', optimize_count_from_files = 0;
SELECT '--- ... and so does a block wider than one batch ---';
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large[7:::]', input_format_hdf5_max_block_size = 64, use_cache_for_count_from_files = 0;
SELECT count() FROM file('$F', 'HDF5', 'id Int32') SETTINGS input_format_hdf5_dataset = '/large[7:::]', input_format_hdf5_max_block_size = 64, optimize_count_from_files = 0;
SELECT '--- An empty dataset ---';
SELECT count() FROM file('$F', 'HDF5', 'x Int32') SETTINGS input_format_hdf5_dataset = '/empty', use_cache_for_count_from_files = 0;
SELECT count() FROM file('$F', 'HDF5', 'x Int32') SETTINGS input_format_hdf5_dataset = '/empty', optimize_count_from_files = 0;
"

echo "=== The environment cannot redirect a read ==="

# The reader pins the object layer and the file driver on the property list it passes to `H5Fopen`,
# and the plugin loader is replaced by a stub at link time. All three variables name things libhdf5
# has built in - `pass_through` is a real connector and `core` is a real driver - so without the
# pinning they would be honoured rather than ignored.

echo "--- HDF5_PLUGIN_PATH does not change the result ---"
HDF5_PLUGIN_PATH="$DATA_DIR" $CLICKHOUSE_LOCAL -q "SELECT count(), sum(toInt64(*)) FROM file('$F') SETTINGS input_format_hdf5_dataset = '/filters/unknown'"

echo "--- HDF5_VOL_CONNECTOR cannot redirect the read ---"
HDF5_VOL_CONNECTOR=pass_through $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$F') ORDER BY x"

echo "--- HDF5_DRIVER cannot replace the file driver ---"
HDF5_DRIVER=core $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$F') ORDER BY x"

echo "=== Concurrency ==="

# libhdf5 is built without `H5_HAVE_THREADSAFE`, so one process-wide lock admits a single thread into
# it. That is the contract this pins: concurrent readers are correct, and they serialize. They have
# to overlap inside one process, because the lock is process-wide - separate `clickhouse-local`
# invocations would never contend for it. `max_threads` puts the `UNION ALL` branches on different
# threads, and one iteration is quick enough that they may not overlap, so this runs more than one.
for _ in {1..2}
do
    $CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(id), sum(value) FROM
    (
        SELECT * FROM (SELECT id, value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large')
        UNION ALL SELECT * FROM (SELECT id, value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate')
        UNION ALL SELECT * FROM (SELECT id, value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/shuffled')
        UNION ALL SELECT * FROM (SELECT id, value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/narrow')
        UNION ALL SELECT * FROM (SELECT id, value FROM file('$F') SETTINGS input_format_hdf5_dataset = '/plain')
    )
    SETTINGS max_threads = 8"
done
