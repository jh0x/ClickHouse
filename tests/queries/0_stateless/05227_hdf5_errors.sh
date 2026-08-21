#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: the HDF5 format needs libhdf5, which the fast test build leaves out
# long: a query that has to fail cannot be batched with others, so each of these cases is a
# `clickhouse-local` process of its own, and there are around forty of them

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The format is experimental, so every invocation has to enable it. The server section below has to
# reach the clickhouse-local check to prove that one still refuses, so the client gets it too.
CLICKHOUSE_LOCAL="$CLICKHOUSE_LOCAL --allow_experimental_hdf5_format=1"
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_hdf5_format=1"

DATA_DIR="$CUR_DIR/data_hdf5"
F="$DATA_DIR/test.h5"

# A query that is expected to fail cannot be batched with others: the error stops the rest of the
# batch, and `--ignore-error` suppresses the message the assertion needs. So each case here is a
# process, which is what makes this test `long` and what keeps it apart from `04612_hdf5.sh`, where
# every query is expected to succeed and a whole section is read by one process.

# One line per case, whether or not the message showed up, and never more than one even when the
# same message is reported twice.
expect_error() {
    local matched
    matched=$($CLICKHOUSE_LOCAL -q "$3" 2>&1 | grep -o "$2" | head -1)
    echo "$1: ${matched:-UNEXPECTED}"
}

expect_error_on_server() {
    local matched
    matched=$($CLICKHOUSE_CLIENT -q "$3" 2>&1 | grep -o "$2" | head -1)
    echo "$1: ${matched:-UNEXPECTED}"
}

echo "=== Errors: hyperslabs ==="

# libhdf5 refuses a hyperslab whose blocks overlap, and so must schema inference, or `count()` would
# report a row count that no read can produce.
expect_error "out of range" 'BAD_ARGUMENTS' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[10:::]'"
expect_error "size overflows" 'count(4611686018427387904) \* block(12) overflows' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[0:8:4611686018427387904:12]'"
expect_error "end overflows" 'stride(9223372036854775807) + block(4) overflows' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[0:9223372036854775807:3:4]'"
expect_error "malformed" 'BAD_ARGUMENTS' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[abc]'"
# A parameter that is a valid `hsize_t` but larger than `Int64` parses, so the message has to say
# that it is out of range rather than that an integer was expected.
expect_error "parameter above Int64 max" 'it must fit into Int64' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/flat[0:1:1:18446744073709551615]'"
expect_error "multi-dimensional" 'BAD_ARGUMENTS' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/enum/colors[0:::,0:::]'"
expect_error "overlapping blocks" 'so the blocks overlap, which is not supported' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large[0:1:5:3]'"
expect_error "zero block size" 'BAD_ARGUMENTS' "SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/large', input_format_hdf5_max_block_size = 0"

echo "=== Errors: unsupported shapes and types ==="

# A compound dataset becomes a column per field, which only works when `input_format_hdf5_dataset`
# names it directly. Reached any other way it would have to become a single `Tuple` column, which the
# reader cannot fill - so schema inference must refuse it instead of describing a schema that no read
# can produce, and the message has to say which path would work.
expect_error "2D dataset" 'has 2 dimensions' "SELECT * FROM file('$F', 'HDF5', 'matrix Float64') SETTINGS input_format_hdf5_dataset = '/nd/matrix'"
expect_error "nested compound type" 'Nested HDF5 compound types are not supported' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/nested_compound/data'"
expect_error "compound dataset in a named group" "Set 'input_format_hdf5_dataset' to '/compound/data'" "DESCRIBE file('$F') SETTINGS input_format_hdf5_dataset = '/compound'"
expect_error "compound dataset at the root" "Set 'input_format_hdf5_dataset' to '/compound'" "DESCRIBE file('$DATA_DIR/compound_root.h5')"
expect_error "unsupported type (enum)" 'NOT_IMPLEMENTED' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/enum/colors'"
expect_error "empty group" 'BAD_ARGUMENTS' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/empty_group'"
expect_error "nonexistent dataset" "Cannot resolve the HDF5 path '/nonexistent' from 'input_format_hdf5_dataset'" "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/nonexistent'"
expect_error "column not found in a compound dataset" 'BAD_ARGUMENTS' "SELECT nonexistent FROM file('$F', 'HDF5', 'nonexistent Int32') SETTINGS input_format_hdf5_dataset = '/compound/data'"
expect_error "column not found in a group" "Column 'nonexistent' not found in HDF5 group '/flat'" "SELECT nonexistent FROM file('$F', 'HDF5', 'nonexistent Int32') SETTINGS input_format_hdf5_dataset = '/flat'"
expect_error "group member that is not a dataset" "Cannot open the HDF5 dataset 'group'" "SELECT * FROM file('$F', 'HDF5', 'group Int32') SETTINGS input_format_hdf5_dataset = '/nested'"
expect_error "Int32 dataset read as Int8" 'BAD_ARGUMENTS' "SELECT * FROM file('$F', 'HDF5', 'x Int8') SETTINGS input_format_hdf5_dataset = '/x'"
expect_error "fixed-length member at the wrong width" 'file has FixedString(3) but query expects FixedString(4)' "SELECT * FROM file('$F', 'HDF5', 'code FixedString(4)') SETTINGS input_format_hdf5_dataset = '/compound_mixed/data'"
expect_error "single dataset, more than one column asked for" 'maps to exactly one column' "SELECT * FROM file('$F', 'HDF5', 'a Int32, b Int32') SETTINGS input_format_hdf5_dataset = '/x'"
expect_error "single dataset named as a different column" "Column 'not_x' not found in HDF5 dataset '/x'" "SELECT * FROM file('$F', 'HDF5', 'not_x Int32') SETTINGS input_format_hdf5_dataset = '/x'"
# An empty dataset must still be compared against its siblings.
expect_error "group whose datasets disagree on length" 'HDF5 datasets in group have different lengths: 0 vs 5' "SELECT * FROM file('$F', 'HDF5', 'a_empty Int32, b_full Int32') SETTINGS input_format_hdf5_dataset = '/mismatched'"

echo "=== Errors: files named by the file being read ==="

# An external link, an external data file and a virtual dataset all name a file inside the file being
# read, which would turn a query over one named file into a read of paths chosen by whoever wrote it.
expect_error "external link, named" 'does not follow external links' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/links/external/linked'"
expect_error "external link, wanted as a column" 'does not follow external links' "SELECT linked FROM file('$F', 'HDF5', 'linked Int32') SETTINGS input_format_hdf5_dataset = '/links/external'"
expect_error "dangling soft link, named" "Cannot resolve the HDF5 path '/links/dangling/dangling'" "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/links/dangling/dangling'"
expect_error "external data file, named" 'does not open external data files' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/external_data/data'"
expect_error "external data file, group iterated" 'does not open external data files' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/external_data'"
expect_error "virtual dataset over another file" 'is a virtual dataset' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/virtual_other/data'"
expect_error "virtual dataset over a dataset of this file" 'is a virtual dataset' "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/virtual_self/data'"

echo "=== Errors: memory limits ==="

# What an HDF5 file makes the reader allocate has to follow from the settings, not from the numbers
# recorded in the file: a chunk is decoded as a whole, and its size is declared by the file. The
# setting bounds what a dataset may declare, refusing it before any of its data is read.
expect_error "chunk over the limit" "is over 'input_format_hdf5_max_chunk_size' (16)" "SELECT * FROM file('$F') SETTINGS input_format_hdf5_dataset = '/deflate', input_format_hdf5_max_chunk_size = 16"
expect_error "chunk over the limit, larger dataset" "is over 'input_format_hdf5_max_chunk_size' (1048576)" "SELECT count() FROM file('$F') SETTINGS input_format_hdf5_dataset = '/big_chunk', input_format_hdf5_max_chunk_size = 1048576, optimize_count_from_files = 0"
expect_error "chunk dimension that does not fit the element size" 'chunk dimension 2305843009213693952 is too large for the element size 8' "SELECT * FROM file('$DATA_DIR/oversized_chunk.h5')"

echo "=== Errors: the input has to be a plain local file ==="

# libhdf5 opens the file by path and follows offsets recorded inside it, so anything that is not a
# local file has to say so rather than read something wrong.

GZ="$CLICKHOUSE_TMP/hdf5_${CLICKHOUSE_DATABASE}.h5.gz"
gzip -c "$F" > "$GZ"
expect_error "compressed input, the same file as .h5.gz" 'reads a regular local file directly' "SELECT * FROM file('$GZ') ORDER BY x SETTINGS input_format_hdf5_dataset = '/flat'"
rm -f "$GZ"

STDIN_ERROR=$($CLICKHOUSE_LOCAL --input-format HDF5 -q "SELECT * FROM table" < "$F" 2>&1 | grep -o 'reads a regular local file directly' | head -1)
echo "input piped on stdin: ${STDIN_ERROR:-UNEXPECTED}"

echo "--- A file locked by another process is still readable ---"
LOCKED="$CLICKHOUSE_TMP/hdf5_locked_${CLICKHOUSE_DATABASE}.h5"
cp "$F" "$LOCKED"
flock -x "$LOCKED" -c "$CLICKHOUSE_BINARY local --allow_experimental_hdf5_format=1 -q \"SELECT * FROM file('$LOCKED') ORDER BY x\""
rm -f "$LOCKED"

echo "=== Errors: only when the experimental setting is enabled ==="

# The setting is a gate of its own, independent of where the format runs: it is off by default, and
# it is checked before anything else, so a user who has not opted in is told how to rather than being
# told about the clickhouse-local restriction that is not what stopped them. A query-level SETTINGS
# clause overrides the command-line flag set at the top of this test.
expect_error "detected from the extension" 'Set `allow_experimental_hdf5_format = 1` to enable it' "SELECT * FROM file('$F') SETTINGS allow_experimental_hdf5_format = 0"
expect_error "structure given, so inference is skipped" 'The HDF5 format is experimental' "SELECT * FROM file('$F', 'HDF5', 'x Int32, y Float64') SETTINGS allow_experimental_hdf5_format = 0"

echo "=== Errors: only in clickhouse-local ==="

# Reading an HDF5 file hands it to libhdf5, which decodes the whole object header, B-tree and
# dataspace structure of the file before any of the reader's own checks get to run. That is only
# offered in clickhouse-local, where the file is one the user could already read by other means.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME="hdf5_${CLICKHOUSE_DATABASE}.h5"
cp "$F" "$USER_FILES_PATH/$FILE_NAME"

expect_error_on_server "detected from the extension" 'The HDF5 format is only available in clickhouse-local' "SELECT * FROM file('$FILE_NAME')"
expect_error_on_server "structure given, so inference is skipped" 'The HDF5 format is only available in clickhouse-local' "SELECT * FROM file('$FILE_NAME', 'HDF5', 'x Int32, y Float64')"

rm -f "$USER_FILES_PATH/$FILE_NAME"

echo "=== Malformed files ==="

# libhdf5 decodes the superblock, the object headers, the B-trees and the filter pipeline of a file
# before any of the reader's own checks can look at what it found, and all of that is C driven
# entirely by the contents of the file. This corrupts a valid file in a number of ways and pins one
# property of every outcome: the reader either produces a result or raises an ordinary ClickHouse
# exception. It never dies on a signal, trips a sanitizer, or reaches a logical error.
#
# Which of the two a particular corruption produces is deliberately not pinned. It depends on where
# the corrupted bytes land, and it would change with a `libhdf5` bump or on a platform of the other
# byte order. So the mutants are all read inside one process, and the outcome is read back out of
# `system.errors` afterwards: `LOGICAL_ERROR` has to be zero, and at least one mutant has to have
# been rejected, which is what stops this from passing without reading anything. A signal or a
# sanitizer report kills the process, so its two lines go missing and the reference no longer
# matches. A read that never finishes is caught by the test harness's own timeout.

WORK_DIR="${CLICKHOUSE_TMP}/hdf5_malformed_${CLICKHOUSE_DATABASE}"
rm -rf "${WORK_DIR:?}"
mkdir -p "$WORK_DIR/mutants"
trap 'rm -rf "${WORK_DIR:?}"' EXIT

BASE_SIZE=$(wc -c < "$F")

# Overwrite `count` bytes at `offset` with the byte `value`, in place.
patch_bytes() {
    head -c "$3" /dev/zero | tr '\0' "\\$(printf '%03o' "$4")" \
        | dd of="$1" bs=1 seek="$2" count="$3" conv=notrunc status=none
}

# Cut the file short at a ladder of lengths: inside the superblock, inside the root object header,
# inside the metadata that follows it, and one byte short of the end. `libhdf5` reads by seeking to
# offsets the file itself records, so a short file is the case where those offsets address bytes
# that are not there.
for length in 48 1024 65536 $((BASE_SIZE - 1)); do
    head -c "$length" "$F" > "$WORK_DIR/mutants/trunc_$length.h5"
done

# The version 0 superblock is a fixed layout. Every one of these is a number `libhdf5` has to believe
# before it has anything to check it against: the two address sizes decide how the rest of the file
# is parsed at all, and the addresses point at the structures that hold everything else.
i=0
for spec in "13 1 255" "14 1 1" "24 8 255" "40 8 127" "64 8 255" "80 8 255"; do
    # shellcheck disable=SC2086
    set -- $spec
    cp "$F" "$WORK_DIR/mutants/sb_$i.h5"
    patch_bytes "$WORK_DIR/mutants/sb_$i.h5" "$1" "$2" "$3"
    i=$((i + 1))
done

# The file is cut into equal slabs and each one is overwritten in turn, so that every byte of it -
# superblock, object headers, B-tree nodes, local heaps, compressed chunks and raw data - is
# destroyed by exactly one of these mutants. Nothing here has to know where anything lives, which
# matters because the fixtures are regenerated by `generate_test_data.py`, whose docstring notes that
# HDF5 output is not byte-reproducible.
SLABS=8
SLAB_SIZE=$(( (BASE_SIZE + SLABS - 1) / SLABS ))
for i in $(seq 0 $((SLABS - 1))); do
    cp "$F" "$WORK_DIR/mutants/slab_$i.h5"
    head -c "$SLAB_SIZE" /dev/zero | tr '\0' '\132' \
        | dd of="$WORK_DIR/mutants/slab_$i.h5" bs="$SLAB_SIZE" seek="$i" count=1 conv=notrunc status=none
done

# One byte at a time, over the region the reads walk before they reach any data. A single flip is the
# corruption most likely to leave a structure self-consistent enough to be acted on rather than
# rejected outright, which is what makes it worth doing separately from destroying a whole slab.
for offset in 100 400 1600; do
    cp "$F" "$WORK_DIR/mutants/byte_$offset.h5"
    patch_bytes "$WORK_DIR/mutants/byte_$offset.h5" "$offset" 1 255
done

# Reading the root group walks the symbol table and the object header of every child. The filtered
# datasets go as deep as the contents of a file reach: a compressed chunk is handed to `inflate` as
# it stands, and a chunk has to be decoded whole before any element of it can be handed out.
# `fletcher32` is the one filter meant to notice damage itself rather than fail on it.
ROOT_READS=""
FILTERED_READS=""
for mutant in "$WORK_DIR"/mutants/*.h5; do
    ROOT_READS="$ROOT_READS SELECT * FROM file('$mutant', 'HDF5') FORMAT Null;"
    for dataset in /deflate /shuffled /big_chunk /filters/fletcher32; do
        FILTERED_READS="$FILTERED_READS SELECT * FROM file('$mutant', 'HDF5') SETTINGS input_format_hdf5_dataset = '$dataset' FORMAT Null;"
    done
done

VERDICT="
SELECT 'logical errors: ' || toString(sum(value)) FROM system.errors WHERE name = 'LOGICAL_ERROR';
SELECT 'at least one mutant reported: ' || toString(sum(value) > 0) FROM system.errors;
"

echo "--- Reading the root group of every mutant ---"
$CLICKHOUSE_LOCAL --ignore-error -q "$ROOT_READS $VERDICT"

echo "--- Reading the filtered datasets of every mutant ---"
$CLICKHOUSE_LOCAL --ignore-error -q "$FILTERED_READS $VERDICT"

# A file that is not HDF5 at all has to fail with the reader's own message. Without this the section
# above would still pass if every read silently returned nothing.
: > "$WORK_DIR/empty.h5"
expect_error "an empty file" 'the file is empty' "SELECT * FROM file('$WORK_DIR/empty.h5', 'HDF5')"

echo "this is not an HDF5 file, it is a line of text" > "$WORK_DIR/text.h5"
expect_error "a file that is not HDF5 at all" 'Cannot open HDF5 file' "SELECT * FROM file('$WORK_DIR/text.h5', 'HDF5')"
