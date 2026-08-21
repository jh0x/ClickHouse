#!/usr/bin/env python3
"""Generate HDF5 test fixtures for tests/queries/0_stateless/04612_hdf5.

Requires: pip install h5py numpy

Regenerate all fixtures:
    python3 tests/queries/0_stateless/data_hdf5/generate_test_data.py

The generated .h5 files are committed alongside this script, and the test runs
against the committed files, not against this script's output. Keep the two in
sync: if you change a fixture here, regenerate and commit it. HDF5 output is not
byte-reproducible across h5py/libhdf5 versions, so a regenerated file will differ
in bytes even when its contents are identical - only regenerate what you changed.

Chunk shapes, compression levels and byte order are set explicitly rather than
left to h5py defaults, because the reader has separate code paths for chunked vs
contiguous layouts, for each filter, and for non-native byte order.

Almost everything lives in `test.h5`, one group per case, because a group is only
looked at when the query names it. The three other files exist because each needs
something the root of `test.h5` cannot also have:

  * `compound_root.h5` - a compound dataset at the root, which makes every read of
    that root an error, including the ones that must succeed in `test.h5`;
  * `unusual_precision.h5` - version 2 object headers, which is a property of the
    whole file;
  * `oversized_chunk.h5` - a hand-patched object header, whose dataset is corrupt.
"""

import os
import struct

import h5py
import numpy as np

DIR = os.path.dirname(os.path.abspath(__file__))


def create_test(path):
    """Everything that can share one file, one group per case.

    The root holds a plain pair of 1D datasets, so that reading the file without naming a dataset
    exercises the flat-group layout at `/`. Groups are skipped when the root is iterated, so no
    case below can affect that read - including the ones that are supposed to fail.
    """
    with h5py.File(path, "w") as f:
        # Root-level 1D datasets: layout 1 at `/`.
        f.create_dataset("x", data=np.array([1, 2, 3], dtype=np.int32))
        f.create_dataset("y", data=np.array([10.5, 20.5, 30.5], dtype=np.float64))

        # Flat group of 1D datasets.
        grp = f.create_group("flat")
        grp.create_dataset("x", data=np.array([1, 2, 3, 4, 5], dtype=np.int32))
        grp.create_dataset("y", data=np.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype=np.float64))

        # Compound dataset.
        dt = np.dtype([("id", np.int32), ("value", np.float64)])
        data = np.array([(1, 1.5), (2, 2.5), (3, 3.5)], dtype=dt)
        f.create_group("compound").create_dataset("data", data=data)

        # A compound whose members are of mixed and mostly odd widths, and not all of them
        # fixed-width numbers. This is the only compound fixture, so it has to carry both halves of
        # what a compound read has to get right.
        #
        # One pass over the dataset's records fills every selected column, into a record the reader
        # lays out itself rather than one the file describes, so each member's offset in that record
        # is computed. A member at the wrong offset reads its neighbour's bytes, which these widths
        # make visible: nothing is a multiple of anything else, `value` needs eight-byte alignment
        # behind a one-byte and a three-byte member, and the two string members are pointers rather
        # than values.
        #
        # A member is read by naming it inside a compound memory type: for a variable-length string
        # that member is a pointer, so the extraction type, the pointer array and the reclaim have
        # to agree on its size, and for a fixed-length string the extraction type has to carry the
        # member's own width. `text` and `tag` differ in charset, which is taken from the member's
        # own type rather than the dataset's. Lengths vary, one row of each string member is empty,
        # and the UTF-8 member holds multi-byte content, so a batch that lands at the wrong offset
        # produces the wrong bytes rather than merely the wrong count.
        #
        # The chunk shape is coprime with the batch sizes the test uses, so no chunk boundary lines
        # up with a batch boundary.
        n = 200
        compound_mixed_dt = np.dtype(
            [
                ("flag", np.uint8),
                ("code", "S3"),
                ("value", np.float64),
                ("small", np.int16),
                ("text", h5py.string_dtype(encoding="utf-8")),
                ("tag", h5py.string_dtype(encoding="ascii")),
                ("id", np.int32),
            ]
        )
        rows = np.empty(n, dtype=compound_mixed_dt)
        rows["flag"] = np.arange(n, dtype=np.uint8)
        rows["code"] = np.array(
            [b"" if i % 29 == 0 else f"c{i % 97:02d}".encode() for i in range(n)], dtype="S3"
        )
        rows["value"] = np.arange(1, n + 1) * 0.25
        rows["small"] = np.arange(-(n // 2), n // 2, dtype=np.int16)
        rows["text"] = np.array(
            ["" if i == 0 else f"row-{i:03d}-{'x' * (i % 17)}-ключ" for i in range(n)], dtype=object
        )
        rows["tag"] = np.array(["" if i == 1 else f"t{i % 13}" for i in range(n)], dtype=object)
        rows["id"] = np.arange(1, n + 1, dtype=np.int32)
        f.create_group("compound_mixed").create_dataset("data", data=rows, chunks=(37,))

        # Nested group. Deliberately widths that differ from the other groups.
        nested = f.create_group("nested/group")
        nested.create_dataset("a", data=np.array([10, 20, 30], dtype=np.int64))
        nested.create_dataset("b", data=np.array([1, 2, 3], dtype=np.float32))

        # Various numeric types.
        types_grp = f.create_group("types")
        types_grp.create_dataset("f32", data=np.array([1.0, 2.0, 3.0], dtype=np.float32))
        types_grp.create_dataset("f64", data=np.array([1e-10, 0.0, 1e10], dtype=np.float64))
        types_grp.create_dataset("i8", data=np.array([-128, 0, 127], dtype=np.int8))
        types_grp.create_dataset("u16", data=np.array([0, 1000, 65535], dtype=np.uint16))

        # Fixed-length strings. The dtype is taken from the array: passing an explicit
        # h5py.string_dtype(length=10) alongside S10 data has no conversion path.
        f.create_group("strings").create_dataset(
            "name", data=np.array([b"alice", b"bob", b"carol"], dtype="S10")
        )

        # Variable-length strings as datasets of their own, one per charset. The charset comes
        # from the dataset's own type, so both have to sit in one group to be read in one pass.
        vlen_grp = f.create_group("vlen")
        vlen_dt = h5py.string_dtype(encoding="utf-8")
        ascii_dt = h5py.string_dtype(encoding="ascii")
        vlen_grp.create_dataset("value", data=np.array([10, 20, 30], dtype=np.int32))
        vlen_grp.create_dataset(
            "label", data=np.array(["hello", "ключ", ""], dtype=object), dtype=vlen_dt
        )
        vlen_grp.create_dataset(
            "name", data=np.array(["alpha", "beta", "gamma"], dtype=object), dtype=ascii_dt
        )

        # Variable-length strings, enough rows to span several batches. The reader allocates the
        # pointer array and reclaims the strings libhdf5 malloc'd once per batch, so a leak or a
        # double free there needs more than one batch to show up. Lengths vary within a batch, one
        # row is empty, and the content is multi-byte, so a batch that lands at the wrong offset
        # produces the wrong bytes rather than merely the wrong count. The chunk shape is coprime
        # with the batch sizes the test uses, so no chunk boundary lines up with a batch boundary.
        n = 200
        vlen_large = f.create_group("vlen_large")
        vlen_large.create_dataset(
            "id", data=np.arange(1, n + 1, dtype=np.int32), chunks=(37,)
        )
        vlen_large.create_dataset(
            "text",
            data=np.array(
                ["" if i == 0 else f"row-{i:03d}-{'x' * (i % 17)}-ключ" for i in range(n)],
                dtype=object,
            ),
            dtype=vlen_dt,
            chunks=(37,),
        )

        # Empty datasets (0 rows).
        empty = f.create_group("empty")
        empty.create_dataset("x", shape=(0,), dtype=np.int32)
        empty.create_dataset("y", shape=(0,), dtype=np.float64)

        # A group whose datasets disagree on length, one of them empty. The empty one is the
        # interesting case: a reader that tracks the group length in a plain integer cannot tell
        # "no dataset seen yet" from "a dataset of zero rows", so it lets the pair through. Which
        # dataset is read first depends on the requested structure, so the test asks for both orders.
        mismatched = f.create_group("mismatched")
        mismatched.create_dataset("a_empty", shape=(0,), dtype=np.int32)
        mismatched.create_dataset("b_full", data=np.arange(1, 6, dtype=np.int32))

        # Large chunked datasets spanning multiple batches (>65536 rows).
        n = 70000
        ids = np.arange(1, n + 1, dtype=np.int32)
        large = f.create_group("large")
        large.create_dataset("id", data=ids, chunks=(2188,), compression="gzip", compression_opts=4)
        large.create_dataset(
            "value", data=ids.astype(np.float64) * 0.5, chunks=(2188,), compression="gzip", compression_opts=4
        )

        # Deflate-compressed chunked datasets, one chunk covering the whole extent.
        n = 100
        ids = np.arange(1, n + 1, dtype=np.int32)
        deflate = f.create_group("deflate")
        deflate.create_dataset("id", data=ids, chunks=(100,), compression="gzip", compression_opts=6)
        deflate.create_dataset(
            "value", data=ids.astype(np.float64) * 1.5, chunks=(100,), compression="gzip", compression_opts=6
        )

        # Chunked datasets with shuffle + deflate.
        n = 200
        ids = np.arange(1, n + 1, dtype=np.int32)
        shuffled = f.create_group("shuffled")
        shuffled.create_dataset(
            "id", data=ids, chunks=(64,), shuffle=True, compression="gzip", compression_opts=1
        )
        shuffled.create_dataset(
            "value",
            data=ids.astype(np.float64) * 1.5,
            chunks=(64,),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
        )

        # A filtered chunk larger than the 8 MiB of decoded chunks libhdf5 keeps per dataset by
        # default. A chunk that does not fit that budget is not kept at all, so it would be decoded
        # again for every block the reader produces; the reader gives such a dataset a budget of its
        # own. The chunk is declared far larger than the data it holds, which an extensible dataset
        # is allowed to do, so that the fixture stays small: the stored chunk is mostly fill value
        # and deflates to almost nothing, while the chunk libhdf5 has to decode is 9.6 MB. The
        # extent spans several batches, all of them inside that one chunk.
        n = 100000
        f.create_group("big_chunk").create_dataset(
            "id",
            data=np.arange(1, n + 1, dtype=np.int64),
            chunks=(1200000,),
            maxshape=(None,),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
        )

        # A group of two such datasets. The reader bounds the chunk cache it asks for across the whole
        # group rather than per dataset, so with a budget that fits one chunk the first dataset by
        # name gets a cache and the second keeps the default one. Both are read correctly either way;
        # only the decoding cost differs.
        big_chunks = f.create_group("big_chunks")
        big_chunks.create_dataset(
            "id",
            data=np.arange(1, n + 1, dtype=np.int64),
            chunks=(1200000,),
            maxshape=(None,),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
        )
        big_chunks.create_dataset(
            "value",
            data=np.arange(1, n + 1, dtype=np.float64) * 0.5,
            chunks=(1200000,),
            maxshape=(None,),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
        )

        # Shuffled datasets for which the HDF5 shuffle filter degenerates to a no-op: it passes the
        # buffer through unchanged when the element is a single byte or when a chunk holds less than
        # two whole elements.
        ids = np.arange(1, 21)
        edge = f.create_group("shuffle_edge")
        edge.create_dataset(
            "i8", data=ids.astype(np.int8), chunks=(8,), shuffle=True, compression="gzip", compression_opts=1
        )
        edge.create_dataset(
            "i64", data=ids.astype(np.int64), chunks=(1,), shuffle=True, compression="gzip", compression_opts=1
        )

        # Big-endian contiguous datasets. Both columns stay big-endian on disk so that the 4-byte and
        # the 8-byte conversion paths are covered. numpy arithmetic returns a native-endian result,
        # so the byte order has to be applied after the arithmetic, not before.
        ids = np.arange(1, 101)
        bigendian = f.create_group("bigendian")
        bigendian.create_dataset("id", data=ids.astype(">i4"))
        bigendian.create_dataset("value", data=(ids * 2.5).astype(">f8"))

        # Big-endian chunked datasets with shuffle + deflate.
        ids = np.arange(1, 151)
        bigendian_c = f.create_group("bigendian_compressed")
        bigendian_c.create_dataset(
            "id", data=ids.astype(">i4"), chunks=(50,), shuffle=True, compression="gzip", compression_opts=1
        )
        bigendian_c.create_dataset(
            "value",
            data=(ids * 3.0).astype(">f8"),
            chunks=(50,),
            shuffle=True,
            compression="gzip",
            compression_opts=1,
        )

        # A 2D dataset, a nested compound type, an enum and an empty group: everything the reader
        # refuses to describe as columns.
        f.create_group("nd").create_dataset("matrix", data=np.zeros((3, 4), dtype=np.int32))

        inner_dt = np.dtype([("a", np.int32), ("b", np.float64)])
        outer_dt = np.dtype([("id", np.int32), ("nested", inner_dt)])
        f.create_group("nested_compound").create_dataset(
            "data", data=np.array([(1, (2, 3.0))], dtype=outer_dt)
        )

        enum_dt = h5py.enum_dtype({"RED": 0, "GREEN": 1, "BLUE": 2}, basetype=np.uint8)
        f.create_group("enum").create_dataset("colors", data=np.array([0, 1, 2], dtype=np.uint8), dtype=enum_dt)

        f.create_group("empty_group")

        # Types whose value does not fill their storage, next to the same values in standard types.
        # A 4-byte integer of precision 24 keeps its value in 24 bits and pads the rest; here the
        # padding is all-ones so that a raw byte copy is guaranteed to differ from the decoded value.
        # `plain` holds the same numbers in standard types and is what they must compare equal to.
        ids = np.array([1, 2, -3, 1000], dtype=np.int32)
        values = np.array([1.5, -2.5, 3.5, -4.5], dtype=np.float64)

        narrow = f.create_group("narrow")
        i24 = h5py.h5t.STD_I32LE.copy()
        i24.set_precision(24)
        i24.set_offset(0)
        i24.set_pad(h5py.h5t.PAD_ONE, h5py.h5t.PAD_ONE)
        space = h5py.h5s.create_simple(ids.shape)
        ds = h5py.h5d.create(narrow.id, b"id", i24, space)
        ds.write(h5py.h5s.ALL, h5py.h5s.ALL, ids, mtype=h5py.h5t.NATIVE_INT32)
        narrow.create_dataset("value", data=values)

        plain = f.create_group("plain")
        plain.create_dataset("id", data=ids)
        plain.create_dataset("value", data=values)

        _add_filters(f)
        _add_links(f, os.path.basename(path))
        _add_data_in_other_files(f)


def _add_filters(f):
    """One dataset per filter that the reader does not decode itself.

    libhdf5 runs the whole filter pipeline, so each of these has to read back correctly. `unknown`
    is filter id 32001 (blosc), which no build of ClickHouse registers; it is recorded as optional,
    so libhdf5 stores the data unfiltered and skips the filter on read. That id is what used to make
    libhdf5 search `$HDF5_PLUGIN_PATH` and `dlopen` whatever it found there, which the plugin-loader
    stub now makes impossible.
    """
    ids = np.arange(64, dtype=np.int32)
    filters = f.create_group("filters")

    filters.create_dataset("fletcher32", data=ids, chunks=(16,), fletcher32=True)
    filters.create_dataset("scaleoffset", data=ids, chunks=(16,), scaleoffset=0)

    space = h5py.h5s.create_simple(ids.shape)

    nbit_dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    nbit_dcpl.set_chunk((16,))
    # The high-level API turns object time tracking off for us; the low-level one does not, and a
    # stored modification time would make this fixture differ on every run.
    nbit_dcpl.set_obj_track_times(False)
    nbit_dcpl.set_filter(h5py.h5z.FILTER_NBIT, h5py.h5z.FLAG_MANDATORY, ())
    nbit = h5py.h5d.create(filters.id, b"nbit", h5py.h5t.STD_I32LE, space, nbit_dcpl)
    nbit.write(h5py.h5s.ALL, h5py.h5s.ALL, ids, mtype=h5py.h5t.NATIVE_INT32)

    unknown_dcpl = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    unknown_dcpl.set_chunk((16,))
    unknown_dcpl.set_obj_track_times(False)
    unknown_dcpl.set_filter(32001, h5py.h5z.FLAG_OPTIONAL, ())
    unknown = h5py.h5d.create(filters.id, b"unknown", h5py.h5t.STD_I32LE, space, unknown_dcpl)
    unknown.write(h5py.h5s.ALL, h5py.h5s.ALL, ids, mtype=h5py.h5t.NATIVE_INT32)


def _add_links(f, own_file_name):
    """External links and soft links, each group next to a readable dataset called `own`.

    An external link names an object in another file, so following it would let the file decide
    which other files get opened; the reader refuses to traverse one. It is still skipped rather
    than fatal when a group is iterated for the schema, which is what `own` is there to show. The
    target is this same file, so that the refusal is observably by link type rather than by the
    target failing to open.

    A soft link stays inside the file and is followed, which is what `resolved` shows. Nothing keeps
    it pointing at an object, though, so a dangling one names no object and therefore no column: the
    group is iterated past it rather than refused, which is what `own` shows next to `dangling`.
    The two dangling links differ in where the target path stops resolving - the last component for
    `dangling`, an earlier one for `dangling_deep` - because libhdf5 reports those two differently
    and the reader has to treat them the same.

    `resolved` points at a root dataset of the same length as `own`, since a group whose datasets
    disagree on length is an error in its own right.
    """
    external = f.create_group("links/external")
    external.create_dataset("own", data=np.array([1, 2, 3], dtype=np.int32))
    external["linked"] = h5py.ExternalLink(own_file_name, "/flat/x")

    dangling = f.create_group("links/dangling")
    dangling.create_dataset("own", data=np.array([1, 2, 3], dtype=np.int32))
    dangling["dangling"] = h5py.SoftLink("/missing")
    dangling["dangling_deep"] = h5py.SoftLink("/missing/deep")

    soft = f.create_group("links/soft")
    soft.create_dataset("own", data=np.array([1, 2, 3], dtype=np.int32))
    soft["resolved"] = h5py.SoftLink("/x")


def _add_data_in_other_files(f):
    """Datasets that belong to this file but whose data does not.

    `H5Pset_external` keeps the raw data in a separate file, which libhdf5 opens with `open`
    directly, and a virtual dataset maps its elements onto datasets in files named by its own source
    mapping. Either way the file being read decides which other files get opened, so both are an
    error - when named explicitly and when the group is iterated.

    `virtual_self` is the case that a per-dataset check alone would miss: a source named `.` is the
    file itself, so libhdf5 reads the source dataset directly without the reader ever inspecting it,
    and that source keeps its raw data in `payload.bin`. None of the referenced files are created,
    because every read here has to fail before anything is opened.
    """
    f.create_group("external_data").create_dataset(
        "data", shape=(3,), dtype=np.int32, external=[("payload.bin", 0, 12)]
    )

    other = h5py.VirtualLayout(shape=(4,), dtype=np.int32)
    other[:] = h5py.VirtualSource("source.h5", "data", shape=(4,))
    f.create_group("virtual_other").create_virtual_dataset("data", other, fillvalue=0)

    self_group = f.create_group("virtual_self")
    self_group.create_dataset("source", shape=(4,), dtype=np.int32, external=[("payload.bin", 0, 16)])
    own = h5py.VirtualLayout(shape=(4,), dtype=np.int32)
    own[:] = h5py.VirtualSource(".", "/virtual_self/source", shape=(4,))
    self_group.create_virtual_dataset("data", own, fillvalue=0)


def create_compound_root(path):
    """A compound dataset at the root of the file.

    A compound becomes a column per field only when `input_format_hdf5_dataset` names it directly.
    Reached by iterating the group that holds it, it would have to become a single `Tuple` column,
    which the reader cannot fill, so it says which path to ask for instead. At the root that path
    has no group component, which is a separate branch from the one a named group takes.

    This cannot live in `test.h5`, because it would make every read of that file's root an error.
    """
    n = 500
    ids = np.arange(1, n + 1, dtype=np.int32)
    dt = np.dtype([("id", np.int32), ("value", np.float64)])
    data = np.empty(n, dtype=dt)
    data["id"] = ids
    data["value"] = ids.astype(np.float64) * 0.1

    with h5py.File(path, "w") as f:
        f.create_dataset("compound", data=data)


def create_unusual_precision(path):
    """A 4-byte integer declaring a precision of 8 bits.

    This is what separates the two ways of sizing a type: its storage size is 4 bytes but its
    native size is 1, so deriving the ClickHouse type from the storage size would make `DESCRIBE`
    say `Int32` while the read produced one byte per element. The precision-24 dataset in the
    `narrow` group of `test.h5` does not catch this, because 24 bits also map to a 4-byte native
    type.

    libhdf5 calls this a numeric type with an unusual number of unused bits and refuses to decode it
    out of an object header that carries no checksum, so the file is written with `libver` `latest`
    to get version-2 (checksummed) object headers. That is a property of the whole file, which is
    why this is not a group of `test.h5`.
    """
    ids = np.array([1, 2, -3, 100], dtype=np.int32)

    with h5py.File(path, "w", libver="latest") as f:
        i8_in_i32 = h5py.h5t.STD_I32LE.copy()
        i8_in_i32.set_precision(8)
        i8_in_i32.set_offset(0)
        i8_in_i32.set_pad(h5py.h5t.PAD_ONE, h5py.h5t.PAD_ONE)
        space = h5py.h5s.create_simple(ids.shape)
        ds = h5py.h5d.create(f.id, b"id", i8_in_i32, space)
        ds.write(h5py.h5s.ALL, h5py.h5s.ALL, ids, mtype=h5py.h5t.NATIVE_INT32)


def create_oversized_chunk(path):
    """A deliberately corrupt file whose chunk dimension does not fit the element size.

    `chunk_dim * element_size` is what a chunk costs to decode, and libhdf5 hands the dimension over
    from the file untouched: a version 4 layout message stores every chunk dimension as a
    variable-length 64 bit integer (`H5Olayout.c`) and rejects only zero, so a file can declare a
    chunk of 2^61 elements that overflows the product. libhdf5 refuses to *create* a chunk larger
    than 4 GB, so the message has to be written by hand - which is also why this is its own file.

    The dataset is created as 3D only to make the version 3 layout message long enough to be
    rewritten in place as a version 4 one; the raw chunk bytes of (64, 1, 1) and of (64,) are the
    same, so the patched 1D dataset describes the data that is actually in the file. Nothing else
    about the file is unusual - it is a single shuffled chunk of 64 `Int64` values.
    """
    huge_chunk_dim = 1 << 61
    element_size = 8
    # Width of every encoded chunk dimension. libhdf5 writes the smallest width that fits the
    # dimensions it created, which is why a dimension this large can only be written by hand.
    enc_bytes_per_dim = 8
    ids = np.arange(64, dtype=np.int64)

    with h5py.File(path, "w") as f:
        ds = f.create_dataset("data", data=ids.reshape(64, 1, 1), chunks=(64, 1, 1), shuffle=True)
        header_addr = h5py.h5o.get_info(ds.id).addr
        chunk = ds.id.get_chunk_info(0)

    msg_nil, msg_dataspace, msg_layout = 0x0000, 0x0001, 0x0008

    # Version 1 dataspace message: 1 dimension, current and maximum sizes.
    dataspace = struct.pack("<BBB5x", 1, 1, 1) + struct.pack("<QQ", ids.size, ids.size)

    # Version 4 chunked layout message with a single chunk index. The chunk is filtered, so the
    # index carries its stored size and filter mask; `2` is the flag that says so.
    layout = (
        struct.pack("<BBBBB", 4, 2, 0x02, 2, enc_bytes_per_dim)
        + struct.pack("<QQ", huge_chunk_dim, element_size)
        + struct.pack("<B", 1)
        + struct.pack("<QI", chunk.size, chunk.filter_mask)
        + struct.pack("<Q", chunk.byte_offset)
    )

    with open(path, "rb") as f:
        buf = bytearray(f.read())

    # Version 1 object headers carry no checksum, so the message block can be rebuilt in place as
    # long as it keeps its total size. Messages are 8-byte aligned and prefixed by type, size and
    # flags; a NIL message takes up whatever is left over.
    version, _, _, _, block_size = struct.unpack_from("<BBHII", buf, header_addr)
    assert version == 1, f"expected a version 1 object header, got {version}"

    block_start = header_addr + 16
    messages = []
    offset = block_start
    while offset < block_start + block_size:
        mtype, msize, flags = struct.unpack_from("<HHB", buf, offset)
        assert mtype != 0x0010, "the object header is continued elsewhere"
        messages.append((mtype, flags, bytes(buf[offset + 8 : offset + 8 + msize])))
        offset += 8 + msize

    rebuilt = bytearray()
    kept = 0
    for mtype, flags, data in messages:
        if mtype == msg_nil:
            continue
        if mtype == msg_dataspace:
            data = dataspace
        elif mtype == msg_layout:
            data = layout
        data += b"\0" * (-len(data) % 8)
        rebuilt += struct.pack("<HHB3x", mtype, len(data), flags) + data
        kept += 1

    padding = block_size - len(rebuilt) - 8
    assert padding >= 0, "the rewritten messages do not fit the object header"
    rebuilt += struct.pack("<HHB3x", msg_nil, padding, 0) + b"\0" * padding

    buf[block_start : block_start + block_size] = rebuilt
    struct.pack_into("<H", buf, header_addr + 2, kept + 1)

    with open(path, "wb") as f:
        f.write(buf)


def main():
    create_test(os.path.join(DIR, "test.h5"))
    create_compound_root(os.path.join(DIR, "compound_root.h5"))
    create_unusual_precision(os.path.join(DIR, "unusual_precision.h5"))
    create_oversized_chunk(os.path.join(DIR, "oversized_chunk.h5"))
    print(f"Generated all HDF5 test fixtures in {DIR}")


if __name__ == "__main__":
    main()
