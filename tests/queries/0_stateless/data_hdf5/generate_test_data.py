#!/usr/bin/env python3
"""Generate HDF5 test fixtures for tests/queries/0_stateless/04612_hdf5_read.

Requires: pip install h5py numpy

Regenerate all fixtures:
    python3 tests/queries/0_stateless/data_hdf5/generate_test_data.py
"""

import os
import struct

import h5py
import numpy as np

DIR = os.path.dirname(os.path.abspath(__file__))


def create_trivial(path):
    """Root-level 1D datasets (Layout 1 at /)."""
    with h5py.File(path, "w") as f:
        f.create_dataset("x", data=np.array([1, 2, 3], dtype=np.int32))
        f.create_dataset("y", data=np.array([10.5, 20.5, 30.5], dtype=np.float64))


def create_test(path):
    """Multi-purpose test file with various layouts and types."""
    with h5py.File(path, "w") as f:
        # Flat group of 1D datasets.
        grp = f.create_group("flat")
        grp.create_dataset("x", data=np.array([1, 2, 3, 4, 5], dtype=np.int32))
        grp.create_dataset("y", data=np.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype=np.float64))

        # Compound dataset.
        dt = np.dtype([("id", np.int32), ("value", np.float64)])
        data = np.array([(1, 1.5), (2, 2.5), (3, 3.5)], dtype=dt)
        f.create_group("compound").create_dataset("data", data=data)

        # Nested group.
        nested = f.create_group("nested/group")
        nested.create_dataset("a", data=np.array([10, 20, 30], dtype=np.int32))
        nested.create_dataset("b", data=np.array([1, 2, 3], dtype=np.int32))

        # Various numeric types.
        types_grp = f.create_group("types")
        types_grp.create_dataset("f32", data=np.array([1.0, 2.0, 3.0], dtype=np.float32))
        types_grp.create_dataset("f64", data=np.array([1e-10, 0.0, 1e10], dtype=np.float64))
        types_grp.create_dataset("i8", data=np.array([-128, 0, 127], dtype=np.int8))
        types_grp.create_dataset("u16", data=np.array([0, 1000, 65535], dtype=np.uint16))

        # Fixed-length strings.
        str_dt = h5py.string_dtype(length=10)
        f.create_group("strings").create_dataset(
            "name", data=np.array([b"alice", b"bob", b"carol"], dtype="S10"), dtype=str_dt
        )

        # Variable-length strings (UTF-8).
        vlen_grp = f.create_group("vlen_utf8")
        vlen_dt = h5py.string_dtype(encoding="utf-8")
        vlen_grp.create_dataset("label", data=np.array(["hello", "world", ""], dtype=object), dtype=vlen_dt)
        vlen_grp.create_dataset("value", data=np.array([10, 20, 30], dtype=np.int32))

        # Variable-length strings (ASCII).
        ascii_grp = f.create_group("vlen_ascii")
        ascii_dt = h5py.string_dtype(encoding="ascii")
        ascii_grp.create_dataset("id", data=np.array([1, 2, 3], dtype=np.int32))
        ascii_grp.create_dataset(
            "name", data=np.array(["alpha", "beta", "gamma"], dtype=object), dtype=ascii_dt
        )


def create_empty(path):
    """Empty datasets (0 rows)."""
    with h5py.File(path, "w") as f:
        f.create_dataset("x", shape=(0,), dtype=np.int32)
        f.create_dataset("y", shape=(0,), dtype=np.float64)


def create_large(path):
    """Large dataset spanning multiple batches (>65536 rows)."""
    n = 70000
    with h5py.File(path, "w") as f:
        f.create_dataset("id", data=np.arange(1, n + 1, dtype=np.int32))


def create_compressed(path):
    """Deflate-compressed chunked dataset."""
    n = 100
    dt = np.dtype([("id", np.int32), ("value", np.float64)])
    data = np.array([(i, i * 1.5) for i in range(1, n + 1)], dtype=dt)
    with h5py.File(path, "w") as f:
        f.create_group("deflate").create_dataset(
            "data", data=data, chunks=(50,), compression="gzip", compression_opts=6
        )


def create_schema_mismatch(path):
    """Int32 dataset for testing schema mismatch errors."""
    with h5py.File(path, "w") as f:
        f.create_dataset("col", data=np.array([1, 2, 3], dtype=np.int32))


def create_unsupported(path):
    """Files with unsupported features for error testing."""
    with h5py.File(path, "w") as f:
        # 2D dataset.
        f.create_group("nd").create_dataset("matrix", data=np.zeros((3, 4), dtype=np.float64))

        # Nested compound type.
        inner_dt = np.dtype([("a", np.int32)])
        outer_dt = np.dtype([("x", np.int32), ("inner", inner_dt)])
        f.create_group("nested_compound").create_dataset("data", data=np.zeros(2, dtype=outer_dt))

        # Enum type.
        enum_dt = h5py.enum_dtype({"RED": 0, "GREEN": 1, "BLUE": 2}, basetype=np.uint8)
        f.create_group("enum").create_dataset("colors", data=np.array([0, 1, 2], dtype=np.uint8), dtype=enum_dt)

        # Empty group.
        f.create_group("empty_group")


def create_shuffle(path):
    """Chunked dataset with shuffle + deflate filters."""
    n = 200
    with h5py.File(path, "w") as f:
        grp = f.create_group("shuffled")
        grp.create_dataset(
            "id",
            data=np.arange(1, n + 1, dtype=np.int32),
            chunks=(50,),
            shuffle=True,
            compression="gzip",
            compression_opts=6,
        )
        grp.create_dataset(
            "value",
            data=np.arange(1, n + 1, dtype=np.float64) * 1.5,
            chunks=(50,),
            shuffle=True,
            compression="gzip",
            compression_opts=6,
        )


def create_bigendian(path):
    """Big-endian contiguous dataset (tests byte-swap in direct read)."""
    n = 100
    with h5py.File(path, "w") as f:
        grp = f.create_group("data")
        grp.create_dataset("id", data=np.arange(1, n + 1, dtype=">i4"))
        grp.create_dataset("value", data=np.arange(1, n + 1, dtype=">f8") * 2.5)


def create_bigendian_compressed(path):
    """Big-endian chunked dataset with shuffle + deflate."""
    n = 150
    with h5py.File(path, "w") as f:
        grp = f.create_group("data")
        grp.create_dataset(
            "id",
            data=np.arange(1, n + 1, dtype=">i4"),
            chunks=(50,),
            shuffle=True,
            compression="gzip",
            compression_opts=6,
        )
        grp.create_dataset(
            "value",
            data=np.arange(1, n + 1, dtype=">f8") * 3.0,
            chunks=(50,),
            shuffle=True,
            compression="gzip",
            compression_opts=6,
        )


def create_crosscheck(path):
    """Cross-check file: same data as flat datasets and compound dataset.

    Used to verify that the direct read path (flat) produces identical
    results to the H5Dread path (compound).
    """
    n = 500
    ids = np.arange(1, n + 1, dtype=np.int32)
    values = ids.astype(np.float64) * 1.1

    with h5py.File(path, "w") as f:
        # Flat (will use direct read).
        flat = f.create_group("flat")
        flat.create_dataset("id", data=ids)
        flat.create_dataset("value", data=values)

        # Flat compressed (will use direct read with decompression).
        flat_c = f.create_group("flat_compressed")
        flat_c.create_dataset("id", data=ids, chunks=(100,), compression="gzip")
        flat_c.create_dataset("value", data=values, chunks=(100,), compression="gzip")

        # Compound (will use H5Dread fallback).
        dt = np.dtype([("id", np.int32), ("value", np.float64)])
        compound_data = np.empty(n, dtype=dt)
        compound_data["id"] = ids
        compound_data["value"] = values
        f.create_dataset("compound", data=compound_data)


def main():
    create_trivial(os.path.join(DIR, "trivial.h5"))
    create_test(os.path.join(DIR, "test.h5"))
    create_empty(os.path.join(DIR, "empty.h5"))
    create_large(os.path.join(DIR, "large.h5"))
    create_compressed(os.path.join(DIR, "compressed.h5"))
    create_schema_mismatch(os.path.join(DIR, "schema_mismatch.h5"))
    create_unsupported(os.path.join(DIR, "unsupported.h5"))
    create_shuffle(os.path.join(DIR, "shuffle.h5"))
    create_bigendian(os.path.join(DIR, "bigendian.h5"))
    create_bigendian_compressed(os.path.join(DIR, "bigendian_compressed.h5"))
    create_crosscheck(os.path.join(DIR, "crosscheck.h5"))
    print(f"Generated all HDF5 test fixtures in {DIR}")


if __name__ == "__main__":
    main()
