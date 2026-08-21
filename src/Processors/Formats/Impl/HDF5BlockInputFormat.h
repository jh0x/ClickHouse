#pragma once

#include "config.h"

#if USE_HDF5

#include <Core/Defines.h>
#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <hdf5.h>

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

/// Resolved hyperslab parameters for 1D selection.
struct ResolvedHyperslab
{
    hsize_t start;
    hsize_t stride;
    hsize_t count;
    hsize_t block;
    hsize_t total_elements; /// = count * block
};

class HDF5Handle
{
    hid_t id = H5I_INVALID_HID;
    herr_t (*closer)(hid_t) = nullptr;

public:
    HDF5Handle() = default;
    /// Throws `INCORRECT_DATA` if `id_` is invalid: an unusable handle means the file did not
    /// give us what it was supposed to.
    HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t), std::string_view what);
    /// The same, for the places where an invalid handle means the query named something wrong
    /// rather than the file being broken.
    HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t), std::string_view what, int error_code);
    ~HDF5Handle();

    HDF5Handle(HDF5Handle && o) noexcept;
    HDF5Handle & operator=(HDF5Handle && o) noexcept;
    HDF5Handle(const HDF5Handle &) = delete;
    HDF5Handle & operator=(const HDF5Handle &) = delete;

    hid_t get() const { return id; }
    operator hid_t() const { return id; }
};

/// Where one column's values sit in the record a single `H5Dread` produces, and how to get them out.
struct HDF5ColumnInfo
{
    /// Offset of the value inside one record. Zero when the read writes the values themselves
    /// rather than records built from members.
    size_t offset = 0;
    /// The width of one value in memory. For a variable-length string that is the width of the
    /// pointer, not of the string.
    size_t element_size = 0;
    /// A variable-length string is a `char *` into a buffer libhdf5 allocated, not the value.
    bool is_vlen_string = false;
    /// Position of the column in the header, which is where the values go.
    size_t column_index = 0;
};

/// Everything one `H5Dread` needs. Prepared once, before the first batch. A layout that gives every
/// column a dataset of its own produces one of these per column; a compound dataset produces a
/// single one that fills all of its columns at once.
struct HDF5DatasetInfo
{
    HDF5Handle dataset;
    /// One per read, because `H5Sselect_hyperslab` writes into the dataspace it selects on.
    HDF5Handle dataspace;
    /// The type `H5Dread` writes through: the value's own type, or a compound holding one member
    /// per column this read fills.
    HDF5Handle mem_type;
    /// The size of one record of `mem_type`, which is the distance between two elements in the
    /// buffer `H5Dread` writes into.
    size_t record_size = 0;
    /// True when `mem_type` has a variable-length member, so the buffer comes back holding pointers
    /// libhdf5 allocated and has to be handed back to it.
    bool has_vlen_member = false;
    std::vector<HDF5ColumnInfo> columns;
};

class HDF5BlockInputFormat final : public IInputFormat
{
public:
    HDF5BlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_);
    ~HDF5BlockInputFormat() override;

    String getName() const override { return "HDF5BlockInputFormat"; }
    void resetParser() override;

protected:
    Chunk read() override;
    void onCancel() noexcept override { is_stopped = 1; }

private:
    void prepareReader();
    void closeHandles();

    const FormatSettings format_settings;
    std::atomic<int> is_stopped{0};
    bool reader_prepared = false;

    HDF5Handle file_handle;

    /// One entry per `H5Dread` a batch needs, which is one per column except for a compound
    /// dataset, whose columns are all filled by a single read.
    std::vector<HDF5DatasetInfo> datasets;

    /// The records of one batch, exactly as `H5Dread` writes them, reused across batches. Only the
    /// reads that cannot write straight into a column need it: a read filling several columns, and
    /// a variable-length string, whose value in the buffer is a pointer rather than the string. It
    /// is a ClickHouse container rather than a plain buffer so that its size counts towards the
    /// query's memory.
    Memory<> record_buffer;

    hsize_t rows_read = 0;
    /// Number of rows in one output chunk, from `input_format_hdf5_max_block_size`. This is also
    /// what one `H5Dread` writes at a time, so a batch costs this many rows times the width of a
    /// row - a bound on rows, not on bytes. The buffers it fills are the columns' own memory and
    /// `record_buffer`, both of which count towards `max_memory_usage`.
    hsize_t batch_size = DEFAULT_BLOCK_SIZE;

    /// The selection to read: the user's hyperslab, or the whole dataset as an identity one.
    ResolvedHyperslab selection{};
};

class HDF5SchemaReader final : public ISchemaReader
{
public:
    HDF5SchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initialize();

    const FormatSettings format_settings;
    NamesAndTypesList cached_schema;
    std::optional<size_t> cached_num_rows;
    bool initialized = false;
};

}

#endif
