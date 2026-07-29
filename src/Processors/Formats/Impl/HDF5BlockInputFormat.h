#pragma once

#include "config.h"

#if USE_HDF5

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <hdf5.h>

#include <optional>

namespace DB
{

class SeekableReadBuffer;

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
    HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t));
    ~HDF5Handle();

    HDF5Handle(HDF5Handle && o) noexcept;
    HDF5Handle & operator=(HDF5Handle && o) noexcept;
    HDF5Handle(const HDF5Handle &) = delete;
    HDF5Handle & operator=(const HDF5Handle &) = delete;

    hid_t get() const { return id; }
    operator hid_t() const { return id; }
};

/// Physical location of a single HDF5 chunk on disk.
struct HDF5ChunkInfo
{
    hsize_t logical_offset; /// Position of the chunk's first element.
    haddr_t file_addr; /// Byte offset in the file.
    hsize_t size; /// On-disk byte count (possibly compressed).
    unsigned filter_mask; /// Bitmask: bit set = filter skipped for this chunk.
};

/// Metadata extracted from an HDF5 dataset
/// When present, bulk data can be read directly rather than via hdf5 functions
struct HDF5DirectReadMeta
{
    H5D_layout_t layout;
    haddr_t contiguous_offset = HADDR_UNDEF;
    hsize_t chunk_dim = 0;
    H5T_order_t byte_order = H5T_ORDER_ERROR;
    size_t element_size = 0;
    std::vector<H5Z_filter_t> filters;
    std::vector<HDF5ChunkInfo> chunks; /// Sorted by logical_offset.
};

/// Metadata about a single 1D dataset within a group ("Layout 1").
struct HDF5DatasetInfo
{
    String name;
    HDF5Handle dataset;
    HDF5Handle dataspace;
    HDF5Handle datatype;
    hsize_t num_rows = 0;
    DataTypePtr ch_type;
    std::optional<HDF5DirectReadMeta> direct_meta; /// Set when direct I/O is possible.
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
    void tryEnableDirectRead();
    void closeHandles();

    const FormatSettings format_settings;
    std::atomic<int> is_stopped{0};
    bool reader_prepared = false;

    HDF5Handle file_handle;

    /// Layout 1: flat group of 1D datasets
    std::vector<HDF5DatasetInfo> datasets;

    /// Layout 2: compound dataset
    bool is_compound = false;
    HDF5Handle compound_dataset;
    HDF5Handle compound_dataspace;
    HDF5Handle compound_datatype;
    std::vector<unsigned> compound_member_indices;

    hsize_t rows_read = 0;
    hsize_t total_rows = 0;
    static constexpr hsize_t BATCH_SIZE = 65536;

    /// User-specified hyperslab
    std::optional<ResolvedHyperslab> user_hyperslab;

    bool use_direct_read = false;
    SeekableReadBuffer * seekable_buf = nullptr;
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
