#pragma once

#include "config.h"

#if USE_HDF5

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <hdf5.h>

namespace DB
{

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

/// Metadata about a single 1D dataset within a group ("Layout 1").
struct HDF5DatasetInfo
{
    String name;
    HDF5Handle dataset;
    HDF5Handle dataspace;
    HDF5Handle datatype;
    hsize_t num_rows = 0;
    DataTypePtr ch_type;
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
