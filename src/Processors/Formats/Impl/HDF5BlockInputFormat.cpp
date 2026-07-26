#include "config.h"

#if USE_HDF5

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WithFileName.h>
#include <Processors/Formats/Impl/HDF5BlockInputFormat.h>

#include <base/scope_guard.h>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int BAD_ARGUMENTS;
}

HDF5Handle::HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t))
    : id(id_)
    , closer(closer_)
{
    if (id < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "HDF5 open failed (hid_t < 0)");
}

HDF5Handle::~HDF5Handle()
{
    if (id >= 0 && closer)
        closer(id);
}

HDF5Handle::HDF5Handle(HDF5Handle && o) noexcept
    : id(std::exchange(o.id, H5I_INVALID_HID))
    , closer(o.closer)
{
}

HDF5Handle & HDF5Handle::operator=(HDF5Handle && o) noexcept
{
    std::swap(id, o.id);
    std::swap(closer, o.closer);
    return *this;
}


namespace
{

/// Single global mutex for all HDF5 API calls.
std::mutex hdf5_global_mutex;

void checkHDF5(herr_t status, const char * what)
{
    if (status < 0)
    {
        std::string msg;
        H5Ewalk2(
            H5E_DEFAULT,
            H5E_WALK_DOWNWARD,
            [](unsigned, const H5E_error2_t * err, void * ctx) -> herr_t
            {
                auto & s = *static_cast<std::string *>(ctx);
                if (!s.empty())
                    s += "; ";
                s += err->desc;
                return 0;
            },
            &msg);
        H5Eclear2(H5E_DEFAULT);
        throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", what, msg);
    }
}

/// Safely extract a compound member name, freeing the HDF5-allocated buffer.
String getMemberName(hid_t compound_type, unsigned index)
{
    char * name = H5Tget_member_name(compound_type, index);
    if (!name)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member name at index {}", index);
    String result(name);
    H5free_memory(name);
    return result;
}

/// Safely extract a compound member type as an RAII handle.
HDF5Handle getMemberType(hid_t compound_type, unsigned index)
{
    hid_t member_type = H5Tget_member_type(compound_type, index);
    if (member_type < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member type at index {}", index);
    return HDF5Handle(member_type, &H5Tclose);
}

DataTypePtr hdf5TypeToClickHouse(hid_t type_id)
{
    H5T_class_t cls = H5Tget_class(type_id);
    size_t size = H5Tget_size(type_id);

    switch (cls)
    {
        case H5T_INTEGER: {
            H5T_sign_t sign = H5Tget_sign(type_id);
            if (sign == H5T_SGN_NONE)
            {
                switch (size)
                {
                    case 1: return std::make_shared<DataTypeUInt8>();
                    case 2: return std::make_shared<DataTypeUInt16>();
                    case 4: return std::make_shared<DataTypeUInt32>();
                    case 8: return std::make_shared<DataTypeUInt64>();
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported HDF5 unsigned integer size: {}", size);
                }
            }
            else
            {
                switch (size)
                {
                    case 1: return std::make_shared<DataTypeInt8>();
                    case 2: return std::make_shared<DataTypeInt16>();
                    case 4: return std::make_shared<DataTypeInt32>();
                    case 8: return std::make_shared<DataTypeInt64>();
                    default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported HDF5 signed integer size: {}", size);
                }
            }
        }

        case H5T_FLOAT: {
            if (size == 4)
                return std::make_shared<DataTypeFloat32>();
            if (size == 8)
                return std::make_shared<DataTypeFloat64>();
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported HDF5 float size: {}", size);
        }

        case H5T_STRING: {
            if (H5Tis_variable_str(type_id))
                return std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeFixedString>(size);
        }

        case H5T_COMPOUND: {
            int n_members = H5Tget_nmembers(type_id);
            if (n_members < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member count");

            auto num_members = static_cast<unsigned>(n_members);
            DataTypes types;
            Strings names;
            types.reserve(num_members);
            names.reserve(num_members);

            for (unsigned i = 0; i < num_members; ++i)
            {
                names.push_back(getMemberName(type_id, i));
                HDF5Handle member_handle = getMemberType(type_id, i);

                if (H5Tget_class(member_handle) == H5T_COMPOUND)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Nested HDF5 compound types are not supported");

                types.push_back(hdf5TypeToClickHouse(member_handle));
            }

            return std::make_shared<DataTypeTuple>(std::move(types), std::move(names));
        }

        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported HDF5 type class: {}", static_cast<int>(cls));
    }
}

void validateTypeCompatibility(hid_t hdf5_type, const DataTypePtr & ch_type, const String & column_name)
{
    DataTypePtr expected = hdf5TypeToClickHouse(hdf5_type);
    if (!expected->equals(*ch_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "HDF5 type mismatch for column '{}': file has {} but query expects {}",
            column_name,
            expected->getName(),
            ch_type->getName());
}

String getFilePath(ReadBuffer & in)
{
    String path = getFileNameFromReadBuffer(in);
    if (path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 format requires a local file path");
    return path;
}


/// Callback context for H5Literate when building schema from a group.
struct GroupSchemaContext
{
    hid_t group_id;
    NamesAndTypesList schema;
    std::optional<hsize_t> expected_rows;
};

herr_t groupSchemaCallback(hid_t group_id, const char * name, const H5L_info2_t *, void * ctx)
{
    auto & context = *static_cast<GroupSchemaContext *>(ctx);

    H5O_info2_t obj_info;
    herr_t status = H5Oget_info_by_name3(group_id, name, &obj_info, H5O_INFO_BASIC, H5P_DEFAULT);
    if (status < 0)
        return 0; /// Skip objects we cannot inspect.

    if (obj_info.type != H5O_TYPE_DATASET)
        return 0; /// Skip non-dataset children.

    HDF5Handle ds(H5Dopen2(group_id, name, H5P_DEFAULT), &H5Dclose);
    HDF5Handle space(H5Dget_space(ds), &H5Sclose);

    int ndims = H5Sget_simple_extent_ndims(space);
    if (ndims != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 dataset '{}' has {} dimensions, expected 1 for tabular layout", name, ndims);

    hsize_t dim = 0;
    H5Sget_simple_extent_dims(space, &dim, nullptr);

    if (context.expected_rows.has_value() && *context.expected_rows != dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 datasets in group have different lengths: {} vs {}", *context.expected_rows, dim);
    context.expected_rows = dim;

    HDF5Handle dtype(H5Dget_type(ds), &H5Tclose);
    context.schema.emplace_back(name, hdf5TypeToClickHouse(dtype));
    return 0;
}

} // anonymous namespace


HDF5SchemaReader::HDF5SchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_)
    , format_settings(format_settings_)
{
}

void HDF5SchemaReader::initialize()
{
    if (initialized)
        return;
    initialized = true;

    String file_path = getFilePath(in);

    std::lock_guard lock(hdf5_global_mutex);

    HDF5Handle file(H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), &H5Fclose);

    const String & dataset_path = format_settings.hdf5.dataset;

    /// Determine whether the path points to a group or a dataset.
    H5O_info2_t obj_info;
    checkHDF5(H5Oget_info_by_name3(file, dataset_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT), "Cannot resolve HDF5 path");

    if (obj_info.type == H5O_TYPE_GROUP)
    {
        /// Layout 1: flat group of 1D datasets.
        HDF5Handle group(H5Gopen2(file, dataset_path.c_str(), H5P_DEFAULT), &H5Gclose);

        GroupSchemaContext ctx;
        ctx.group_id = group;

        hsize_t idx = 0;
        checkHDF5(H5Literate2(group, H5_INDEX_NAME, H5_ITER_INC, &idx, groupSchemaCallback, &ctx), "Cannot iterate HDF5 group");

        if (ctx.schema.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 group '{}' contains no datasets", dataset_path);

        cached_schema = std::move(ctx.schema);
        cached_num_rows = ctx.expected_rows.value_or(0);
    }
    else if (obj_info.type == H5O_TYPE_DATASET)
    {
        HDF5Handle ds(H5Dopen2(file, dataset_path.c_str(), H5P_DEFAULT), &H5Dclose);
        HDF5Handle space(H5Dget_space(ds), &H5Sclose);
        HDF5Handle dtype(H5Dget_type(ds), &H5Tclose);

        int ndims = H5Sget_simple_extent_ndims(space);
        hsize_t dim = 0;
        if (ndims >= 1)
            H5Sget_simple_extent_dims(space, &dim, nullptr);

        H5T_class_t cls = H5Tget_class(dtype);
        if (cls == H5T_COMPOUND)
        {
            /// Layout 2: compound dataset - expand members into flat columns.
            int n_members_raw = H5Tget_nmembers(dtype);
            if (n_members_raw < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member count");
            auto num_members = static_cast<unsigned>(n_members_raw);

            for (unsigned i = 0; i < num_members; ++i)
            {
                String member_name = getMemberName(dtype, i);
                HDF5Handle member_handle = getMemberType(dtype, i);
                cached_schema.emplace_back(std::move(member_name), hdf5TypeToClickHouse(member_handle));
            }
            cached_num_rows = dim;
        }
        else
        {
            /// Single non-compound dataset: one column.
            if (ndims != 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "HDF5 dataset '{}' has {} dimensions, expected 1 for tabular layout", dataset_path, ndims);

            /// Use the last component of the path as column name.
            String col_name = dataset_path;
            if (auto pos = col_name.rfind('/'); pos != String::npos && pos + 1 < col_name.size())
                col_name = col_name.substr(pos + 1);

            cached_schema.emplace_back(std::move(col_name), hdf5TypeToClickHouse(dtype));
            cached_num_rows = dim;
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 path '{}' is neither a group nor a dataset", dataset_path);
    }
}

NamesAndTypesList HDF5SchemaReader::readSchema()
{
    initialize();
    return cached_schema;
}

std::optional<size_t> HDF5SchemaReader::readNumberOrRows()
{
    initialize();
    return cached_num_rows;
}


namespace
{

/// Low-level helpers for reading typed HDF5 data into ClickHouse columns.
/// Each handles one type class and takes a ready-to-use memory type for H5Dread.

void readNumericBlock(hid_t dataset, hid_t mem_type, hid_t mem_space, hid_t file_space, hsize_t count, size_t elem_size, IColumn & column)
{
    auto dest = column.insertRawUninitialized(count);

    if (dest.size() != count * elem_size)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "HDF5 type size mismatch: column element is {} bytes but HDF5 type is {} bytes",
            dest.size() / count,
            elem_size);

    checkHDF5(H5Dread(dataset, mem_type, mem_space, file_space, H5P_DEFAULT, dest.data()), "Cannot read HDF5 numeric data");
}

void readVlenStringBlock(hid_t dataset, hid_t mem_type, hid_t mem_space, hid_t file_space, hsize_t count, IColumn & column)
{
    std::vector<char *> ptrs(count, nullptr);
    SCOPE_EXIT({ H5Treclaim(mem_type, mem_space, H5P_DEFAULT, ptrs.data()); });

    checkHDF5(H5Dread(dataset, mem_type, mem_space, file_space, H5P_DEFAULT, ptrs.data()), "Cannot read HDF5 vlen string data");

    auto & str_col = assert_cast<ColumnString &>(column);
    for (hsize_t i = 0; i < count; ++i)
    {
        if (ptrs[i])
            str_col.insertData(ptrs[i], strlen(ptrs[i]));
        else
            str_col.insertDefault();
    }
}

void readFixedStringBlock(hid_t dataset, hid_t mem_type, hid_t mem_space, hid_t file_space, hsize_t count, IColumn & column)
{
    auto dest = column.insertRawUninitialized(count);
    checkHDF5(H5Dread(dataset, mem_type, mem_space, file_space, H5P_DEFAULT, dest.data()), "Cannot read HDF5 fixed string data");
}

/// Build a variable-length string memory type matching the file's charset.
HDF5Handle makeVlenStringType(hid_t file_type)
{
    HDF5Handle vlen_type(H5Tcopy(H5T_C_S1), &H5Tclose);
    H5Tset_size(vlen_type, H5T_VARIABLE);
    H5Tset_cset(vlen_type, H5Tget_cset(file_type));
    return vlen_type;
}

/// Dispatch a block read based on HDF5 type class.
/// native_type determines the class and element size; mem_type is passed to H5Dread
/// (they differ for compound field extraction where mem_type is a single-member compound wrapper).
void readTypedBlock(hid_t dataset, hid_t mem_type, hid_t mem_space, hid_t file_space, hsize_t count, hid_t native_type, IColumn & column)
{
    H5T_class_t cls = H5Tget_class(native_type);
    size_t elem_size = H5Tget_size(native_type);

    if (cls == H5T_INTEGER || cls == H5T_FLOAT)
        readNumericBlock(dataset, mem_type, mem_space, file_space, count, elem_size, column);
    else if (cls == H5T_STRING)
    {
        if (H5Tis_variable_str(native_type))
            readVlenStringBlock(dataset, mem_type, mem_space, file_space, count, column);
        else
            readFixedStringBlock(dataset, mem_type, mem_space, file_space, count, column);
    }
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported HDF5 type class: {}", static_cast<int>(cls));
}

/// Read a 1D dataset (or slice) into a ClickHouse column.
/// Called under hdf5_global_mutex.
void readDatasetIntoColumn(hid_t dataset, hid_t file_dataspace, hid_t file_datatype, hsize_t start, hsize_t count, IColumn & column)
{
    checkHDF5(H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, &start, nullptr, &count, nullptr), "Cannot select HDF5 hyperslab");
    HDF5Handle mem_space(H5Screate_simple(1, &count, nullptr), &H5Sclose);

    HDF5Handle native_type(H5Tget_native_type(file_datatype, H5T_DIR_DEFAULT), &H5Tclose);

    /// For variable-length strings, build a charset-preserving memory type.
    hid_t mem_type = native_type;
    HDF5Handle vlen_type;
    if (H5Tget_class(native_type) == H5T_STRING && H5Tis_variable_str(native_type))
    {
        vlen_type = makeVlenStringType(file_datatype);
        mem_type = vlen_type;
    }

    readTypedBlock(dataset, mem_type, mem_space, file_dataspace, count, native_type, column);
}

/// Read one field of a compound dataset into a column.
/// Uses the HDF5 trick of creating a memory compound type with a single member
/// at offset 0 - H5Dread then extracts just that field.
void readCompoundFieldIntoColumn(
    hid_t dataset, hid_t file_dataspace, hid_t file_compound_type, unsigned member_index, hsize_t start, hsize_t count, IColumn & column)
{
    String member_name = getMemberName(file_compound_type, member_index);
    HDF5Handle member_type = getMemberType(file_compound_type, member_index);
    HDF5Handle native_member(H5Tget_native_type(member_type, H5T_DIR_DEFAULT), &H5Tclose);

    checkHDF5(H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, &start, nullptr, &count, nullptr), "Cannot select HDF5 hyperslab");
    HDF5Handle mem_space(H5Screate_simple(1, &count, nullptr), &H5Sclose);

    /// For vlen strings, use a charset-preserving type; for others, use the native type.
    hid_t inner_type = native_member;
    HDF5Handle vlen_type;
    if (H5Tget_class(native_member) == H5T_STRING && H5Tis_variable_str(native_member))
    {
        vlen_type = makeVlenStringType(member_type);
        inner_type = vlen_type;
    }

    /// Build a single-member compound type for field extraction.
    HDF5Handle mem_compound(H5Tcreate(H5T_COMPOUND, H5Tget_size(inner_type)), &H5Tclose);
    checkHDF5(H5Tinsert(mem_compound, member_name.c_str(), 0, inner_type), "Cannot create HDF5 compound extraction type");

    readTypedBlock(dataset, mem_compound, mem_space, file_dataspace, count, native_member, column);
}

} // anonymous namespace


HDF5BlockInputFormat::HDF5BlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , format_settings(format_settings_)
{
}

HDF5BlockInputFormat::~HDF5BlockInputFormat()
{
    closeHandles();
}

void HDF5BlockInputFormat::closeHandles()
{
    std::lock_guard lock(hdf5_global_mutex);
    datasets.clear();
    compound_member_indices.clear();
    compound_dataset = {};
    compound_dataspace = {};
    compound_datatype = {};
    file_handle = {};
}

void HDF5BlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    closeHandles();
    rows_read = 0;
    total_rows = 0;
    reader_prepared = false;
    is_compound = false;
}

void HDF5BlockInputFormat::prepareReader()
{
    if (reader_prepared)
        return;
    reader_prepared = true;

    String file_path = getFilePath(*in);

    std::lock_guard lock(hdf5_global_mutex);

    file_handle = HDF5Handle(H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), &H5Fclose);

    const String & dataset_path = format_settings.hdf5.dataset;

    H5O_info2_t obj_info;
    checkHDF5(H5Oget_info_by_name3(file_handle, dataset_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT), "Cannot resolve HDF5 path");

    const auto & header = getPort().getHeader();

    if (obj_info.type == H5O_TYPE_GROUP)
    {
        /// Layout 1: flat group of 1D datasets.
        HDF5Handle group(H5Gopen2(file_handle, dataset_path.c_str(), H5P_DEFAULT), &H5Gclose);

        for (size_t col = 0; col < header.columns(); ++col)
        {
            const String & col_name = header.getByPosition(col).name;
            const DataTypePtr & col_type = header.getByPosition(col).type;

            HDF5DatasetInfo info;
            info.name = col_name;
            info.ch_type = col_type;

            info.dataset = HDF5Handle(H5Dopen2(group, col_name.c_str(), H5P_DEFAULT), &H5Dclose);
            info.dataspace = HDF5Handle(H5Dget_space(info.dataset), &H5Sclose);
            info.datatype = HDF5Handle(H5Dget_type(info.dataset), &H5Tclose);

            validateTypeCompatibility(info.datatype, col_type, col_name);

            hsize_t dim = 0;
            H5Sget_simple_extent_dims(info.dataspace, &dim, nullptr);
            info.num_rows = dim;

            if (total_rows == 0)
                total_rows = dim;
            else if (total_rows != dim)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 datasets in group have different lengths: {} vs {}", total_rows, dim);

            datasets.push_back(std::move(info));
        }
    }
    else if (obj_info.type == H5O_TYPE_DATASET)
    {
        HDF5Handle ds(H5Dopen2(file_handle, dataset_path.c_str(), H5P_DEFAULT), &H5Dclose);
        HDF5Handle space(H5Dget_space(ds), &H5Sclose);
        HDF5Handle dtype(H5Dget_type(ds), &H5Tclose);

        hsize_t dim = 0;
        int ndims = H5Sget_simple_extent_ndims(space);
        if (ndims >= 1)
            H5Sget_simple_extent_dims(space, &dim, nullptr);
        total_rows = dim;

        H5T_class_t cls = H5Tget_class(dtype);
        if (cls == H5T_COMPOUND)
        {
            is_compound = true;

            compound_dataset = std::move(ds);
            compound_dataspace = std::move(space);
            compound_datatype = std::move(dtype);

            for (size_t col = 0; col < header.columns(); ++col)
            {
                const String & col_name = header.getByPosition(col).name;

                int m = H5Tget_member_index(compound_datatype, col_name.c_str());
                if (m < 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in HDF5 compound dataset", col_name);

                HDF5Handle member_handle = getMemberType(compound_datatype, static_cast<unsigned>(m));
                validateTypeCompatibility(member_handle, header.getByPosition(col).type, col_name);
                compound_member_indices.push_back(static_cast<unsigned>(m));
            }
        }
        else
        {
            /// Single non-compound dataset. Treat as one column.
            is_compound = false;

            validateTypeCompatibility(dtype, header.getByPosition(0).type, header.getByPosition(0).name);

            HDF5DatasetInfo info;
            info.name = header.getByPosition(0).name;
            info.ch_type = header.getByPosition(0).type;
            info.dataset = std::move(ds);
            info.dataspace = std::move(space);
            info.datatype = std::move(dtype);
            info.num_rows = dim;
            datasets.push_back(std::move(info));
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 path '{}' is neither a group nor a dataset", dataset_path);
    }
}

Chunk HDF5BlockInputFormat::read()
{
    prepareReader();

    if (is_stopped || rows_read >= total_rows)
        return {};

    hsize_t batch_rows = std::min(BATCH_SIZE, total_rows - rows_read);

    const auto & header = getPort().getHeader();
    size_t num_cols = header.columns();
    MutableColumns columns = header.cloneEmptyColumns();

    std::lock_guard lock(hdf5_global_mutex);

    if (is_compound)
    {
        for (size_t col = 0; col < num_cols; ++col)
        {
            readCompoundFieldIntoColumn(
                compound_dataset,
                compound_dataspace,
                compound_datatype,
                compound_member_indices[col],
                rows_read,
                batch_rows,
                *columns[col]);
        }
    }
    else
    {
        for (size_t col = 0; col < num_cols; ++col)
        {
            auto & ds = datasets[col];
            readDatasetIntoColumn(ds.dataset, ds.dataspace, ds.datatype, rows_read, batch_rows, *columns[col]);
        }
    }

    rows_read += batch_rows;
    return Chunk(std::move(columns), batch_rows);
}


void registerInputFormatHDF5(FormatFactory & factory);
void registerInputFormatHDF5(FormatFactory & factory)
{
    factory.registerInputFormat(
        "HDF5",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<HDF5BlockInputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.registerFileExtension("h5", "HDF5");
    factory.registerFileExtension("hdf5", "HDF5");

    factory.markFormatSupportsSubsetOfColumns("HDF5");
}

void registerHDF5SchemaReader(FormatFactory & factory);
void registerHDF5SchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "HDF5", [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<HDF5SchemaReader>(buf, settings); });
}

}

#else

namespace DB
{

class FormatFactory;
void registerInputFormatHDF5(FormatFactory &)
{
}
void registerHDF5SchemaReader(FormatFactory &)
{
}

}

#endif
