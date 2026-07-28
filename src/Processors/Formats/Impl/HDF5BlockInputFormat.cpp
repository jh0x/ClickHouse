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

#include <charconv>
#include <mutex>
#include <ranges>

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


/// Hyperslab parameter set [start]:[stride]:[count]:[block]
struct HDF5HyperslabParams
{
    std::optional<Int64> start; /// can be negative
    std::optional<hsize_t> stride;
    std::optional<hsize_t> count;
    std::optional<hsize_t> block;
};

struct HDF5ParsedDatasetPath
{
    String path;
    std::vector<HDF5HyperslabParams> dimensions; /// Empty = no hyperslab.
};

/// Parse an HDFql like format
/// Syntax: path [ [start]:[stride]:[count]:[block] [, ...per dim] ] (note we do 1D right now)
HDF5ParsedDatasetPath parseDatasetPath(const String & setting)
{
    auto bracket_pos = setting.find('[');
    if (bracket_pos == String::npos)
        return {setting, {}};

    String path = setting.substr(0, bracket_pos);

    auto close_pos = setting.find(']', bracket_pos);
    if (close_pos == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Malformed hyperslab notation: unmatched '[' in '{}'", setting);

    /// Reject content after closing bracket (whitespace is allowed).
    for (size_t i = close_pos + 1; i < setting.size(); ++i)
        if (!isspace(setting[i]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected content after ']' in '{}'", setting);

    std::string_view spec(setting.data() + bracket_pos + 1, close_pos - bracket_pos - 1);

    /// Empty brackets = identity (no hyperslab).
    if (spec.empty())
        return {path, {}};

    auto parseToken = [&](std::string_view tok, bool allow_negative) -> std::optional<Int64>
    {
        auto begin = tok.find_first_not_of(" \t");
        if (begin == std::string_view::npos)
            return std::nullopt;
        auto end = tok.find_last_not_of(" \t") + 1;
        auto trimmed = tok.substr(begin, end - begin);

        Int64 val{};
        auto [ptr, ec] = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), val);
        if (ec != std::errc{} || ptr != trimmed.data() + trimmed.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid hyperslab parameter '{}': expected integer in '{}'", trimmed, setting);
        if (!allow_negative && val < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Negative value not allowed for this hyperslab parameter in '{}'", setting);
        return val;
    };

    /// Split on comma for per-dimension specs.
    std::vector<HDF5HyperslabParams> dims;

    for (auto dim_range : spec | std::views::split(','))
    {
        std::string_view dim_spec(dim_range.begin(), dim_range.end());

        HDF5HyperslabParams params;
        unsigned field = 0;
        for (auto tok_range : dim_spec | std::views::split(':'))
        {
            if (field >= 4)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Too many hyperslab parameters (expected at most 4: start:stride:count:block) in '{}'",
                    setting);
            if (auto val = parseToken({tok_range.begin(), tok_range.end()}, field == 0))
            {
                switch (field)
                {
                    case 0: params.start = *val; break;
                    case 1: params.stride = static_cast<hsize_t>(*val); break;
                    case 2: params.count = static_cast<hsize_t>(*val); break;
                    case 3: params.block = static_cast<hsize_t>(*val); break;
                    default: UNREACHABLE();
                }
            }
            ++field;
        }
        dims.push_back(params);
    }

    /// Check if all parameters are defaults (all nullopt) - treat as identity.
    bool all_defaults = std::ranges::all_of(dims, [](const auto & d) { return !d.start && !d.stride && !d.count && !d.block; });

    if (all_defaults)
        return {path, {}};

    return {path, std::move(dims)};
}

/// Resolve parsed hyperslab parameters against a known dimension size.
ResolvedHyperslab resolveHyperslabParams(const HDF5HyperslabParams & params, hsize_t dim_size)
{
    ResolvedHyperslab result;

    /// 1. start: default 0. Negative indexes from end.
    if (params.start)
    {
        Int64 s = *params.start;
        if (s < 0)
            s = static_cast<Int64>(dim_size) + s;
        if (s < 0 || static_cast<hsize_t>(s) >= dim_size)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Hyperslab start index {} is out of range for dimension of size {}", *params.start, dim_size);
        result.start = static_cast<hsize_t>(s);
    }
    else
    {
        result.start = 0;
    }

    /// 2. count: default 1.
    result.count = params.count.value_or(1);
    if (result.count < 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hyperslab count must be >= 1");

    /// 3. block: default (dim_size - start) / count.
    if (params.block)
    {
        result.block = *params.block;
        if (result.block < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hyperslab block must be >= 1");
    }
    else
    {
        result.block = (dim_size - result.start) / result.count;
        if (result.block < 1)
            result.block = 1;
    }

    /// 4. stride: default block (contiguous blocks).
    if (params.stride)
    {
        result.stride = *params.stride;
        if (result.stride < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hyperslab stride must be >= 1");
    }
    else
    {
        result.stride = result.block;
    }

    /// 5. total_elements.
    result.total_elements = result.count * result.block;

    /// 6. Bounds check: start + (count - 1) * stride + block <= dim_size.
    hsize_t end = result.start + (result.count - 1) * result.stride + result.block;
    if (end > dim_size)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hyperslab selection exceeds dataset dimension: "
            "start({}) + (count({})-1)*stride({}) + block({}) = {} > dim_size({})",
            result.start,
            result.count,
            result.stride,
            result.block,
            end,
            dim_size);

    return result;
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

    auto parsed = parseDatasetPath(format_settings.hdf5.dataset);
    const String & dataset_path = parsed.path;

    if (parsed.dimensions.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multi-dimensional hyperslab is not yet supported");

    std::lock_guard lock(hdf5_global_mutex);

    HDF5Handle file(H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), &H5Fclose);

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
        hsize_t dim = ctx.expected_rows.value_or(0);

        if (!parsed.dimensions.empty())
            cached_num_rows = resolveHyperslabParams(parsed.dimensions[0], dim).total_elements;
        else
            cached_num_rows = dim;
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
        }

        if (!parsed.dimensions.empty())
            cached_num_rows = resolveHyperslabParams(parsed.dimensions[0], dim).total_elements;
        else
            cached_num_rows = dim;
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
void readDatasetIntoColumn(
    hid_t dataset, hid_t file_dataspace, hid_t file_datatype, hsize_t start, hsize_t stride, hsize_t count, hsize_t block, IColumn & column)
{
    hsize_t total_elements = count * block;
    checkHDF5(H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, &start, &stride, &count, &block), "Cannot select HDF5 hyperslab");
    HDF5Handle mem_space(H5Screate_simple(1, &total_elements, nullptr), &H5Sclose);

    HDF5Handle native_type(H5Tget_native_type(file_datatype, H5T_DIR_DEFAULT), &H5Tclose);

    /// For variable-length strings, build a charset-preserving memory type.
    hid_t mem_type = native_type;
    HDF5Handle vlen_type;
    if (H5Tget_class(native_type) == H5T_STRING && H5Tis_variable_str(native_type))
    {
        vlen_type = makeVlenStringType(file_datatype);
        mem_type = vlen_type;
    }

    readTypedBlock(dataset, mem_type, mem_space, file_dataspace, total_elements, native_type, column);
}

/// Read one field of a compound dataset into a column.
/// Uses the HDF5 trick of creating a memory compound type with a single member
/// at offset 0 - H5Dread then extracts just that field.
void readCompoundFieldIntoColumn(
    hid_t dataset,
    hid_t file_dataspace,
    hid_t file_compound_type,
    unsigned member_index,
    hsize_t start,
    hsize_t stride,
    hsize_t count,
    hsize_t block,
    IColumn & column)
{
    hsize_t total_elements = count * block;

    String member_name = getMemberName(file_compound_type, member_index);
    HDF5Handle member_type = getMemberType(file_compound_type, member_index);
    HDF5Handle native_member(H5Tget_native_type(member_type, H5T_DIR_DEFAULT), &H5Tclose);

    checkHDF5(H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, &start, &stride, &count, &block), "Cannot select HDF5 hyperslab");
    HDF5Handle mem_space(H5Screate_simple(1, &total_elements, nullptr), &H5Sclose);

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

    readTypedBlock(dataset, mem_compound, mem_space, file_dataspace, total_elements, native_member, column);
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
    user_hyperslab.reset();
}

void HDF5BlockInputFormat::prepareReader()
{
    if (reader_prepared)
        return;
    reader_prepared = true;

    String file_path = getFilePath(*in);

    auto parsed = parseDatasetPath(format_settings.hdf5.dataset);
    const String & dataset_path = parsed.path;

    if (parsed.dimensions.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multi-dimensional hyperslab is not yet supported");

    std::lock_guard lock(hdf5_global_mutex);

    file_handle = HDF5Handle(H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), &H5Fclose);

    H5O_info2_t obj_info;
    checkHDF5(H5Oget_info_by_name3(file_handle, dataset_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT), "Cannot resolve HDF5 path");

    const auto & header = getPort().getHeader();

    if (obj_info.type == H5O_TYPE_GROUP)
    {
        /// Layout 1: flat group of 1D datasets.
        HDF5Handle group(H5Gopen2(file_handle, dataset_path.c_str(), H5P_DEFAULT), &H5Gclose);

        hsize_t group_dim = 0;
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

            if (group_dim == 0)
                group_dim = dim;
            else if (group_dim != dim)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 datasets in group have different lengths: {} vs {}", group_dim, dim);

            datasets.push_back(std::move(info));
        }

        if (!parsed.dimensions.empty())
        {
            user_hyperslab = resolveHyperslabParams(parsed.dimensions[0], group_dim);
            total_rows = user_hyperslab->total_elements;
        }
        else
        {
            total_rows = group_dim;
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

        if (!parsed.dimensions.empty())
        {
            user_hyperslab = resolveHyperslabParams(parsed.dimensions[0], dim);
            total_rows = user_hyperslab->total_elements;
        }
        else
        {
            total_rows = dim;
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

    /// Compute file-space hyperslab parameters for this batch.
    hsize_t file_start, file_stride, file_count, file_block, batch_elements;

    if (user_hyperslab)
    {
        const auto & hs = *user_hyperslab;
        hsize_t blocks_read = rows_read / hs.block;
        hsize_t remaining_blocks = hs.count - blocks_read;

        hsize_t batch_blocks;
        if (hs.block >= BATCH_SIZE)
            batch_blocks = 1;
        else
            batch_blocks = std::min(BATCH_SIZE / hs.block, remaining_blocks);

        file_start = hs.start + blocks_read * hs.stride;
        file_stride = hs.stride;
        file_count = batch_blocks;
        file_block = hs.block;
        batch_elements = batch_blocks * hs.block;
    }
    else
    {
        batch_elements = std::min(BATCH_SIZE, total_rows - rows_read);
        file_start = rows_read;
        file_stride = 1;
        file_count = batch_elements;
        file_block = 1;
    }

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
                file_start,
                file_stride,
                file_count,
                file_block,
                *columns[col]);
        }
    }
    else
    {
        for (size_t col = 0; col < num_cols; ++col)
        {
            auto & ds = datasets[col];
            readDatasetIntoColumn(ds.dataset, ds.dataspace, ds.datatype, file_start, file_stride, file_count, file_block, *columns[col]);
        }
    }

    rows_read += batch_elements;
    return Chunk(std::move(columns), batch_elements);
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
