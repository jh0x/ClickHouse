#include "config.h"

#if USE_HDF5

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Formats/Impl/HDF5BlockInputFormat.h>

#include <base/arithmeticOverflow.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <ranges>
#include <span>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int SUPPORT_IS_DISABLED;
}

namespace
{

std::mutex hdf5_global_mutex; // HDF5 parallel mode would use HDF5 owned global mutex instead

struct HDF5ErrorMessage
{
    static constexpr size_t capacity = 1024;
    char data[capacity]{};
    size_t size = 0;

    void append(const char * str)
    {
        if (!str)
            return;
        size_t length = std::min(strlen(str), capacity - size);
        memcpy(data + size, str, length);
        size += length;
    }

    std::string_view view() const { return {data, size}; }
};

HDF5ErrorMessage getHDF5Error()
{
    HDF5ErrorMessage msg;
    H5Ewalk2(
        H5E_DEFAULT,
        H5E_WALK_UPWARD, // innermost error first
        [](unsigned, const H5E_error2_t * err, void * ctx) -> herr_t
        {
            auto & message = *static_cast<HDF5ErrorMessage *>(ctx);
            if (message.size != 0)
                message.append("; ");
            message.append(err->desc);
            return 0;
        },
        &msg);
    H5Eclear2(H5E_DEFAULT);
    return msg;
}

/// Set by the external link traversal callback (under hdf5_global_mutex)
bool refused_external_link = false;

/// Get a better error for refused traversal than "name doesn't exist"
void checkExternalLinkWasNotRefused()
{
    if (!refused_external_link)
        return;

    refused_external_link = false;
    H5Eclear2(H5E_DEFAULT);
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The HDF5 path goes through an external link. The HDF5 format does not follow external links, because they "
        "name an object in a file chosen by the file being read rather than by the query");
}

[[noreturn]] void throwHDF5Error(int error_code, std::string_view what)
{
    checkExternalLinkWasNotRefused();
    throw Exception(error_code, "{}: {}", what, getHDF5Error().view());
}

template <typename T>
T checkHDF5(T value, std::string_view what, int error_code = ErrorCodes::INCORRECT_DATA)
{
    if (value >= 0)
        return value;

    throwHDF5Error(error_code, what);
}

/// Held while calling into libhdf5
class HDF5Lock
{
public:
    HDF5Lock()
        : lock(hdf5_global_mutex)
    {
        /// Don't let libhdf5 print to stderr.
        static std::once_flag disable_auto_error_printing;
        std::call_once(disable_auto_error_printing, [] { H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr); });
    }

    ~HDF5Lock() { refused_external_link = false; }

private:
    std::unique_lock<std::mutex> lock;
};

} // anonymous namespace

HDF5Handle::HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t), std::string_view what)
    : HDF5Handle(id_, closer_, what, ErrorCodes::INCORRECT_DATA)
{
}

HDF5Handle::HDF5Handle(hid_t id_, herr_t (*closer_)(hid_t), std::string_view what, int error_code)
    : id(id_)
    , closer(closer_)
{
    if (id >= 0)
        return;

    throwHDF5Error(error_code, what);
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

/// An external link names an object in another file - do not follow, fail the callback
herr_t refuseExternalLink(const char *, const char *, const char *, const char *, unsigned *, hid_t, void *)
{
    refused_external_link = true;
    return -1;
}

hid_t createAccessPlistRefusingExternalLinks(hid_t plist_class)
{
    hid_t plist = H5Pcreate(plist_class);
    if (plist < 0)
        return plist;

    if (H5Pset_elink_cb(plist, &refuseExternalLink, nullptr) < 0)
    {
        H5Pclose(plist);
        return H5I_INVALID_HID;
    }

    return plist;
}

hid_t checkAccessPlist(hid_t plist)
{
    if (plist < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create an HDF5 access property list");
    return plist;
}

/// Property lists for lookups - to refuse external traversal
hid_t getLinkAccessPlist()
{
    static const hid_t plist = createAccessPlistRefusingExternalLinks(H5P_LINK_ACCESS);
    return checkAccessPlist(plist);
}

hid_t getDatasetAccessPlist()
{
    static const hid_t plist = createAccessPlistRefusingExternalLinks(H5P_DATASET_ACCESS);
    return checkAccessPlist(plist);
}

hid_t getGroupAccessPlist()
{
    static const hid_t plist = createAccessPlistRefusingExternalLinks(H5P_GROUP_ACCESS);
    return checkAccessPlist(plist);
}

HDF5Handle openDataset(hid_t loc, const String & name, int error_code = ErrorCodes::INCORRECT_DATA)
{
    return HDF5Handle(
        H5Dopen2(loc, name.c_str(), getDatasetAccessPlist()),
        &H5Dclose,
        fmt::format("Cannot open the HDF5 dataset '{}'", name),
        error_code);
}

/// How many bytes of decoded chunks libhdf5 keeps per dataset when nothing says otherwise.
size_t getDefaultChunkCacheBytes()
{
    static const size_t bytes = []
    {
        HDF5Handle fapl(
            H5Pcreate(H5P_FILE_ACCESS), &H5Pclose, "Cannot create the HDF5 file access property list", ErrorCodes::LOGICAL_ERROR);
        size_t nbytes = 0;
        checkHDF5(
            H5Pget_cache(fapl, nullptr, nullptr, &nbytes, nullptr),
            "Cannot get the default HDF5 chunk cache size",
            ErrorCodes::LOGICAL_ERROR);
        return nbytes;
    }();
    return bytes;
}

/// A dataset access property list that refuses external links - and sets the chunk cache for one chunk
HDF5Handle createChunkCacheAccessPlist(size_t chunk_bytes)
{
    HDF5Handle dapl(
        createAccessPlistRefusingExternalLinks(H5P_DATASET_ACCESS),
        &H5Pclose,
        "Cannot create an HDF5 dataset access property list",
        ErrorCodes::LOGICAL_ERROR);

    checkHDF5(
        H5Pset_chunk_cache(dapl, H5D_CHUNK_CACHE_NSLOTS_DEFAULT, chunk_bytes, H5D_CHUNK_CACHE_W0_DEFAULT),
        "Cannot size the HDF5 chunk cache",
        ErrorCodes::LOGICAL_ERROR);

    return dapl;
}

HDF5Handle getDataspace(hid_t dataset)
{
    return HDF5Handle(H5Dget_space(dataset), &H5Sclose, "Cannot get HDF5 dataspace");
}

HDF5Handle getDatatype(hid_t dataset)
{
    return HDF5Handle(H5Dget_type(dataset), &H5Tclose, "Cannot get HDF5 datatype");
}

HDF5Handle getCreatePlist(hid_t dataset)
{
    return HDF5Handle(H5Dget_create_plist(dataset), &H5Pclose, "Cannot get HDF5 dataset creation property list");
}

HDF5Handle getNativeType(hid_t datatype)
{
    return HDF5Handle(H5Tget_native_type(datatype, H5T_DIR_DEFAULT), &H5Tclose, "Cannot get HDF5 native datatype");
}

/// A variable-length string is a pointer to a buffer libhdf5 allocates.
bool isVlenString(hid_t datatype)
{
    if (H5Tget_class(datatype) != H5T_STRING)
        return false;
    return checkHDF5(H5Tis_variable_str(datatype), "Cannot tell whether the HDF5 string type is variable-length") > 0;
}

String getMemberName(hid_t compound_type, unsigned index)
{
    char * name = H5Tget_member_name(compound_type, index);
    if (!name)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member name at index {}", index);
    SCOPE_EXIT({ H5free_memory(name); });
    return String(name);
}

HDF5Handle getMemberType(hid_t compound_type, unsigned index)
{
    hid_t member_type = H5Tget_member_type(compound_type, index);
    if (member_type < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get HDF5 compound member type at index {}", index);
    return HDF5Handle(member_type, &H5Tclose, "Cannot get HDF5 compound member type");
}

hsize_t getSingleDimension(hid_t dataspace, std::string_view name)
{
    int ndims = H5Sget_simple_extent_ndims(dataspace);
    if (ndims < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get the rank of the HDF5 dataspace of '{}'", name);
    if (ndims != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 dataset '{}' has {} dimensions, expected 1 for tabular layout", name, ndims);

    hsize_t dim = 0;
    checkHDF5(H5Sget_simple_extent_dims(dataspace, &dim, nullptr), "Cannot get HDF5 dataspace dimensions");
    return dim;
}

DataTypePtr hdf5TypeToClickHouse(hid_t file_type_id)
{
    /// https://support.hdfgroup.org/documentation/hdf5/latest/_h5_t__u_g.html
    H5T_class_t cls = H5Tget_class(file_type_id);
    if (cls == H5T_COMPOUND)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Nested HDF5 compound types are not supported");
    if (cls == H5T_NO_CLASS)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get the class of the HDF5 type");

    HDF5Handle native_type = getNativeType(file_type_id);
    hid_t type_id = native_type.get();
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
                    default: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported HDF5 unsigned integer size: {}", size);
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
                    default: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported HDF5 signed integer size: {}", size);
                }
            }
        }

        case H5T_FLOAT: {
            if (size == 4)
                return std::make_shared<DataTypeFloat32>();
            if (size == 8)
                return std::make_shared<DataTypeFloat64>();
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported HDF5 float size: {}", size);
        }

        case H5T_STRING: {
            if (isVlenString(type_id))
                return std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeFixedString>(size);
        }

        default: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported HDF5 type class: {}", static_cast<int>(cls));
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

void checkGroupMemberIsNotCompound(hid_t datatype, const String & group_path, const String & name)
{
    if (H5Tget_class(datatype) != H5T_COMPOUND)
        return;

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The HDF5 dataset '{}' in group '{}' has a compound type. Set 'input_format_hdf5_dataset' to "
        "'{}{}{}' to read its fields as columns",
        name,
        group_path,
        group_path,
        group_path.ends_with('/') ? "" : "/",
        name);
}

/// What the dataset's creation property list says about the way its raw data is stored.
struct HDF5DatasetLayoutInfo
{
    size_t chunk_bytes = 0;
    bool has_filters = false; /// Only a chunked layout can have any.
};

/// Refuse: virtual datasets, datasets with data in separate files, chunk >= max_chunk_size
HDF5DatasetLayoutInfo checkDatasetIsSupported(hid_t dataset, const String & name, size_t max_chunk_size)
{
    HDF5DatasetLayoutInfo layout_info;

    HDF5Handle dcpl = getCreatePlist(dataset);

    H5D_layout_t layout = checkHDF5(H5Pget_layout(dcpl), "Cannot get the HDF5 dataset layout");

    if (layout == H5D_VIRTUAL)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The HDF5 dataset '{}' is a virtual dataset. The HDF5 format does not read virtual datasets, because their "
            "source data lives in files named by the file itself rather than by the query",
            name);

    int external_count = checkHDF5(H5Pget_external_count(dcpl), "Cannot get the HDF5 external file count");

    if (external_count > 0)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The HDF5 dataset '{}' stores its data in {} external file(s). The HDF5 format does not open external data "
            "files, because they are named by the file itself rather than by the query",
            name,
            external_count);

    if (layout == H5D_CHUNKED)
    {
        hsize_t chunk_dim = 0;
        if (checkHDF5(H5Pget_chunk(dcpl, 1, &chunk_dim), "Cannot get the HDF5 chunk dimensions") == 1)
        {
            HDF5Handle datatype = getDatatype(dataset);
            size_t element_size = H5Tget_size(datatype);
            if (element_size == 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Cannot get the size of the HDF5 type of the dataset '{}': {}",
                    name,
                    getHDF5Error().view());

            size_t chunk_bytes = 0;
            if (common::mulOverflow(static_cast<size_t>(chunk_dim), element_size, chunk_bytes))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "The HDF5 chunk dimension {} is too large for the element size {}",
                    chunk_dim,
                    element_size);

            if (max_chunk_size != 0 && chunk_bytes > max_chunk_size)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The HDF5 dataset '{}' declares a chunk of {} bytes, which is over "
                    "'input_format_hdf5_max_chunk_size' ({}). Raise the setting (0 removes the limit) if the file is trusted",
                    name,
                    chunk_bytes,
                    max_chunk_size);

            layout_info.chunk_bytes = chunk_bytes;
        }

        layout_info.has_filters = checkHDF5(H5Pget_nfilters(dcpl), "Cannot get the HDF5 filter count") > 0;
    }

    return layout_info;
}

/// Only an oversized filtered chunk needs a bigger cache: an unfiltered one is read straight into the caller's buffer.
bool shouldSizeChunkCache(const HDF5DatasetLayoutInfo & layout_info)
{
    return layout_info.has_filters && layout_info.chunk_bytes > getDefaultChunkCacheBytes();
}

/// Open a dataset, refuse what this format cannot decode, and size its chunk cache when the default one cannot hold a chunk.
/// The size is only known once the dataset is open, and libhdf5 honours the access property list only on its first open, hence
/// the close and reopen below. If a concurrent query still holds the dataset open, the reopen reuses its cache instead, which
/// is only ever slower, never wrong.
///
/// `chunk_cache_budget` is how much cache the rest of this read may still ask for, and a dataset gets one only when a whole
/// chunk fits in what is left: libhdf5 bypasses a cache too small to hold one chunk, so handing out a partial remainder
/// would reserve memory that buys nothing. A dataset denied its share keeps the default cache and is only slower. The
/// budget is null for schema inference, which reads no data.
HDF5Handle openDatasetForReading(
    hid_t loc, const String & name, size_t max_chunk_size, size_t * chunk_cache_budget, int error_code = ErrorCodes::INCORRECT_DATA)
{
    size_t chunk_cache_bytes = 0;

    {
        HDF5Handle dataset = openDataset(loc, name, error_code);
        HDF5DatasetLayoutInfo layout_info = checkDatasetIsSupported(dataset, name, max_chunk_size);

        if (!chunk_cache_budget || !shouldSizeChunkCache(layout_info) || layout_info.chunk_bytes > *chunk_cache_budget)
            return dataset;

        chunk_cache_bytes = layout_info.chunk_bytes;
        *chunk_cache_budget -= chunk_cache_bytes;
    }

    /// `H5Dopen2` copies the property list into the dataset, so the list itself is only needed for
    /// the duration of the call.
    HDF5Handle dapl = createChunkCacheAccessPlist(chunk_cache_bytes);
    return HDF5Handle(H5Dopen2(loc, name.c_str(), dapl), &H5Dclose, fmt::format("Cannot open the HDF5 dataset '{}'", name), error_code);
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
    std::optional<HDF5HyperslabParams> hyperslab; /// Unset = read the whole dataset.
};

/// Parse the HDFql-like syntax: path[[start]:[stride]:[count]:[block]]
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
        if (!isspace(static_cast<unsigned char>(setting[i])))
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
        if (ec == std::errc::result_out_of_range)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Hyperslab parameter '{}' in '{}' is out of range: it must fit into Int64", trimmed, setting);
        if (ec != std::errc{} || ptr != trimmed.data() + trimmed.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid hyperslab parameter '{}': expected integer in '{}'", trimmed, setting);
        if (!allow_negative && val < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Negative value not allowed for this hyperslab parameter in '{}'", setting);
        return val;
    };

    if (spec.contains(','))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Multi-dimensional hyperslab is not yet supported: expected a single 'start:stride:count:block' in '{}'",
            setting);

    HDF5HyperslabParams params;
    unsigned field = 0;
    for (auto tok_range : spec | std::views::split(':'))
    {
        if (field >= 4)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Too many hyperslab parameters (expected at most 4: start:stride:count:block) in '{}'", setting);
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

    /// All parameters left at their defaults - treat as identity.
    if (!params.start && !params.stride && !params.count && !params.block)
        return {path, {}};

    return {path, params};
}

ResolvedHyperslab resolveHyperslabParams(const HDF5HyperslabParams & params, hsize_t dim_size)
{
    ResolvedHyperslab result;

    /// 1. start: default 0. Negative indexes from end.
    if (params.start)
    {
        Int64 s = *params.start;
        if (s < 0)
        {
            /// A dimension past `Int64`'s range cannot be indexed from the end by an `Int64` offset,
            /// and casting it would be implementation-defined.
            if (dim_size > static_cast<hsize_t>(std::numeric_limits<Int64>::max()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Hyperslab start index {} is negative, but the dataset dimension {} is too large to index from the end",
                    s,
                    dim_size);
            s = static_cast<Int64>(dim_size) + s;
        }
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

    if (common::mulOverflow(result.count, result.block, result.total_elements))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Hyperslab selection is too large: count({}) * block({}) overflows", result.count, result.block);

    hsize_t end = 0;
    if (common::mulOverflow(result.count - 1, result.stride, end) || common::addOverflow(end, result.start, end)
        || common::addOverflow(end, result.block, end))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hyperslab selection is too large: start({}) + (count({})-1)*stride({}) + block({}) overflows",
            result.start,
            result.count,
            result.stride,
            result.block);

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

    /// Overlapping blocks are refused by libhdf5 in `H5S__set_regular_hyperslab`
    if (result.count > 1 && result.stride < result.block)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hyperslab stride({}) is smaller than block({}), so the blocks overlap, which is not supported",
            result.stride,
            result.block);

    return result;
}

/// Reading the whole dataset is the identity selection, so the batching has only one shape to handle.
ResolvedHyperslab resolveSelection(const std::optional<HDF5HyperslabParams> & params, hsize_t dim)
{
    if (params)
        return resolveHyperslabParams(*params, dim);
    return {.start = 0, .stride = 1, .count = dim, .block = 1, .total_elements = dim};
}


/// One open dataset and its dataspace, which holds one selection at a time - enough, because one read fills all its columns.
struct HDF5Dataset
{
    HDF5Handle dataset;
    HDF5Handle dataspace;
};

/// One column the dataset path exposes.
struct HDF5Column
{
    String name;
    HDF5Handle datatype; /// The dataset's own type, or the compound member's.
    /// Which of `HDF5Columns::datasets` to read from; a compound dataset points every column at one.
    size_t dataset_index = 0;
    bool is_compound_member = false;
};

struct HDF5Columns
{
    std::vector<HDF5Dataset> datasets;
    /// In the order the columns were requested, which is the order of the header.
    std::vector<HDF5Column> columns;
    hsize_t dim = 0; /// The length of the datasets, before any hyperslab is applied to it.
};

/// Callback context for `H5Literate2`. It collects names only
struct GroupDatasetNames
{
    Names names;
    std::exception_ptr exception;
};

herr_t collectGroupDatasetName(hid_t group_id, const char * name, const H5L_info2_t * link_info, void * ctx)
{
    auto & context = *static_cast<GroupDatasetNames *>(ctx);
    try
    {
        /// External links (and other user-defined link classes) name an object in another file,
        /// outside the input stream. Hard and soft links point inside this file and stay.
        if (link_info && link_info->type > H5L_TYPE_BUILTIN_MAX)
            return 0;

        /// A soft link's target can be deleted or never created - not make it an error
        /// A hard link always has an object behind it - failure is an error
        /// An unresolved target comes back as `false` or as an error depending on which path
        /// component is missing; both mean the same thing here. Resolution may also have hit a
        /// refused external link, which is the same case and must not leak into the next call.
        if (link_info && link_info->type == H5L_TYPE_SOFT && H5Oexists_by_name(group_id, name, getLinkAccessPlist()) <= 0)
        {
            H5Eclear2(H5E_DEFAULT);
            refused_external_link = false;
            return 0;
        }

        H5O_info2_t obj_info;
        /// An object we cannot inspect must not silently drop a column from the schema.
        if (H5Oget_info_by_name3(group_id, name, &obj_info, H5O_INFO_BASIC, getLinkAccessPlist()) < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get info about the HDF5 object '{}': {}", name, getHDF5Error().view());

        if (obj_info.type == H5O_TYPE_DATASET)
            context.names.emplace_back(name);
        return 0;
    }
    catch (...)
    {
        context.exception = std::current_exception();
        return H5_ITER_ERROR;
    }
}

Names listGroupDatasets(hid_t group)
{
    GroupDatasetNames context;
    hsize_t idx = 0;
    herr_t iterate_status = H5Literate2(group, H5_INDEX_NAME, H5_ITER_INC, &idx, collectGroupDatasetName, &context);
    if (context.exception)
    {
        H5Eclear2(H5E_DEFAULT);
        std::rethrow_exception(context.exception);
    }
    checkHDF5(iterate_status, "Cannot iterate HDF5 group");
    return std::move(context.names);
}

/// Open the columns the path exposes: one per child 1D dataset of a group, one per member of a
/// compound dataset, or a single one for any other 1D dataset.
/// `wanted` selects and orders the columns (for reading a known header); null returns them all
/// (for schema inference).
HDF5Columns openColumns(hid_t file, const String & dataset_path, size_t max_chunk_size, size_t * chunk_cache_budget, const Names * wanted)
{
    H5O_info2_t obj_info;
    /// The path comes from `input_format_hdf5_dataset`, so a path that resolves to nothing is a
    /// mistake in the query rather than a broken file.
    checkHDF5(
        H5Oget_info_by_name3(file, dataset_path.c_str(), &obj_info, H5O_INFO_BASIC, getLinkAccessPlist()),
        fmt::format("Cannot resolve the HDF5 path '{}' from 'input_format_hdf5_dataset'", dataset_path),
        ErrorCodes::BAD_ARGUMENTS);

    HDF5Columns result;

    if (obj_info.type == H5O_TYPE_GROUP)
    {
        /// Layout 1: flat group of 1D datasets, one per column.
        HDF5Handle group(
            H5Gopen2(file, dataset_path.c_str(), getGroupAccessPlist()),
            &H5Gclose,
            fmt::format("Cannot open the HDF5 group '{}'", dataset_path));

        Names names = wanted ? *wanted : listGroupDatasets(group);

        /// Without a name to read, the number of rows would come from nowhere: this layout takes it
        /// from the datasets the columns name, unlike the other two, which take it from the dataset
        /// the path resolves to. Returning no rows for a group that has them would be a wrong
        /// answer rather than a missing one, so it is refused.
        if (names.empty())
        {
            if (wanted)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No columns to read from HDF5 group '{}'", dataset_path);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 group '{}' contains no datasets", dataset_path);
        }

        for (const auto & name : names)
        {
            /// Only `wanted` names come from the query; the ones the group yielded are known to
            /// name an object in it.
            if (wanted && H5Lexists(group, name.c_str(), getLinkAccessPlist()) <= 0)
            {
                /// The lookup may have been refused because the name resolves through an external
                /// link - say so rather than reporting a column that is not there.
                checkExternalLinkWasNotRefused();
                H5Eclear2(H5E_DEFAULT);
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in HDF5 group '{}'", name, dataset_path);
            }

            HDF5Dataset source;
            /// The name exists, but it may still not be a readable dataset - a subgroup, or a soft
            /// link with nothing behind it.
            source.dataset = openDatasetForReading(
                group, name, max_chunk_size, chunk_cache_budget, wanted ? ErrorCodes::BAD_ARGUMENTS : ErrorCodes::INCORRECT_DATA);
            source.dataspace = getDataspace(source.dataset);

            HDF5Column column;
            column.name = name;
            column.datatype = getDatatype(source.dataset);
            column.dataset_index = result.datasets.size();
            checkGroupMemberIsNotCompound(column.datatype, dataset_path, name);

            hsize_t dim = getSingleDimension(source.dataspace, name);
            if (!result.columns.empty() && result.dim != dim)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 datasets in group have different lengths: {} vs {}", result.dim, dim);
            result.dim = dim;

            result.datasets.push_back(std::move(source));
            result.columns.push_back(std::move(column));
        }

        return result;
    }

    if (obj_info.type != H5O_TYPE_DATASET)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDF5 path '{}' is neither a group nor a dataset", dataset_path);

    HDF5Dataset source;
    source.dataset = openDatasetForReading(file, dataset_path, max_chunk_size, chunk_cache_budget);
    source.dataspace = getDataspace(source.dataset);
    HDF5Handle datatype = getDatatype(source.dataset);

    result.dim = getSingleDimension(source.dataspace, dataset_path);

    if (H5Tget_class(datatype) != H5T_COMPOUND)
    {
        /// Layout 3: a single non-compound dataset, which is exactly one column.
        if (wanted && wanted->size() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "HDF5 dataset '{}' is a single 1D dataset and maps to exactly one column, but {} columns were requested",
                dataset_path,
                wanted->size());

        HDF5Column column;

        /// Named after the last path component; a trailing slash names the same object.
        column.name = dataset_path;
        while (column.name.size() > 1 && column.name.back() == '/')
            column.name.pop_back();
        if (auto pos = column.name.rfind('/'); pos != String::npos && pos + 1 < column.name.size())
            column.name = column.name.substr(pos + 1);

        if (wanted && wanted->front() != column.name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' not found in HDF5 dataset '{}', which is a single column named '{}'",
                wanted->front(),
                dataset_path,
                column.name);

        column.datatype = std::move(datatype);

        result.datasets.push_back(std::move(source));
        result.columns.push_back(std::move(column));
        return result;
    }

    /// Layout 2: compound dataset - one member per column. Every column is read from the one open
    /// dataset, because one read of a record holding all the requested members fills all of them.
    auto num_members = static_cast<unsigned>(checkHDF5(H5Tget_nmembers(datatype), "Cannot get HDF5 compound member count"));

    Names names;
    if (wanted)
    {
        names = *wanted;
    }
    else
    {
        names.reserve(num_members);
        for (unsigned i = 0; i < num_members; ++i)
            names.push_back(getMemberName(datatype, i));
    }

    result.datasets.push_back(std::move(source));

    for (const auto & name : names)
    {
        int member_index = H5Tget_member_index(datatype, name.c_str());
        if (member_index < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in HDF5 compound dataset", name);

        HDF5Column column;
        column.name = name;
        /// The member type, not the compound: `H5Dread` matches the members of the memory type
        /// against the file's by name, so the index is not needed past this point.
        column.datatype = getMemberType(datatype, static_cast<unsigned>(member_index));
        column.is_compound_member = true;

        result.columns.push_back(std::move(column));
    }

    return result;
}

/// libhdf5 decodes the whole object header, B-tree and dataspace structure of the file before any
/// of this format's own checks run, so the format only runs in clickhouse-local, where the file is
/// one the user could already read by other means. The experimental gate is checked first so that
/// someone who has not opted in is told how to enable the format rather than about a restriction
/// that is not what stopped them.
void checkHDF5IsAvailable(const FormatSettings & format_settings)
{
    if (!format_settings.hdf5.allow_experimental)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED, "The HDF5 format is experimental. Set `allow_experimental_hdf5_format = 1` to enable it");

    if (!format_settings.is_clickhouse_local)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The HDF5 format is only available in clickhouse-local");
}

/// libhdf5 needs random access over the whole file, so it opens the path itself instead of reading
/// the format's input stream. Only a plain local file has a path to give it.
/// VFD could be implemented - but with much added complexity
String resolveLocalFilePath(ReadBuffer & in)
{
    size_t view_offset = 0;
    auto * file_in = dynamic_cast<ReadBufferFromFileBase *>(&in);
    if (!file_in || !file_in->isRegularLocalFile(&view_offset) || view_offset != 0)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "The HDF5 format reads a regular local file directly, so it cannot read from a stream, a compressed file, "
            "or a remote source");

    return file_in->getFileName();
}

/// Every public HDF5 call clears the error stack on entry, so a plain `H5Pclose` on a failure path
/// would discard the error the caller is about to report.
void closePropertyListPreservingError(hid_t plist)
{
    hid_t saved_stack = H5Eget_current_stack();
    H5Pclose(plist);
    if (saved_stack >= 0)
        H5Eset_current_stack(saved_stack);
}

HDF5Handle openHDF5File(const String & path)
{
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    if (fapl < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create the HDF5 file access property list: {}", getHDF5Error().view());

    SCOPE_EXIT({ closePropertyListPreservingError(fapl); });

    /// Pin the driver and the object layer, so that the HDF5_DRIVER and HDF5_VOL_CONNECTOR
    /// environment variables cannot redirect the read somewhere else.
    checkHDF5(H5Pset_fapl_sec2(fapl), "Cannot select the HDF5 file driver", ErrorCodes::LOGICAL_ERROR);
    checkHDF5(H5Pset_vol(fapl, H5VL_NATIVE, nullptr), "Cannot select the HDF5 object layer", ErrorCodes::LOGICAL_ERROR);

    /// The file is only ever read, let's not lock it
    checkHDF5(
        H5Pset_file_locking(fapl, /*use_file_locking=*/false, /*ignore_when_disabled=*/true),
        "Cannot disable HDF5 file locking",
        ErrorCodes::LOGICAL_ERROR);

    return HDF5Handle(H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl), &H5Fclose, "Cannot open HDF5 file");
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

    cached_schema.clear();
    cached_num_rows.reset();

    checkHDF5IsAvailable(format_settings);

    String path = resolveLocalFilePath(in);

    auto parsed = parseDatasetPath(format_settings.hdf5.dataset);
    const String & dataset_path = parsed.path;

    HDF5Lock lock;

    HDF5Handle file = openHDF5File(path);
    auto opened = openColumns(file, dataset_path, format_settings.hdf5.max_chunk_size, /*chunk_cache_budget=*/nullptr, /*wanted=*/nullptr);

    for (const auto & column : opened.columns)
        cached_schema.emplace_back(column.name, hdf5TypeToClickHouse(column.datatype));

    cached_num_rows = resolveSelection(parsed.hyperslab, opened.dim).total_elements;

    /// Last, so that a failure does not leave the next call reporting an empty schema.
    initialized = true;
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

/// Build a variable-length string memory type matching the file's charset.
HDF5Handle makeVlenStringType(hid_t file_type)
{
    HDF5Handle vlen_type(H5Tcopy(H5T_C_S1), &H5Tclose, "Cannot create HDF5 string datatype");

    /// A failure here has to stop the read: a type that stays fixed-length would be read as the
    /// one-byte `H5T_C_S1` it was copied from, changing what the read means.
    checkHDF5(H5Tset_size(vlen_type, H5T_VARIABLE), "Cannot make the HDF5 string datatype variable-length");

    /// A failed lookup returns `H5T_CSET_ERROR`, which must not be passed on as a charset.
    H5T_cset_t cset = checkHDF5(H5Tget_cset(file_type), "Cannot get the character set of the HDF5 string datatype");
    checkHDF5(H5Tset_cset(vlen_type, cset), "Cannot set the character set of the HDF5 string datatype");

    return vlen_type;
}

/// The type one value is read as: the file type's native type, except for a variable-length string.
HDF5Handle makeValueMemoryType(hid_t file_type)
{
    HDF5Handle mem_type = getNativeType(file_type);

    if (isVlenString(mem_type))
        mem_type = makeVlenStringType(file_type);

    return mem_type;
}

size_t getMemoryTypeSize(hid_t mem_type, const String & column_name)
{
    size_t size = H5Tget_size(mem_type);
    if (size == 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Cannot get the size of the HDF5 memory type of column '{}': {}",
            column_name,
            getHDF5Error().view());
    return size;
}

/// Numbers and pointers are aligned to their own width; a fixed-length string is a byte array.
size_t getMemoryTypeAlignment(hid_t mem_type, size_t size)
{
    if (H5Tget_class(mem_type) == H5T_STRING && !isVlenString(mem_type))
        return 1;
    return std::min<size_t>(size, 8);
}

/// The memory record that one `H5Dread` fills, and where each column's value sits in it.
struct HDF5MemoryCompound
{
    HDF5Handle type;
    size_t record_size = 0;
    bool has_vlen_member = false;
    /// One entry per requested column, in the requested order.
    std::vector<HDF5ColumnInfo> columns;
};

/// Build the compound memory type that reads the given members of a compound dataset in one pass:
/// `H5Dread` matches memory members against the file's by name and skips the rest, so any subset,
/// in any order, works. Members are aligned rather than packed to avoid unaligned loads per row,
/// and a member asked for twice is inserted once - `H5Tinsert` refuses a duplicate name.
HDF5MemoryCompound makeMemoryCompound(const std::vector<HDF5Column> & columns, const std::vector<size_t> & members)
{
    /// The member types have to stay alive until the compound they go into is created, which can
    /// only happen once every member is laid out and the record size is known.
    struct InsertedMember
    {
        const String * name;
        HDF5Handle type;
        size_t offset;
        size_t size;
    };

    HDF5MemoryCompound result;
    result.columns.reserve(members.size());

    std::vector<InsertedMember> inserted;
    std::unordered_map<std::string_view, size_t> inserted_by_name;
    size_t record_alignment = 1;

    for (size_t member : members)
    {
        const HDF5Column & column = columns[member];

        auto [it, is_new] = inserted_by_name.try_emplace(column.name, inserted.size());
        if (is_new)
        {
            HDF5Handle member_type = makeValueMemoryType(column.datatype);
            size_t size = getMemoryTypeSize(member_type, column.name);
            size_t alignment = getMemoryTypeAlignment(member_type, size);

            result.record_size = (result.record_size + alignment - 1) / alignment * alignment;
            record_alignment = std::max(record_alignment, alignment);

            inserted.push_back({.name = &column.name, .type = std::move(member_type), .offset = result.record_size, .size = size});
            result.record_size += size;
        }

        const InsertedMember & member_info = inserted[it->second];
        bool is_vlen = isVlenString(column.datatype);
        result.has_vlen_member |= is_vlen;
        result.columns.push_back(
            {.offset = member_info.offset, .element_size = member_info.size, .is_vlen_string = is_vlen, .column_index = member});
    }

    result.record_size = (result.record_size + record_alignment - 1) / record_alignment * record_alignment;

    result.type = HDF5Handle(H5Tcreate(H5T_COMPOUND, result.record_size), &H5Tclose, "Cannot create HDF5 memory compound datatype");

    for (const auto & member_info : inserted)
        checkHDF5(
            H5Tinsert(result.type, member_info.name->c_str(), member_info.offset, member_info.type),
            "Cannot create HDF5 compound extraction type");

    return result;
}

/// Group the columns by the dataset they are read from and build the memory type each read writes
/// through: one read per column when every column has its own dataset, one read for all of them for
/// a compound dataset. Nothing here depends on the batch, so it is done once per query.
std::vector<HDF5DatasetInfo> prepareDatasetReads(HDF5Columns & opened)
{
    std::vector<std::vector<size_t>> columns_by_dataset(opened.datasets.size());
    for (size_t col = 0; col < opened.columns.size(); ++col)
        columns_by_dataset[opened.columns[col].dataset_index].push_back(col);

    std::vector<HDF5DatasetInfo> result;
    result.reserve(opened.datasets.size());

    for (size_t i = 0; i < opened.datasets.size(); ++i)
    {
        const auto & members = columns_by_dataset[i];
        /// None of the three layouts produces a dataset no column reads from.
        if (members.empty())
            continue;

        HDF5DatasetInfo info;
        info.dataset = std::move(opened.datasets[i].dataset);
        info.dataspace = std::move(opened.datasets[i].dataspace);

        if (members.size() == 1 && !opened.columns[members[0]].is_compound_member)
        {
            /// `H5Dread` writes the values themselves, so a record is one value.
            const HDF5Column & column = opened.columns[members[0]];
            info.mem_type = makeValueMemoryType(column.datatype);
            info.record_size = getMemoryTypeSize(info.mem_type, column.name);
            info.has_vlen_member = isVlenString(column.datatype);
            info.columns.push_back(
                {.offset = 0, .element_size = info.record_size, .is_vlen_string = info.has_vlen_member, .column_index = members[0]});
        }
        else
        {
            HDF5MemoryCompound compound = makeMemoryCompound(opened.columns, members);
            info.mem_type = std::move(compound.type);
            info.record_size = compound.record_size;
            info.has_vlen_member = compound.has_vlen_member;
            info.columns = std::move(compound.columns);
        }

        result.push_back(std::move(info));
    }

    return result;
}

/// The values land in the column's own memory, so its element has to be exactly as wide as the
/// value written into it.
std::span<char> insertUninitialized(IColumn & column, hsize_t count, size_t element_size)
{
    auto dest = column.insertRawUninitialized(count);

    if (dest.size() != count * element_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column of type {} has {}-byte elements but the HDF5 memory type is {} bytes; "
            "`validateTypeCompatibility` should have rejected this before the read",
            column.getName(),
            dest.size() / count,
            element_size);

    return dest;
}

/// Copy one fixed-width value out of every record into the column, one record apart.
void scatterFixedWidthMember(const char * records, size_t record_size, const HDF5ColumnInfo & info, hsize_t count, IColumn & column)
{
    auto dest = insertUninitialized(column, count, info.element_size);

    const char * source = records + info.offset;
    char * target = dest.data();
    for (hsize_t i = 0; i < count; ++i)
    {
        memcpy(target, source, info.element_size);
        source += record_size;
        target += info.element_size;
    }
}

/// A variable-length string is a pointer into a buffer libhdf5 allocated, so the value has to be
/// copied into the column before the records are handed back.
void scatterVlenStringMember(const char * records, size_t record_size, const HDF5ColumnInfo & info, hsize_t count, IColumn & column)
{
    auto & str_col = assert_cast<ColumnString &>(column);

    const char * source = records + info.offset;
    for (hsize_t i = 0; i < count; ++i)
    {
        /// The record is a byte buffer libhdf5 wrote a pointer into, so the pointer has to be
        /// copied out rather than read through a cast.
        char * value = nullptr;
        memcpy(&value, source, sizeof(value));

        if (value)
            str_col.insertData(value, strlen(value));
        else
            str_col.insertDefault();

        source += record_size;
    }
}

/// Read one batch of a 1D dataset (or slice) into every ClickHouse column it feeds.
/// Called under hdf5_global_mutex.
void readIntoColumns(const HDF5DatasetInfo & info, const ResolvedHyperslab & sel, Memory<> & record_buffer, MutableColumns & columns)
{
    /// H5Sselect_hyperslab takes its parameters by pointer, so they need addressable copies.
    hsize_t start = sel.start;
    hsize_t stride = sel.stride;
    hsize_t count = sel.count;
    hsize_t block = sel.block;
    hsize_t total_elements = sel.total_elements;

    checkHDF5(H5Sselect_hyperslab(info.dataspace, H5S_SELECT_SET, &start, &stride, &count, &block), "Cannot select HDF5 hyperslab");
    HDF5Handle mem_space(H5Screate_simple(1, &total_elements, nullptr), &H5Sclose, "Cannot create HDF5 memory dataspace");

    /// A read that fills a single column with values it can write itself goes straight into the
    /// column's memory; a record buffer would only add a copy of the whole batch.
    if (info.columns.size() == 1 && !info.has_vlen_member && info.columns[0].offset == 0
        && info.columns[0].element_size == info.record_size)
    {
        const HDF5ColumnInfo & column_info = info.columns[0];
        auto dest = insertUninitialized(*columns[column_info.column_index], total_elements, column_info.element_size);
        checkHDF5(H5Dread(info.dataset, info.mem_type, mem_space, info.dataspace, H5P_DEFAULT, dest.data()), "Cannot read HDF5 data");
        return;
    }

    /// Everything else goes through a buffer of whole records: several values to take apart, or a
    /// variable-length string, whose value in the record is a pointer rather than the string.
    size_t bytes = 0;
    if (common::mulOverflow(static_cast<size_t>(total_elements), info.record_size, bytes))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The HDF5 batch of {} records of {} bytes is too large", total_elements, info.record_size);

    record_buffer.resize(bytes);

    /// A read that fails partway leaves part of the buffer untouched, and the reclaim below walks
    /// all of it, so an untouched member has to be a null pointer rather than a stale one.
    if (info.has_vlen_member)
        memset(record_buffer.data(), 0, bytes);

    /// One walk of the buffer frees every string libhdf5 allocated for it. It has to happen even
    /// when a column below throws, and is skipped entirely for a type with nothing to free, because
    /// the walk visits every element either way.
    SCOPE_EXIT({
        if (info.has_vlen_member)
            H5Treclaim(info.mem_type, mem_space, H5P_DEFAULT, record_buffer.data());
    });

    checkHDF5(H5Dread(info.dataset, info.mem_type, mem_space, info.dataspace, H5P_DEFAULT, record_buffer.data()), "Cannot read HDF5 data");

    for (const HDF5ColumnInfo & column_info : info.columns)
    {
        IColumn & column = *columns[column_info.column_index];
        if (column_info.is_vlen_string)
            scatterVlenStringMember(record_buffer.data(), info.record_size, column_info, total_elements, column);
        else
            scatterFixedWidthMember(record_buffer.data(), info.record_size, column_info, total_elements, column);
    }
}

} // anonymous namespace


HDF5BlockInputFormat::HDF5BlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , format_settings(format_settings_)
    , batch_size(format_settings_.hdf5.max_block_size)
{
}

HDF5BlockInputFormat::~HDF5BlockInputFormat()
{
    closeHandles();
}

void HDF5BlockInputFormat::closeHandles()
{
    HDF5Lock lock;
    datasets.clear();
    file_handle = {};
    /// Nothing in the buffer outlives the read that filled it - every pointer libhdf5 put there was
    /// reclaimed before that read returned - so this only gives the memory back.
    record_buffer = {};
}

void HDF5BlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    closeHandles();
    rows_read = 0;
    reader_prepared = false;
    /// Reading starts over, so a cancellation the previous read saw must not end this one.
    is_stopped = 0;
    selection = {};
}

void HDF5BlockInputFormat::prepareReader()
{
    if (reader_prepared)
        return;

    checkHDF5IsAvailable(format_settings);

    if (!in)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The HDF5 format has no read buffer to take the file path from. "
            "`setReadBuffer` has to be called before the format is read from again");

    String path = resolveLocalFilePath(getReadBuffer());

    auto parsed = parseDatasetPath(format_settings.hdf5.dataset);
    const String & dataset_path = parsed.path;

    const auto & header = getPort().getHeader();
    Names wanted = header.getNames();

    HDF5Lock lock;

    file_handle = openHDF5File(path);

    /// `input_format_hdf5_max_chunk_size` bounds what one dataset may declare, and the same number bounds the chunk cache
    /// this read may request in total, so a group of many filtered datasets cannot multiply it by the number of columns.
    /// Zero means unlimited, as it does for the declared size.
    size_t chunk_cache_budget
        = format_settings.hdf5.max_chunk_size ? format_settings.hdf5.max_chunk_size : std::numeric_limits<size_t>::max();

    auto opened = openColumns(file_handle, dataset_path, format_settings.hdf5.max_chunk_size, &chunk_cache_budget, &wanted);

    /// The columns come back in the order they were requested, which is the order of the header.
    for (size_t col = 0; col < opened.columns.size(); ++col)
        validateTypeCompatibility(opened.columns[col].datatype, header.getByPosition(col).type, opened.columns[col].name);

    datasets = prepareDatasetReads(opened);

    selection = resolveSelection(parsed.hyperslab, opened.dim);

    /// Last, so that a failure leaves nothing behind that would make the next read return an empty
    /// chunk - an end of the data - instead of throwing again.
    reader_prepared = true;
}

Chunk HDF5BlockInputFormat::read()
{
    if (is_stopped)
        return {};

    prepareReader();

    if (is_stopped || rows_read >= selection.total_elements)
        return {};

    if (need_only_count)
    {
        rows_read = selection.total_elements;
        return getChunkForCount(selection.total_elements);
    }

    /// Carve this batch out of the selection, as a selection of the same shape.
    ResolvedHyperslab batch;
    hsize_t blocks_read = rows_read / selection.block;
    hsize_t offset_in_block = rows_read % selection.block;

    if (selection.block > batch_size)
    {
        /// A block wider than a batch is read in pieces, one piece of one block at a time. This is
        /// the only way `offset_in_block` becomes non-zero: the branch below always reads whole
        /// blocks, and which branch is taken does not change over the read.
        batch.block = std::min(selection.block - offset_in_block, batch_size);
        batch.start = selection.start + blocks_read * selection.stride + offset_in_block;
        batch.stride = batch.block; /// Unused, and unchecked by libhdf5, with one block.
        batch.count = 1;
    }
    else
    {
        /// Otherwise take as many whole blocks as fit in a batch.
        batch.block = selection.block;
        batch.start = selection.start + blocks_read * selection.stride;
        batch.stride = selection.stride;
        batch.count = std::min(batch_size / selection.block, selection.count - blocks_read);
    }

    batch.total_elements = batch.count * batch.block;

    const auto & header = getPort().getHeader();
    MutableColumns columns = header.cloneEmptyColumns();

    {
        HDF5Lock lock;

        for (const auto & info : datasets)
        {
            /// A read cannot be interrupted, so this is the only place a cancel is observed. What
            /// one read costs is bounded by `input_format_hdf5_max_chunk_size`, since a filtered
            /// chunk is decoded whole. The batch is dropped rather than returned short - the
            /// columns filled so far are longer than the ones still empty - and `rows_read` is
            /// left where it was.
            if (is_stopped)
                return {};

            readIntoColumns(info, batch, record_buffer, columns);
        }
    }

    rows_read += batch.total_elements;
    return Chunk(std::move(columns), batch.total_elements);
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

    factory.setDocumentation(
        "HDF5",
        Documentation{
            .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

[HDF5](https://www.hdfgroup.org/solutions/hdf5/) is a container format for large scientific datasets.
A file holds a tree of groups and datasets, much like a filesystem holds directories and files.

`HDF5` is a read-only format. It is recognised automatically for files with the `.h5` and `.hdf5`
extensions.

## Availability {#availability}

`HDF5` is experimental. It is available **only in `clickhouse-local`**, and only once
[`allow_experimental_hdf5_format`](/reference/settings/session-settings/allow-experimental#allow_experimental_hdf5_format) is
enabled. Otherwise the format raises `SUPPORT_IS_DISABLED`.

The file must be a plain local file: `libhdf5` opens it by path and reads it in random order, so a
stream, a compressed file, and a remote source such as
[`url`](/reference/functions/table-functions/url) or [`s3`](/reference/functions/table-functions/s3) are
rejected.

```bash
clickhouse-local --allow_experimental_hdf5_format 1 \
    --query "SELECT * FROM file('measurements.h5')"
```

## Selecting what to read {#selecting-what-to-read}

The [`input_format_hdf5_dataset`](/reference/settings/formats/input-format#input_format_hdf5_dataset)
setting chooses the path inside the file. It defaults to `/`, the root group.

| Path points at              | Result                                                  |
|-----------------------------|---------------------------------------------------------|
| A group                     | Each child 1-dimensional dataset becomes a column       |
| A compound dataset          | Each member of the compound type becomes a column       |
| A single 1D dataset         | Exactly one column, named after the last path component |

All datasets in a group must have the same length. Only 1-dimensional datasets are supported.

```bash
clickhouse-local --query "
    SELECT * FROM file('measurements.h5')
    SETTINGS input_format_hdf5_dataset = '/run_1/sensors'"
```

## Hyperslabs {#hyperslabs}

A path may be followed by an HDFql-style hyperslab that selects part of the dataset:

```text
path[start:stride:count:block]
```

The first `[` separates the path from the hyperslab, so an object whose own name contains a `[`
cannot be named by this setting.

| Parameter | Meaning                                     | Default          |
|-----------|---------------------------------------------|------------------|
| `start`   | Index of the first element. May be negative, counting from the end | `0` |
| `stride`  | Distance between the starts of two consecutive blocks | `block`, so the blocks are adjacent |
| `count`   | Number of blocks                            | `1`              |
| `block`   | Number of elements per block                | `(dim - start) / count`, at least `1` |

Any parameter may be omitted. The defaults are resolved in the order `count`, `block`, `stride`, so a
spec that gives only `count` splits the rest of the dataset into that many adjacent blocks - it does
*not* take `count` single elements one apart. `'/data[0::2:]'` on a dataset of 1000 elements
therefore reads all 1000, as two adjacent blocks of 500, and `'/data[0:2:2:1]'` is the spec that
reads elements `0` and `2`.

Blocks must not overlap, so `stride` may not be smaller than `block` when there is more than one of
them, and the selection must fit in the dataset: `start + (count - 1) * stride + block` may not
exceed the dimension. A selection of a single block has no stride between blocks, so `stride` is
ignored for it.

```bash
# 500 elements, starting at index 100, taking every second one
clickhouse-local --query "
    SELECT * FROM file('measurements.h5')
    SETTINGS input_format_hdf5_dataset = '/data[100:2:500:1]'"

# The last 10 elements: one block of (dim - start) / 1 = 10
clickhouse-local --query "
    SELECT * FROM file('measurements.h5')
    SETTINGS input_format_hdf5_dataset = '/data[-10::]'"
```

## Data types matching {#data-types-matching}

The type is derived from the *native* type of the dataset, which follows the type's precision rather
than its storage size. A query reading an existing table must name types that match exactly; the
format does not cast.

| HDF5 data type          | ClickHouse data type                                         |
|-------------------------|--------------------------------------------------------------|
| 8/16/32/64-bit signed   | [Int8/Int16/Int32/Int64](/reference/data-types/int-uint)     |
| 8/16/32/64-bit unsigned | [UInt8/UInt16/UInt32/UInt64](/reference/data-types/int-uint) |
| 32-bit float            | [Float32](/reference/data-types/float)                       |
| 64-bit float            | [Float64](/reference/data-types/float)                       |
| Fixed-length string     | [FixedString(N)](/reference/data-types/fixedstring)          |
| Variable-length string  | [String](/reference/data-types/string)                       |

Enumerations, arrays, bitfields, opaque types, references and nested compound types are not
supported.

## Compression {#compression}

Chunked datasets are decoded by `libhdf5`, so its built-in filters - including `deflate`, `shuffle`,
`fletcher32`, `nbit` and `scaleoffset` - work without any configuration. Filter plugins are not
loaded: a dataset using a filter that is not built in is an error, and the `HDF5_PLUGIN_PATH`
environment variable has no effect.

## Datasets that are refused {#datasets-that-are-refused}

Virtual datasets and datasets with external data storage name the files they read inside the file
itself, which would turn a query over one named file into a read of paths chosen by whoever wrote it.
Both are refused, and external links inside a group are skipped for the same reason. Hard and soft
links, which stay inside the file, are followed normally.

A soft link whose target does not exist names no object, and therefore no column: iterating a group
steps past it, while naming it directly in `input_format_hdf5_dataset` is an error.

## Format settings {#format-settings}

| Setting                                                                                                                      | Description                                                                                                               | Default       |
|------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|---------------|
| [`input_format_hdf5_dataset`](/reference/settings/formats/input-format#input_format_hdf5_dataset)                            | Dataset or group path within the file, optionally with a hyperslab.                                                       | `/`           |
| [`input_format_hdf5_max_chunk_size`](/reference/settings/formats/input-format#input_format_hdf5_max_chunk_size)              | Largest decompressed chunk size a chunked dataset may declare, and the reader's total chunk cache. `0` removes the limit. | `268435456`   |
| [`input_format_hdf5_max_block_size`](/reference/settings/formats/input-format#input_format_hdf5_max_block_size)              | Maximum number of rows in one block produced by the reader.                                                               | `65409`       |
| [`allow_experimental_hdf5_format`](/reference/settings/session-settings/allow-experimental#allow_experimental_hdf5_format)   | Enables the format. Without it every read raises `SUPPORT_IS_DISABLED`.                                                   | `0`           |

`input_format_hdf5_max_chunk_size` bounds the chunk size a dataset may *declare*, refusing it before
any data is read, and bounds the total chunk cache this reader requests while reading one file, so a
group of many compressed datasets cannot multiply it by the number of columns. A dataset whose chunk
does not fit in the remaining budget keeps the 8 MiB cache `libhdf5` provides by default, which only
makes it slower.

It still does not bound everything `libhdf5` allocates: that cache, the recorded compressed chunk
length and the `deflate` output buffer all use plain `malloc` and are invisible to `max_memory_usage`.

`input_format_hdf5_max_block_size` is a bound of the other kind: it counts rows, so one batch costs
that many rows times the width of a row and a wide `FixedString` column makes a batch correspondingly
larger. That memory is allocated by ClickHouse and counts towards `max_memory_usage`.

## Concurrency {#concurrency}

`libhdf5` is built without thread safety, so all calls into it are serialized on a single
process-wide mutex. Concurrent queries reading HDF5 files are correct but do not read in parallel.
)DOCS_MD",
            .introduced_in = {26, 10}});
}

void registerHDF5SchemaReader(FormatFactory & factory);
void registerHDF5SchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "HDF5", [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<HDF5SchemaReader>(buf, settings); });

    /// We need not only file, but dataset.
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "HDF5", [](const FormatSettings & settings) { return fmt::format("dataset={}", settings.hdf5.dataset); });
}

}

#else

namespace DB
{

class FormatFactory;
void registerInputFormatHDF5(FormatFactory &);
void registerInputFormatHDF5(FormatFactory &)
{
}

void registerHDF5SchemaReader(FormatFactory &);
void registerHDF5SchemaReader(FormatFactory &)
{
}

}

#endif
