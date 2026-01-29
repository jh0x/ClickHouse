#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionHelpers.h>


#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/FieldVisitorSum.h>
#include <Common/assert_cast.h>
#include "AggregateFunctions/IAggregateFunction_fwd.h"
#include "Core/Field.h"
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

#include <type_traits>
#include <utility>
#include <vector>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename Key>
class AggregateFunctionMapDataT
{ 
public:
    using KeyT = Key;
    using MappedIndex = UInt64;
    using Map = HashMapWithStackMemory<KeyT, MappedIndex, DefaultHash<KeyT>, 10>;
    using ValueStore = std::vector<Array>;
    static constexpr bool isSorted = false;
private:
    // work_map:  Key  -->  index
    //                    ┌───────────────┐
    // value_store:       │ [0] Array     │
    //                    │ [1] Array     │
    //                    │ [2] Array     │
    //                    └───────────────┘
    Map work_map;
    ValueStore value_store;

public:
    AggregateFunctionMapDataT()
    {
        value_store.reserve(1024);
    }

    size_t mapSize() const { return work_map.size(); }

    template <typename Visitor>
    void add(const KeyT& key, const Field& value, size_t col, size_t value_array_size)
    {
        typename Map::LookupResult it;
        bool inserted;
        work_map.emplace(key, it, inserted);

        if (inserted)
        {
            it->getMapped() = value_store.size();
            auto& arr = value_store.emplace_back(value_array_size);
            arr[col] = value;
        }
        else if (!value.isNull())
        {
            auto& entry = value_store[it->getMapped()];
            if (entry[col].isNull())
                entry[col] = value;
            else
                applyVisitor(Visitor(value), entry[col]);
        }
    }

    void add(const KeyT& key, Array values)
    {
        typename Map::LookupResult it;
        bool inserted;
        work_map.emplace(key, it, inserted);
        if(inserted) [[likely]]
        {
            it->getMapped() = value_store.size();
            value_store.push_back(std::move(values));
        }
    }

    template <typename Visitor>
    void merge(const AggregateFunctionMapDataT& other, size_t value_array_size)
    {
        for (auto it = std::begin(other.work_map); it != std::end(other.work_map); ++it)
        {
            const auto& other_key = it->getKey();
            const auto other_index = it->getMapped();
            typename Map::LookupResult self_it;
            bool inserted;
            work_map.emplace(other_key, self_it, inserted);
            if (inserted)
            {
                self_it->getMapped() = value_store.size();
                value_store.push_back(other.value_store[other_index]);
            }
            else
            {
                auto& self_entry = value_store[self_it->getMapped()];
                const auto& other_entry = other.value_store[other_index];
                for (size_t i = 0; i < value_array_size; ++i)
                {
                    if (!other_entry[i].isNull())
                    {
                        if (self_entry[i].isNull())
                            self_entry[i] = other_entry[i];
                        else
                            applyVisitor(Visitor(other_entry[i]), self_entry[i]);
                    }
                }
            }
        }
    }

    template <typename F>
    void forEach(F&& f) const
    {
        for (auto it = std::begin(work_map); it != std::end(work_map); ++it)
        {
            const auto& key = it->getKey();
            const auto index = it->getMapped();
            f(key, value_store[index]);
        }
    }

    template <typename F>
    void forEachCell(F&& f) const
    {
        for (auto it = std::begin(work_map); it != std::end(work_map); ++it)
        {
            f(it);
        }
    }

    const Array& getValueByIndex(size_t index) const
    {
        return value_store[index];
    }
};

template <>
class AggregateFunctionMapDataT<Field>
{
public:
    using KeyT = Field;
    using Value = Array;
    using Map = absl::btree_map<KeyT, Value>;
    static constexpr bool isSorted = true;

private:
    Map merged_maps;

public:
    size_t mapSize() const
    {
        return merged_maps.size();
    }

    template <typename Visitor>
    void add(const KeyT & key, const Field & value, size_t col, size_t value_array_size)
    {
        auto [it, inserted] =
            merged_maps.try_emplace(key, Value(value_array_size));

        auto & entry = it->second;

        if (inserted)
        {
            entry[col] = value;
        }
        else if (!value.isNull())
        {
            if (entry[col].isNull())
                entry[col] = value;
            else
                applyVisitor(Visitor(value), entry[col]);
        }
    }

    void add(const KeyT & key, Array values)
    {
        merged_maps.try_emplace(key, std::move(values));
    }

    template <typename Visitor>
    void merge(const AggregateFunctionMapDataT & other, size_t value_array_size)
    {
        for (const auto & [other_key, other_entry] : other.merged_maps)
        {
            auto [it, inserted] =
                merged_maps.try_emplace(other_key, other_entry);

            if (!inserted)
            {
                auto & self_entry = it->second;

                for (size_t i = 0; i < value_array_size; ++i)
                {
                    if (!other_entry[i].isNull())
                    {
                        if (self_entry[i].isNull())
                            self_entry[i] = other_entry[i];
                        else
                            applyVisitor(
                                Visitor(other_entry[i]),
                                self_entry[i]);
                    }
                }
            }
        }
    }

    template <typename F>
    void forEach(F && f) const
    {
        for (const auto & [key, value] : merged_maps)
            f(key, value);
    }
};


// JH: TODO:


#define COLUMN_TYPE_TRAITS_LIST(M) \
    M(Int8,   ColumnInt8)          \
    M(UInt8,  ColumnUInt8)         \
    M(Int16,  ColumnInt16)         \
    M(UInt16, ColumnUInt16)        \
    M(Int32,  ColumnInt32)         \
    M(UInt32, ColumnUInt32)        \
    M(Int64,  ColumnInt64)         \
    M(UInt64, ColumnUInt64)

template <typename T>
struct ColumnTypeTraits;

#define DEFINE_COLUMN_TYPE_TRAITS(Type, Column) \
    template <>                                \
    struct ColumnTypeTraits<Type>              \
    {                                          \
        using ColumnType = Column;             \
    };

COLUMN_TYPE_TRAITS_LIST(DEFINE_COLUMN_TYPE_TRAITS)

#undef DEFINE_COLUMN_TYPE_TRAITS

/** Aggregate function, that takes at least two arguments: keys and values, and as a result, builds a tuple of at least 2 arrays -
  * ordered keys and variable number of argument values aggregated by corresponding keys.
  *
  * sumMap function is the most useful when using SummingMergeTree to sum Nested columns, which name ends in "Map".
  *
  * Example: sumMap(k, v...) of:
  *  k           v
  *  [1,2,3]     [10,10,10]
  *  [3,4,5]     [10,10,10]
  *  [4,5,6]     [10,10,10]
  *  [6,7,8]     [10,10,10]
  *  [7,5,3]     [5,15,25]
  *  [8,9,10]    [20,20,20]
  * will return:
  *  ([1,2,3,4,5,6,7,8,9,10],[10,10,45,20,35,20,15,30,20,20])
  *
  * minMap and maxMap share the same idea, but calculate min and max correspondingly.
  *
  * NOTE: The implementation of these functions are "amateur grade" - not efficient and low quality.
  */

class AggregateFunctionMapCommon
{
protected:
    static constexpr auto STATE_VERSION_1_MIN_REVISION = 54452;

    DataTypePtr keys_type;
    SerializationPtr keys_serialization;
    DataTypes values_types;
    Serializations values_serializations;
    Serializations promoted_values_serializations;

    explicit AggregateFunctionMapCommon(
        const DataTypePtr & keys_type_,
        const DataTypes & values_types_)
        : keys_type(keys_type_)
        , keys_serialization(keys_type_->getDefaultSerialization())
        , values_types(values_types_)
    {
        values_serializations.reserve(values_types.size());
        promoted_values_serializations.reserve(values_types.size());

        for (const auto & type : values_types)
        {
            values_serializations.emplace_back(type->getDefaultSerialization());

            if (type->canBePromoted())
            {
                if (type->isNullable())
                    promoted_values_serializations.emplace_back(
                        makeNullable(removeNullable(type)->promoteNumericType())
                            ->getDefaultSerialization());
                else
                    promoted_values_serializations.emplace_back(
                        type->promoteNumericType()->getDefaultSerialization());
            }
            else
            {
                promoted_values_serializations.emplace_back(
                    type->getDefaultSerialization());
            }
        }
    }

    size_t getVersionFromRevisionImpl(size_t revision) const
    {
        return revision >= STATE_VERSION_1_MIN_REVISION ? 1 : 0;
    }

    bool notCompacted(const Array & values) const
    {
        for (size_t col = 0; col < values_types.size(); ++col)
        {
            if (!values[col].isNull() &&
                values[col] != values_types[col]->getDefault())
                return true;
        }
        return false;
    }

    template <typename Visitor, bool overflow>
    static DataTypePtr createResultType(
        const DataTypePtr & keys_type_,
        const DataTypes & values_types_)
    {
        DataTypes types;
        types.emplace_back(std::make_shared<DataTypeArray>(keys_type_));

        for (const auto & value_type : values_types_)
        {
            if constexpr (std::is_same_v<Visitor, FieldVisitorSum>)
            {
                if (!value_type->isSummable())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for -Map cannot be summed, passed type {}",
                        value_type->getName()};
            }

            DataTypePtr result_type;

            if constexpr (overflow)
            {
                if (value_type->onlyNull())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Cannot calculate -Map of type {}",
                        value_type->getName()};

                // Overflow, meaning that the returned type is the same as
                // the input type. Nulls are skipped.
                result_type = removeNullable(value_type);
            }
            else
            {
                auto value_type_without_nullable = removeNullable(value_type);

                // No overflow, meaning we promote the types if necessary.
                if (!value_type_without_nullable->canBePromoted())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for -Map are expected to be Numeric, Float or Decimal, passed type {}",
                        value_type->getName()};

                WhichDataType value_type_to_check(value_type_without_nullable);

                /// Do not promote decimal because of implementation issues of this function design
                /// Currently we cannot get result column type in case of decimal we cannot get decimal scale
                /// in method void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
                /// If we decide to make this function more efficient we should promote decimal type during summ
                if (value_type_to_check.isDecimal())
                    result_type = value_type_without_nullable;
                else
                    result_type = value_type_without_nullable->promoteNumericType();
            }

            types.emplace_back(std::make_shared<DataTypeArray>(result_type));
        }

        return std::make_shared<DataTypeTuple>(types);
    }
};



template <typename Key, typename Derived, typename Visitor, bool overflow, bool tuple_argument, bool compact>
class AggregateFunctionMapBaseT 
    : public IAggregateFunctionDataHelper<AggregateFunctionMapDataT<Key>, Derived>
    , private AggregateFunctionMapCommon
{
private:
    using KeyT = Key;
    using State = AggregateFunctionMapDataT<KeyT>;

    static constexpr bool is_generic_field = std::is_same_v<KeyT, Field>;
    
public:
    using Base = IAggregateFunctionDataHelper<State, Derived>;

    AggregateFunctionMapBaseT(
        const DataTypePtr & keys_type_,
        const DataTypes & values_types_,
        const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType<Visitor, overflow>(keys_type_, values_types_))
        , AggregateFunctionMapCommon(keys_type_, values_types_)
    {}

    bool isVersioned() const override { return true; }

    size_t getDefaultVersion() const override { return 1; } // JH TODO ???

    size_t getVersionFromRevision(size_t revision) const override
    {
        return getVersionFromRevisionImpl(revision);
    }

    bool allocatesMemoryInArena() const override { return false; }

    static auto getArgumentColumns(const IColumn ** columns)
    {
        if constexpr (tuple_argument)
        {
            return assert_cast<const ColumnTuple *>(columns[0])->getColumns();
        }
        else
        {
            return columns;
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns_, const size_t row_num, Arena *) const override
    {
        const auto & columns = getArgumentColumns(columns_);

        // Column 0 contains array of keys of known type
        const ColumnArray & array_column0 = assert_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets0 = array_column0.getOffsets();
        const IColumn & key_column_generic = array_column0.getData();
        const size_t keys_vec_offset = offsets0[row_num - 1];
        const size_t keys_vec_size = (offsets0[row_num] - keys_vec_offset);

        const auto & key_column = [&]() -> decltype(auto) {
            if constexpr(is_generic_field)
                return key_column_generic;
            else
            {
                using ColumnType = typename ColumnTypeTraits<KeyT>::ColumnType;
                return assert_cast<const ColumnType &>(key_column_generic);
            }
        }();

        // Columns 1..n contain arrays of numeric values to sum
        auto & scratch = this->data(place);
        const auto values_types_size = values_types.size();
        for (size_t col = 0, size = values_types_size; col < size; ++col)
        {
            const auto & array_column = assert_cast<const ColumnArray &>(*columns[col + 1]);
            const IColumn & value_column = array_column.getData();
            const IColumn::Offsets & offsets = array_column.getOffsets();
            const size_t values_vec_offset = offsets[row_num - 1];
            const size_t values_vec_size = (offsets[row_num] - values_vec_offset);

            // Expect key and value arrays to be of same length
            if (keys_vec_size != values_vec_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of keys and values arrays do not match");

            // Insert column values for all keys
            for (size_t i = 0; i < keys_vec_size; ++i)
            {
                Field value = value_column[values_vec_offset + i];
                auto key = [&]{
                    if constexpr(is_generic_field)
                        return key_column[keys_vec_offset + i];
                    else
                        return key_column.getElement(keys_vec_offset + i);
                }();

                if (!keepKey(key))
                    continue;

                scratch.template add<Visitor>(key, value, col, values_types_size);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & our_scratch = this->data(place);
        const auto & their_scratch = this->data(rhs);
        our_scratch.template merge<Visitor>(their_scratch, values_types.size());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if (!version)
            version = getDefaultVersion();

        const auto & scratch = this->data(place);
        size_t size = scratch.mapSize();
        writeVarUInt(size, buf);

        std::function<void(size_t, const Array &)> serialize;
        switch (*version)
        {
            case 0:
            {
                serialize = [&](size_t col_idx, const Array & values)
                {
                    values_serializations[col_idx]->serializeBinary(values[col_idx], buf, {});
                };
                break;
            }
            case 1:
            {
                serialize = [&](size_t col_idx, const Array & values)
                {
                    Field value = values[col_idx];

                    /// Compatibility with previous versions.
                    WhichDataType value_type(values_types[col_idx]);
                    if (value_type.isDecimal32())
                    {
                        auto source = value.safeGet<DecimalField<Decimal32>>();
                        value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                    }
                    else if (value_type.isDecimal64())
                    {
                        auto source = value.safeGet<DecimalField<Decimal64>>();
                        value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                    }

                    promoted_values_serializations[col_idx]->serializeBinary(value, buf, {});
                };
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown version {}, of -Map aggregate function serialization state", *version);
        }

        scratch.forEach([&](const auto & key, const Array & values)
        {
            if constexpr (is_generic_field)
                keys_serialization->serializeBinary(key, buf, {});
            else
                writeBinary(key, buf);
            for (size_t col = 0; col < values_types.size(); ++col)
                serialize(col, values);
        });
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const override
    {
        if (!version)
            version = getDefaultVersion();

        auto & scratch = this->data(place);
        size_t size = 0;
        readVarUInt(size, buf);

        FormatSettings format_settings;
        std::function<void(size_t, Array &)> deserialize;
        switch (*version)
        {
            case 0:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    values_serializations[col_idx]->deserializeBinary(values[col_idx], buf, format_settings);
                };
                break;
            }
            case 1:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    Field & value = values[col_idx];
                    promoted_values_serializations[col_idx]->deserializeBinary(value, buf, format_settings);

                    /// Compatibility with previous versions.
                    if (value.getType() == Field::Types::Decimal128)
                    {
                        auto source = value.safeGet<DecimalField<Decimal128>>();
                        WhichDataType value_type(values_types[col_idx]);
                        if (value_type.isDecimal32())
                        {
                            value = DecimalField<Decimal32>(source.getValue(), source.getScale());
                        }
                        else if (value_type.isDecimal64())
                        {
                            value = DecimalField<Decimal64>(source.getValue(), source.getScale());
                        }
                    }
                };
                break;
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected version {} of -Map aggregate function serialization state", *version);
        }

        for (size_t i = 0; i < size; ++i)
        {
            KeyT key;
            if constexpr (is_generic_field)
                keys_serialization->deserializeBinary(key, buf, format_settings);
            else
                readBinary(key, buf);

            Array values;
            values.resize(values_types.size());

            for (size_t col = 0; col < values_types.size(); ++col)
                deserialize(col, values);

            scratch.add(key, std::move(values));
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        size_t num_columns = values_types.size();

        // Final step does compaction of keys that have zero values, this mutates the state
        auto & scratch = this->data(place);

        size_t size = [&]{
            if constexpr (!compact) {
                return scratch.mapSize();
            }
            else
            {
                size_t res{};
                scratch.forEach([&](const auto & /*key*/, const Array & values)
                {
                    res += notCompacted(values);
                });
                return res;
            }
        }();

        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        // Advance column offsets
        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back(to_keys_offsets.back() + size);
        to_keys_col.reserve(size);

        for (size_t col = 0; col < num_columns; ++col)
        {
            auto & to_values_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1));
            auto & to_values_offsets = to_values_arr.getOffsets();
            to_values_offsets.push_back(to_values_offsets.back() + size);
            to_values_arr.getData().reserve(size);
        }

        if constexpr (State::isSorted) {
            // For Field keys, the map is already sorted (absl::btree_map), so we can iterate directly
            scratch.forEach([&](const auto & key, const Array & values)
            {
                if constexpr (compact)
                {
                    if (!notCompacted(values))
                        return;
                }
                to_keys_col.insert(key);
                // Write 0..n arrays of values
                for (size_t col = 0; col < num_columns; ++col)
                {
                    auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                    if (values[col].isNull())
                        to_values_col.insertDefault();
                    else
                        to_values_col.insert(values[col]);
                }
            });
        } else {
            // For non-Field keys, we need to sort the results to ensure consistent order
            // Create a vector of iterators to the hash map entries and sort them by key
            std::vector<typename State::Map::const_iterator> iterators;
            iterators.reserve(scratch.mapSize());

            scratch.forEachCell([&iterators](auto it) {
                iterators.push_back(it);
            });

            std::sort(iterators.begin(), iterators.end(),
                      [](const auto & a, const auto & b) {
                          return a->getKey() < b->getKey();
                      });

            for (const auto & it : iterators) {
                const auto & key = it->getKey();
                const auto & values = scratch.getValueByIndex(it->getMapped());

                if constexpr (compact)
                {
                    if (!notCompacted(values))
                        continue;
                }

                to_keys_col.insert(key);
                // Write 0..n arrays of values
                for (size_t col = 0; col < num_columns; ++col)
                {
                    auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                    if (values[col].isNull())
                        to_values_col.insertDefault();
                    else
                        to_values_col.insert(values[col]);
                }
            }
        }
    }

    bool keepKey(const std::conditional_t<is_generic_field, Field, KeyT> & key) const { return static_cast<const Derived &>(*this).keepKey(key); }
    String getName() const override { return Derived::getNameImpl(); }
};

template <typename Key, bool overflow, bool tuple_argument>
class AggregateFunctionSumMap final :
    public AggregateFunctionMapBaseT<
        Key,
        AggregateFunctionSumMap<Key, overflow, tuple_argument>,
        FieldVisitorSum,
        overflow,
        tuple_argument,
        true>
{
private:
    using KeyT = Key;
    using Self = AggregateFunctionSumMap<KeyT, overflow, tuple_argument>;
    using Base = AggregateFunctionMapBaseT<KeyT, Self, FieldVisitorSum, overflow, tuple_argument, true>;

public:
    AggregateFunctionSumMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl()
    {
        if constexpr (overflow)
        {
            return "sumMapWithOverflow";
        }
        else
        {
            return "sumMap";
        }
    }

    bool keepKey(const Field &) const { return true; }
};

class KeySet
{
    using ContainerT = absl::btree_set<Field>;

    ContainerT set;

public:
    template <typename... Args>
    bool emplace(Args&&... args)
    {
        auto [it, inserted] = set.emplace(std::forward<Args>(args)...);
        return inserted;
    }

    bool contains(const Field& value) const
    {
        return set.contains(value);
    }
};
// JH We'd template above and use for Field, and HashSet for others, but few Field related questions below...


template <typename Key, bool overflow, bool tuple_argument>
class AggregateFunctionSumMapFiltered final :
    public AggregateFunctionMapBaseT<
        Key,
        AggregateFunctionSumMapFiltered<Key, overflow, tuple_argument>,
        FieldVisitorSum,
        overflow,
        tuple_argument,
        true>
{
private:
    using KeyT = Key;
    using Self = AggregateFunctionSumMapFiltered<KeyT, overflow, tuple_argument>;
    using Base = AggregateFunctionMapBaseT<KeyT, Self, FieldVisitorSum, overflow, tuple_argument, true>;

    using ContainerT = KeySet; // JH FIXME - maybe - there's some field business below...
    ContainerT keys_to_keep;

public:
    AggregateFunctionSumMapFiltered(const DataTypePtr & keys_type_,
            const DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        if (params_.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function '{}' requires exactly one parameter "
                "of Array type", getNameImpl());

        Array keys_to_keep_values;
        if (!params_.front().tryGet<Array>(keys_to_keep_values))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires an Array as a parameter",
                getNameImpl());

        this->parameters = params_;

        for (const Field & f : keys_to_keep_values)
            keys_to_keep.emplace(f); // and this - if we went for KeyT - how to handle?
    }
        

    static String getNameImpl()
    {
        if constexpr (overflow)
        {
            return "sumMapFilteredWithOverflow";
        }
        else
        {
            return "sumMapFiltered";
        }
    }

    bool keepKey(const Field & key) const
    {
        if (keys_to_keep.contains(key))
            return true;

        // JH TODO... - some field business mentioned earlier
        // Determine whether the numerical value of the key can have both types (UInt or Int),
        // and use the other type with the same numerical value for keepKey verification.
        if (key.getType() == Field::Types::UInt64)
        {
            const auto & value = key.template safeGet<UInt64>();
            if (value <= std::numeric_limits<Int64>::max())
                return keys_to_keep.contains(Field(Int64(value)));
        }
        else if (key.getType() == Field::Types::Int64)
        {
            const auto & value = key.template safeGet<Int64>();
            if (value >= 0)
                return keys_to_keep.contains(Field(UInt64(value)));
        }

        return false;
    }
};


/** Implements `Max` operation.
 *  Returns true if changed
 */
class FieldVisitorMax : public StaticVisitor<bool>
{
private:
    const Field & rhs;

    template <typename FieldType>
    bool compareImpl(FieldType & x) const
    {
        auto val = rhs.safeGet<FieldType>();
        if (val > x)
        {
            x = val;
            return true;
        }

        return false;
    }

public:
    explicit FieldVisitorMax(const Field & rhs_) : rhs(rhs_) {}

    bool operator() (Null &) const
    {
        /// Do not update current value, skip nulls
        return false;
    }

    bool operator() (AggregateFunctionStateData &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compare AggregateFunctionStates"); }

    bool operator() (Array & x) const { return compareImpl<Array>(x); }
    bool operator() (Tuple & x) const { return compareImpl<Tuple>(x); }
    template <typename T>
    bool operator() (DecimalField<T> & x) const { return compareImpl<DecimalField<T>>(x); }
    template <typename T>
    bool operator() (T & x) const { return compareImpl<T>(x); }
};

/** Implements `Min` operation.
 *  Returns true if changed
 */
class FieldVisitorMin : public StaticVisitor<bool>
{
private:
    const Field & rhs;

    template <typename FieldType>
    bool compareImpl(FieldType & x) const
    {
        auto val = rhs.safeGet<FieldType>();
        if (val < x)
        {
            x = val;
            return true;
        }

        return false;
    }

public:
    explicit FieldVisitorMin(const Field & rhs_) : rhs(rhs_) {}


    bool operator() (Null &) const
    {
        /// Do not update current value, skip nulls
        return false;
    }

    bool operator() (AggregateFunctionStateData &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum AggregateFunctionStates"); }

    bool operator() (Array & x) const { return compareImpl<Array>(x); }
    bool operator() (Tuple & x) const { return compareImpl<Tuple>(x); }
    template <typename T>
    bool operator() (DecimalField<T> & x) const { return compareImpl<DecimalField<T>>(x); }
    template <typename T>
    bool operator() (T & x) const { return compareImpl<T>(x); }
};


template <typename Key, bool tuple_argument>
class AggregateFunctionMinMap final :
    public AggregateFunctionMapBaseT<Key, AggregateFunctionMinMap<Key, tuple_argument>, FieldVisitorMin, true, tuple_argument, false>
{
private:
    using KeyT = Key;
    using Self = AggregateFunctionMinMap<KeyT, tuple_argument>;
    using Base = AggregateFunctionMapBaseT<KeyT, Self, FieldVisitorMin, true, tuple_argument, false>;

public:
    AggregateFunctionMinMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl() { return "minMap"; }

    bool keepKey(const Field &) const { return true; }
};

template <typename Key, bool tuple_argument>
class AggregateFunctionMaxMap final :
    public AggregateFunctionMapBaseT<Key, AggregateFunctionMaxMap<Key, tuple_argument>, FieldVisitorMax, true, tuple_argument, false>
{
private:
    using KeyT = Key;
    using Self = AggregateFunctionMaxMap<KeyT, tuple_argument>;
    using Base = AggregateFunctionMapBaseT<KeyT, Self, FieldVisitorMax, true, tuple_argument, false>;

public:
    AggregateFunctionMaxMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl() { return "maxMap"; }

    bool keepKey(const Field &) const { return true; }
};


auto parseArguments(const std::string & name, const DataTypes & arguments)
{
    DataTypes args;
    bool tuple_argument = false;

    if (arguments.size() == 1)
    {
        // sumMap state is fully given by its result, so it can be stored in
        // SimpleAggregateFunction columns. There is a caveat: it must support
        // sumMap(sumMap(...)), e.g. it must be able to accept its own output as
        // an input. This is why it also accepts a Tuple(keys, values) argument.
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "When function {} gets one argument it must be a tuple", name);

        const auto elems = tuple_type->getElements();
        args.insert(args.end(), elems.begin(), elems.end());
        tuple_argument = true;
    }
    else
    {
        args.insert(args.end(), arguments.begin(), arguments.end());
        tuple_argument = false;
    }

    if (args.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least two arguments of Array type or one argument of tuple of two arrays", name);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(args[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #1 for function {} must be an array, not {}",
            name, args[0]->getName());

    DataTypePtr keys_type = array_type->getNestedType();

    DataTypes values_types;
    values_types.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i)
    {
        array_type = checkAndGetDataType<DataTypeArray>(args[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} for function {} must be an array, not {}",
                i + 1, name, args[i]->getName());
        values_types.push_back(array_type->getNestedType());
    }

    return std::tuple<DataTypePtr, DataTypes, bool>{std::move(keys_type), std::move(values_types), tuple_argument};
}

}

template <typename Builder, typename... Args>
AggregateFunctionPtr createAggregateFunctionMap(
    Builder && builder,
    DataTypePtr keys_type,
    Args&&... args)
{
    WhichDataType which(keys_type);
    switch (which.idx)
    {
        case TypeIndex::UInt8:
            return builder.template operator()<UInt8>(std::forward<Args>(args)...);
        case TypeIndex::UInt16:
            return builder.template operator()<UInt16>(std::forward<Args>(args)...);
        case TypeIndex::UInt32:
            return builder.template operator()<UInt32>(std::forward<Args>(args)...);
        case TypeIndex::UInt64:
            return builder.template operator()<UInt64>(std::forward<Args>(args)...);
        case TypeIndex::Int8:
            return builder.template operator()<Int8>(std::forward<Args>(args)...);
        case TypeIndex::Int16:
            return builder.template operator()<Int16>(std::forward<Args>(args)...);
        case TypeIndex::Int32:
            return builder.template operator()<Int32>(std::forward<Args>(args)...);
        case TypeIndex::Int64:
            return builder.template operator()<Int64>(std::forward<Args>(args)...);
        default:
            return builder.template operator()<Field>(std::forward<Args>(args)...);
    }
}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    // these functions used to be called *Map, with now these names occupied by
    // Map combinator, which redirects calls here if was called with
    // array or tuple arguments.
    FunctionDocumentation::Description sumMappedArrays_description = R"(
Totals one or more `value` arrays according to the keys specified in the `key` array. Returns a tuple of arrays: keys in sorted order, followed by values summed for the corresponding keys without overflow.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and all `value` arrays must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax sumMappedArrays_syntax = R"(
sumMappedArrays(key, value1 [, value2, ...])
sumMappedArrays(Tuple(key, value1 [, value2, ...]))
    )";
    FunctionDocumentation::Arguments sumMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array"}},
        {"value1, value2, ...", "Arrays of values to sum for each key.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue sumMappedArrays_returned_value = {"Returns a tuple of arrays: the first array contains keys in sorted order, followed by arrays containing values summed for the corresponding keys.", {"Tuple"}};
    FunctionDocumentation::Examples sumMappedArrays_examples = {
    {
        "Basic usage with Nested type",
        R"(
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Memory;

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMappedArrays(statusMap.status, statusMap.requests),
    sumMappedArrays(statusMapTuple)
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─sumMappedArrays(statusMap.status, statusMap.requests)─┬─sumMappedArrays(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])                        │ ([1,2,3,4,5],[10,10,20,10,10])          │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])                        │ ([4,5,6,7,8],[10,10,20,10,10])          │
└─────────────────────┴───────────────────────────────────────────────────────┴─────────────────────────────────────────┘
        )"
    },
    {
        "Multiple value arrays example",
        R"(
CREATE TABLE multi_metrics(
    date Date,
    browser_metrics Nested(
        browser String,
        impressions UInt32,
        clicks UInt32
    )
)
ENGINE = Memory;

INSERT INTO multi_metrics VALUES
    ('2000-01-01', ['Firefox', 'Chrome'], [100, 200], [10, 25]),
    ('2000-01-01', ['Chrome', 'Safari'], [150, 50], [20, 5]),
    ('2000-01-01', ['Firefox', 'Edge'], [80, 40], [8, 4]);

SELECT
    sumMappedArrays(browser_metrics.browser, browser_metrics.impressions, browser_metrics.clicks) AS result
FROM multi_metrics;
        )",
        R"(
┌─result────────────────────────────────────────────────────────────────────────┐
│ (['Chrome', 'Edge', 'Firefox', 'Safari'], [350, 40, 180, 50], [45, 4, 18, 5]) │
└───────────────────────────────────────────────────────────────────────────────┘
-- In this example:
-- The result tuple contains three arrays
-- First array: keys (browser names) in sorted order
-- Second array: total impressions for each browser
-- Third array: total clicks for each browser
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sumMappedArrays_introduced_in = {1, 1};
    FunctionDocumentation::Category sumMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation sumMappedArrays_documentation = {sumMappedArrays_description, sumMappedArrays_syntax, sumMappedArrays_arguments, {}, sumMappedArrays_returned_value, sumMappedArrays_examples, sumMappedArrays_introduced_in, sumMappedArrays_category};

    factory.registerFunction("sumMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMap<Key, false, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMap<Key, false, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    }, {}, sumMappedArrays_documentation});

    FunctionDocumentation::Description minMappedArrays_description = R"(
Calculates the minimum from `value` array according to the keys specified in the `key` array.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax minMappedArrays_syntax = R"(
minMappedArrays(key, value)
minMappedArrays(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments minMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array(T)"}},
        {"value", "Array of values.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue minMappedArrays_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys.", {"Tuple(Array(T), Array(T))"}};
    FunctionDocumentation::Examples minMappedArrays_examples = {
    {
        "Usage example",
        R"(
SELECT minMappedArrays(a, b)
FROM VALUES('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]));
        )",
        R"(
┌─minMappedArrays(a, b)───────────┐
│ ([1, 2, 3], [2, 1, 1])          │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn minMappedArrays_introduced_in = {20, 5};
    FunctionDocumentation::Category minMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation minMappedArrays_documentation = {minMappedArrays_description, minMappedArrays_syntax, minMappedArrays_arguments, {}, minMappedArrays_returned_value, minMappedArrays_examples, minMappedArrays_introduced_in, minMappedArrays_category};

    factory.registerFunction("minMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionMinMap<Key, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionMinMap<Key, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    }, {}, minMappedArrays_documentation});

    FunctionDocumentation::Description maxMappedArrays_description = R"(
Calculates the maximum from `value` array according to the keys specified in the `key` array.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax maxMappedArrays_syntax = R"(
maxMappedArrays(key, value)
maxMappedArrays(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments maxMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array(T)"}},
        {"value", "Array of values.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue maxMappedArrays_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys.", {"Tuple(Array(T), Array(T))"}};
    FunctionDocumentation::Examples maxMappedArrays_examples = {
    {
        "Usage example",
        R"(
SELECT maxMappedArrays(a, b)
FROM VALUES('a Array(Char), b Array(Int64)', (['x', 'y'], [2, 2]), (['y', 'z'], [3, 1]));
        )",
        R"(
┌─maxMappedArrays(a, b)────────────────┐
│ [['x', 'y', 'z'], [2, 3, 1]].        │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn maxMappedArrays_introduced_in = {20, 5};
    FunctionDocumentation::Category maxMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation maxMappedArrays_documentation = {maxMappedArrays_description, maxMappedArrays_syntax, maxMappedArrays_arguments, {}, maxMappedArrays_returned_value, maxMappedArrays_examples, maxMappedArrays_introduced_in, maxMappedArrays_category};

    factory.registerFunction("maxMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionMaxMap<Key, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionMaxMap<Key, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    }, {}, maxMappedArrays_documentation});

    // these functions could be renamed to *MappedArrays too, but it would
    // break backward compatibility
    FunctionDocumentation::Description sumMapWithOverflow_description = R"(
Totals a `value` array according to the keys specified in the `key` array. Returns a tuple of two arrays: keys in sorted order, and values summed for the corresponding keys.
It differs from the [`sumMap`](/sql-reference/aggregate-functions/reference/summap) function in that it does summation with overflow - i.e. returns the same data type for the summation as the argument data type.

:::note
- Passing a tuple of key and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax sumMapWithOverflow_syntax = R"(
sumMapWithOverflow(key, value)
sumMapWithOverflow(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments sumMapWithOverflow_arguments = {
        {"key", "Array of keys.", {"Array"}},
        {"value", "Array of values.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue sumMapWithOverflow_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values summed for the corresponding keys.", {"Tuple(Array, Array)"}};
    FunctionDocumentation::Examples sumMapWithOverflow_examples = {
    {
        "Array syntax demonstrating overflow behavior",
        R"(
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt8,
        requests UInt8
    ),
    statusMapTuple Tuple(Array(Int8), Array(Int8))
) ENGINE = Memory;

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    toTypeName(sumMap(statusMap.status, statusMap.requests)),
    toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests))
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─toTypeName(sumMap⋯usMap.requests))─┬─toTypeName(sumMa⋯usMap.requests))─┐
│ 2000-01-01 00:01:00 │ Tuple(Array(UInt8), Array(UInt64)) │ Tuple(Array(UInt8), Array(UInt8)) │
│ 2000-01-01 00:00:00 │ Tuple(Array(UInt8), Array(UInt64)) │ Tuple(Array(UInt8), Array(UInt8)) │
└─────────────────────┴────────────────────────────────────┴───────────────────────────────────┘
        )"
    },
    {
        "Tuple syntax with same result",
        R"(
SELECT
    timeslot,
    toTypeName(sumMap(statusMapTuple)),
    toTypeName(sumMapWithOverflow(statusMapTuple))
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─toTypeName(sumMap(statusMapTuple))─┬─toTypeName(sumM⋯tatusMapTuple))─┐
│ 2000-01-01 00:01:00 │ Tuple(Array(Int8), Array(Int64))   │ Tuple(Array(Int8), Array(Int8)) │
│ 2000-01-01 00:00:00 │ Tuple(Array(Int8), Array(Int64))   │ Tuple(Array(Int8), Array(Int8)) │
└─────────────────────┴────────────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sumMapWithOverflow_introduced_in = {20, 1};
    FunctionDocumentation::Category sumMapWithOverflow_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation sumMapWithOverflow_documentation = {sumMapWithOverflow_description, sumMapWithOverflow_syntax, sumMapWithOverflow_arguments, {}, sumMapWithOverflow_returned_value, sumMapWithOverflow_examples, sumMapWithOverflow_introduced_in, sumMapWithOverflow_category};

    factory.registerFunction("sumMapWithOverflow", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMap<Key, true, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMap<Key, true, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    }, {}, sumMapWithOverflow_documentation});


    factory.registerFunction("sumMapFiltered", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMapFiltered<Key, false, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMapFiltered<Key, false, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    });


    factory.registerFunction("sumMapFilteredWithOverflow", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMapFiltered<Key, true, true>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
        return createAggregateFunctionMap(
                    [&]<typename Key>() -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionSumMapFiltered<Key, true, false>>(keys_type, values_types, arguments, params);
                    },
                    keys_type
                );
    });
}

}
