#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/BFloat16.h>
#include <base/bit_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

template <typename Float>
inline bool is_nan(Float v)
{
    if constexpr (std::is_same_v<Float, BFloat16>)
        return v.isNaN();
    else
        return std::isnan(v);
}

template <typename Float, typename MaskType>
inline Float process_one(Float v, MaskType mask)
{
    if (is_nan(v))
        return v;
    return bit_cast<Float>(bit_cast<MaskType>(v) & mask);
}

class FunctionFloatBitTrim : public IFunction
{
public:
    static constexpr auto name = "floatBitTrim";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFloatBitTrim>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isFloat(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be BFloat16, Float32 or Float64, got {}",
                getName(),
                arguments[0]->getName());
        if (!isInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be an integer, got {}",
                getName(),
                arguments[1]->getName());
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        WhichDataType which(arguments[0].type);
        if (which.isFloat32())
            return executeForType<Float32, UInt32, 23>(arguments, input_rows_count);
        if (which.isFloat64())
            return executeForType<Float64, UInt64, 52>(arguments, input_rows_count);
        if (which.isBFloat16())
            return executeForType<BFloat16, UInt16, 7>(arguments, input_rows_count);
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected type for {}", getName());
    }

private:
    //jhtodo: SIMD?
    template <typename Float, typename MaskType, size_t MantissaBits>
    static ColumnPtr executeForType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto values_col = arguments[0].column->convertToFullColumnIfConst();
        const auto & bits_col_ptr = arguments[1].column;

        const auto & values = assert_cast<const ColumnVector<Float> &>(*values_col).getData();

        auto result = ColumnVector<Float>::create(input_rows_count);
        auto & result_data = result->getData();

        auto get_mask = [&](auto n_raw) -> MaskType
        {
            if (n_raw < 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number of bits to trim in {} must be non-negative", name);
            auto n = std::min<UInt64>(static_cast<UInt64>(n_raw), MantissaBits);
            return static_cast<MaskType>(~((static_cast<MaskType>(1) << n) - 1));
        };

        if (isColumnConst(*bits_col_ptr))
        {
            Int64 n_raw = bits_col_ptr->getInt(0);
            const auto mask = get_mask(n_raw);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                result_data[i] = process_one(values[i], mask);
            }

            return result;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Int64 n_raw = bits_col_ptr->getInt(i);
            const auto mask = get_mask(n_raw);

            result_data[i] = process_one(values[i], mask);
        }

        return result;
    }
};

}

REGISTER_FUNCTION(FloatBitTrim)
{
    FunctionDocumentation::Description description = R"(
Zeroes the lowest `n` bits of the IEEE 754 mantissa of a floating-point value.
This is a lossy precision reduction useful for improving compression of float columns.
The exponent and sign are preserved; `n` is clamped to the mantissa width
(7 for `BFloat16`, 23 for `Float32`, 52 for `Float64`).
)";
    FunctionDocumentation::Syntax syntax = "floatBitTrim(value, n)";
    FunctionDocumentation::Arguments arguments
        = {{"value", "Floating-point value to trim.", {"BFloat16", "Float32", "Float64"}},
           {"n", "Number of low mantissa bits to zero.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `value` with the lowest `n` mantissa bits zeroed, of the same type as `value`.", {"BFloat16", "Float32", "Float64"}};
    FunctionDocumentation::Examples examples = {{"Trim 20 mantissa bits", "SELECT floatBitTrim(1.234::Float64, 20)", "1.2339999999385327"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3}; //jhtodo
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFloatBitTrim>(documentation);
}

}
