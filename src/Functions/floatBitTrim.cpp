#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
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
                "First argument of {} must be Float32 or Float64, got {}",
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
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected type for {}", getName());
    }

private:
    //jhtodo: Const mantissa optimisation
    //jhtodo: SIMD?
    //jhtodo: bfloat
    template <typename Float, typename UInt, size_t MantissaBits>
    static ColumnPtr executeForType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto values_col = arguments[0].column->convertToFullColumnIfConst();
        auto bits_col = arguments[1].column->convertToFullColumnIfConst();

        const auto & values = assert_cast<const ColumnVector<Float> &>(*values_col).getData();

        auto result = ColumnVector<Float>::create(input_rows_count);
        auto & result_data = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Int64 n_signed = bits_col->getInt(i);
            if (n_signed < 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number of bits to trim in {} must be non-negative", name);

            Float v = values[i];
            if (std::isnan(v))
            {
                result_data[i] = v;
                continue;
            }

            UInt64 n = std::min<UInt64>(static_cast<UInt64>(n_signed), MantissaBits);
            UInt mask = ~((static_cast<UInt>(1) << n) - 1);
            result_data[i] = bit_cast<Float>(bit_cast<UInt>(v) & mask);
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
(23 for `Float32`, 52 for `Float64`).
)";
    FunctionDocumentation::Syntax syntax = "floatBitTrim(value, n)";
    FunctionDocumentation::Arguments arguments
        = {{"value", "Floating-point value to trim.", {"Float32", "Float64"}}, {"n", "Number of low mantissa bits to zero.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `value` with the lowest `n` mantissa bits zeroed, of the same type as `value`.", {"Float32", "Float64"}};
    FunctionDocumentation::Examples examples = {{"Trim 20 mantissa bits", "SELECT floatBitTrim(1.234::Float64, 20)", "1.2339999999385327"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3}; //jhtodo
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFloatBitTrim>(documentation);
}

}
