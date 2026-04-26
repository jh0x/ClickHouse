-- Identity: trimming zero bits is a no-op.
SELECT floatBitTrim(1.234::Float64, 0) = 1.234::Float64;
SELECT floatBitTrim(1.234::Float32, 0) = 1.234::Float32;

-- Low bits of the bit representation are actually zero after trimming.
SELECT bitAnd(reinterpretAsUInt64(floatBitTrim(1.234567890123::Float64, 30)), bitShiftLeft(1::UInt64, 30) - 1);
SELECT bitAnd(reinterpretAsUInt32(floatBitTrim(1.234::Float32, 10)), bitShiftLeft(1::UInt32, 10) - 1);

-- Sign and exponent are preserved (clamped to mantissa width).
SELECT floatBitTrim(3.14::Float32, 100) = floatBitTrim(3.14::Float32, 23);
SELECT floatBitTrim(-3.14::Float64, 100) = floatBitTrim(-3.14::Float64, 52);

-- Result type matches input float type.
SELECT toTypeName(floatBitTrim(1.0::Float32, 5));
SELECT toTypeName(floatBitTrim(1.0::Float64, 5));

-- Per-row n via a column.
SELECT floatBitTrim(1.234::Float64, n) FROM (SELECT arrayJoin([0, 10, 20, 30, 52, 100]) AS n) ORDER BY n;

-- Errors.
SELECT floatBitTrim('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT floatBitTrim(1.0::Float64, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT floatBitTrim(1.0::Float64, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- jhtodo materialize