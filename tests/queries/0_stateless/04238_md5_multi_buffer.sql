-- Tags: no-fasttest, no-openssl-fips
-- Test MD5 multi-buffer SIMD implementation for correctness.
-- Verifies that the SIMD batched path produces byte-identical results
-- to the RFC 1321 reference vectors across various input sizes and batch boundaries.

-- RFC 1321 test vectors
SELECT hex(MD5(''));
SELECT hex(MD5('a'));
SELECT hex(MD5('abc'));
SELECT hex(MD5('message digest'));
SELECT hex(MD5('abcdefghijklmnopqrstuvwxyz'));
SELECT hex(MD5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'));
SELECT hex(MD5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'));

-- Boundary sizes for MD5 padding (1 block vs 2 blocks threshold is at 55/56 bytes)
SELECT hex(MD5(repeat('x', 55)));  -- exactly fills 1 block after padding
SELECT hex(MD5(repeat('x', 56)));  -- spills to 2 blocks
SELECT hex(MD5(repeat('x', 63)));  -- just before a full block
SELECT hex(MD5(repeat('x', 64)));  -- exactly 1 full block + padding block
SELECT hex(MD5(repeat('x', 65)));  -- just after a full block
SELECT hex(MD5(repeat('x', 127))); -- just before 2 full blocks
SELECT hex(MD5(repeat('x', 128))); -- 2 full blocks + padding block
SELECT hex(MD5(repeat('x', 129))); -- just after 2 full blocks
SELECT hex(MD5(repeat('x', 1000)));

-- Batch boundary tests: exercise partial SIMD batches
-- (AVX2 = 2 x 8-lane groups, AVX-512 = 2 x 16-lane groups)
-- 7 rows (partial AVX2 group)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(7);
-- 8 rows (exact AVX2 group)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(8);
-- 9 rows (AVX2 group + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(9);
-- 15 rows (partial AVX2 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(15);
-- 16 rows (exact AVX2 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(16);
-- 17 rows (AVX2 batch + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(17);
-- 31 rows (partial AVX-512 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(31);
-- 32 rows (exact AVX-512 batch)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(32);
-- 33 rows (AVX-512 batch + 1 overflow)
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(33);
-- Larger batch to exercise multiple full SIMD iterations
SELECT sum(reinterpretAsUInt64(substring(MD5(toString(number)), 1, 8))) FROM numbers(100);

-- FixedString input
SELECT hex(MD5(toFixedString('hello', 5)));
SELECT hex(MD5(toFixedString('abc', 10)));

-- Variable-length strings with very different lengths in the same batch
-- (tests lane divergence in multi-buffer processing)
SELECT hex(MD5(s)) FROM
(
    SELECT arrayJoin(['', 'a', repeat('b', 55), repeat('c', 56), repeat('d', 100), repeat('e', 200)]) AS s
)
ORDER BY length(s);

-- Mixed-size row-order check. The row lengths cross padding and SIMD batch boundaries.
WITH
    [
        repeat('0', 1), repeat('1', 14), repeat('2', 27), repeat('3', 40),
        repeat('4', 53), repeat('5', 66), repeat('6', 79), repeat('7', 92),
        repeat('8', 105), repeat('9', 118), repeat('10', 1), repeat('11', 14),
        repeat('12', 27), repeat('13', 40), repeat('14', 53), repeat('15', 66),
        repeat('16', 79), repeat('17', 92), repeat('18', 105), repeat('19', 118),
        repeat('20', 1), repeat('21', 14), repeat('22', 27), repeat('23', 40)
    ] AS inputs,
    [
        'CFCD208495D565EF66E7DFF9F98764DA', 'E78582C7FA761CB9358009503F2810A9',
        'ED008F50F0D24FD54162C2749700958A', '50F8C0C582EB29449AC8D0E76C3B97A1',
        '7C56420B1A2379208C4FA04FA21EE246', 'DD7A0FCD74C3A90DEC1C99E29FC11B06',
        '77E9B80BBE96DFC3B88E20B5376D62B9', 'DB30DCB198A783F8BA4A55FB162D6627',
        '583B25111483B14CA58755A7775D02E9', '2DCAF6FEA5AF2E354A2AF333B576C87B',
        'D3D9446802A44259755D38E6D163E820', '2C3A51285D24FFB34B14E35399B01339',
        'A1147C86DB7D8B1501FF585E81E30089', 'C3566D330C01FA9502C0854FB0145EE1',
        '45B0FC1E4A0B17B4B9D9E2244B4A99D1', 'A844B5AD1ABD4288CA0D91D41E3306D7',
        '159AD629C1D32E2F16CA91FC4DC8D7CA', 'CC3CE6437055488DE4F174DC97407318',
        '5AD594A294627155A603D05B9500DAB1', 'CFE3A220546835073551E0074AC5AE15',
        '98F13708210194C475687BE6106A3B84', 'E938F43DF2C4DB81B55E988C318228B2',
        'D9CB760A8F61BB2E54A0462DC2BD423B', 'EBCA35E90BD3456234468A710649217C'
    ] AS expected
SELECT countIf(hex(MD5(tupleElement(item, 1))) != tupleElement(item, 2))
FROM
(
    SELECT arrayJoin(arrayZip(inputs, expected)) AS item
);

-- Known-answer rows sprinkled across several SIMD batches.
WITH '900150983CD24FB0D6963F7D28E17F72' AS abc_md5
SELECT countIf(is_known AND hex(MD5(if(is_known, 'abc', repeat(toString(number), (number % 17) + 1)))) != abc_md5)
FROM
(
    SELECT
        number,
        number IN (0, 15, 16, 31, 32, 47, 48, 63, 64, 95, 96) AS is_known
    FROM numbers(101)
);

-- IPv6 input
SELECT hex(MD5(toIPv6('::1')));

-- Single row (tests batch size = 1)
SELECT hex(MD5('single'));
