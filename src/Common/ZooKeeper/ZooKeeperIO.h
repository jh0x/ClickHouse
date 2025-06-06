#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <cstdint>
#include <vector>
#include <array>


namespace Coordination
{

using namespace DB;

template <typename T>
requires is_arithmetic_v<T>
void write(T x, WriteBuffer & out)
{
    writeBinaryBigEndian(x, out);
}

void write(OpNum x, WriteBuffer & out);
void write(const std::string & s, WriteBuffer & out);
void write(const ACL & acl, WriteBuffer & out);
void write(const Stat & stat, WriteBuffer & out);
void write(const Error & x, WriteBuffer & out);

template <size_t N>
void write(const std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <typename T>
void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

template <typename T>
requires is_arithmetic_v<T>
size_t size(T x)
{
    return sizeof(x);
}

size_t size(OpNum x);
size_t size(const std::string & s);
size_t size(const ACL & acl);
size_t size(const Stat & stat);
size_t size(const Error & x);

template <size_t N>
size_t size(const std::array<char, N>)
{
    return size(static_cast<int32_t>(N)) + N;
}

template <typename T>
size_t size(const std::vector<T> & arr)
{
    size_t total_size = size(static_cast<int32_t>(arr.size()));
    for (const auto & elem : arr)
        total_size += size(elem);

    return total_size;
}


template <typename T>
requires is_arithmetic_v<T>
void read(T & x, ReadBuffer & in)
{
    readBinaryBigEndian(x, in);
}

void read(OpNum & x, ReadBuffer & in);
void read(std::string & s, ReadBuffer & in);
void read(ACL & acl, ReadBuffer & in);
void read(Stat & stat, ReadBuffer & in);
void read(Error & x, ReadBuffer & in);

template <size_t N>
void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected array size while reading from ZooKeeper");
    in.readStrict(s.data(), N);
}

template <typename T>
void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Negative size while reading array from ZooKeeper");
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Too large array size while reading from ZooKeeper");
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

}
