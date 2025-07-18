---
description: 'Guide for cross-compiling ClickHouse from Linux for macOS systems'
sidebar_label: 'Build on Linux for macOS'
sidebar_position: 20
slug: /development/build-cross-osx
title: 'Build on Linux for macOS'
---

# How to Build ClickHouse on Linux for macOS

This is for the case when you have a Linux machine and want to use it to build `clickhouse` binary that will run on OS X.
The main use case is continuous integration checks which run on Linux machines.
If you want to build ClickHouse directly on macOS, proceed with the [native build instructions](../development/build-osx.md).

The cross-build for macOS is based on the [Build instructions](../development/build.md), follow them first.

The following sections provide a walk-through for building ClickHouse for `x86_64` macOS.
If you're targeting ARM architecture, simply substitute all occurrences of `x86_64` with `aarch64`.
For example, replace `x86_64-apple-darwin` with `aarch64-apple-darwin` throughout the steps.

## Install cross-compilation toolset {#install-cross-compilation-toolset}

Let's remember the path where we install `cctools` as `${CCTOOLS}`

```bash
mkdir ~/cctools
export CCTOOLS=$(cd ~/cctools && pwd)
cd ${CCTOOLS}

git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
git checkout 15dfc2a8c9a2a89d06ff227560a69f5265b692f9
INSTALLPREFIX=${CCTOOLS} ./build.sh
./install.sh
cd ..

git clone https://github.com/tpoechtrager/cctools-port.git
cd cctools-port/cctools
git checkout 2a3e1c2a6ff54a30f898b70cfb9ba1692a55fad7
./configure --prefix=$(readlink -f ${CCTOOLS}) --with-libtapi=$(readlink -f ${CCTOOLS}) --target=x86_64-apple-darwin
make install
```

Also, we need to download macOS X SDK into the working tree.

```bash
cd ClickHouse/cmake/toolchain/darwin-x86_64
curl -L 'https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz' | tar xJ --strip-components=1
```

## Build ClickHouse {#build-clickhouse}

```bash
cd ClickHouse
mkdir build-darwin
cd build-darwin
CC=clang-19 CXX=clang++-19 cmake -DCMAKE_AR:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=${CCTOOLS}/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=${CCTOOLS}/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=${CCTOOLS}/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE=cmake/darwin/toolchain-x86_64.cmake ..
ninja
```

The resulting binary will have a Mach-O executable format and can't be run on Linux.
