option(ENABLE_HDF5 "Enable HDF5" ${ENABLE_LIBRARIES})
if (NOT ENABLE_HDF5)
    message(STATUS "Not using HDF5")
    return()
endif()

set(HDF5_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../hdf5/src")

# Collect all C sources from the HDF5 src/ directory.
file(GLOB HDF5_SRCS CONFIGURE_DEPENDS "${HDF5_SOURCE_DIR}/H5*.c")

# Remove the generated H5build_settings.c - we supply our own stub.
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5build_settings\\.c$")

# Remove the dynamic plugin loader - we supply our own stub that never loads anything. A filter id,
# driver name or VOL connector name coming from an untrusted file must not be able to dlopen a shared
# object. See H5PLstub.c.
list(FILTER HDF5_SRCS EXCLUDE REGEX "/H5PL[A-Za-z_]*\\.c$")

# Remove MPI-specific files that fail without MPI headers.
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDmpio\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDmpi\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5Dmpio\\.c$")

# Those pull in extra dependencies
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDros3\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDros3_s3comms\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDhdfs\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDmirror\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDdirect\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5Zszip\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDwindows\\.c$")

# The remaining built-in drivers - core, sec2, log, family, multi, stdio, splitter, onion - stay,
# because the library references them itself: H5Pfapl.c looks them up by name for the HDF5_DRIVER
# environment variable, and H5FD.c registers the onion driver during initialization. They are not
# reachable from file content: openHDF5File in src/Processors/Formats/Impl/HDF5BlockInputFormat.cpp
# pins the driver with H5Pset_fapl_sec2 and the object layer with H5Pset_vol, and the sec2 driver
# leaves sb_decode null, so a hostile superblock cannot select a different driver.

set(HDF5_UNUSED_SRCS
    H5Atest.c H5Ctest.c H5Dtest.c H5FDtest.c H5FStest.c H5Ftest.c H5Gtest.c
    H5HFtest.c H5Itest.c H5Otest.c H5Ptest.c H5SMtest.c H5Stest.c H5VLtest.c
    H5ACdbg.c H5B2dbg.c H5Ddbg.c H5EAdbg.c H5FAdbg.c H5FSdbg.c H5Fdbg.c
    H5HFdbg.c H5HGdbg.c H5HLdbg.c H5Idbg.c H5MFdbg.c H5dbg.c
    H5Adeprec.c H5Ddeprec.c H5Fdeprec.c H5Ideprec.c H5Ldeprec.c H5Odeprec.c
    H5Pdeprec.c H5Rdeprec.c H5Sdeprec.c H5Tdeprec.c)
foreach (src IN LISTS HDF5_UNUSED_SRCS)
    list(FILTER HDF5_SRCS EXCLUDE REGEX "/${src}$")
endforeach()

add_library(_hdf5
    ${HDF5_SRCS}
    "${CMAKE_CURRENT_SOURCE_DIR}/H5build_settings.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/H5PLstub.c"
)

target_include_directories(_hdf5 SYSTEM PUBLIC
    "${HDF5_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${HDF5_SOURCE_DIR}/H5FDsubfiling"
)

target_compile_definitions(_hdf5 PRIVATE
    H5_BUILT_AS_STATIC_LIB
    _GNU_SOURCE
    # Disables libhdf5's asserts, which a malformed file reaches and aborts on before we can throw.
    NDEBUG
)

# Suppress all compiler warnings from vendored HDF5 code.
target_compile_options(_hdf5 PRIVATE -w)

# Link ClickHouse's zlib for DEFLATE filter support. The H5_HAVE_* macros are defined here rather
# than in H5pubconf.h so that a build without zlib drops the DEFLATE filter instead of failing at
# link time. They are only used by H5Z.c, H5Zpkg.h and H5Zdeflate.c, all internal to this library.
if (TARGET ch_contrib::zlib)
    target_link_libraries(_hdf5 PRIVATE ch_contrib::zlib)
    target_compile_definitions(_hdf5 PRIVATE H5_HAVE_FILTER_DEFLATE H5_HAVE_ZLIB_H H5_HAVE_LIBZ)
endif()

add_library(ch_contrib::hdf5 ALIAS _hdf5)
