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

# Remove MPI-specific files that fail without MPI headers.
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDmpio\\.c$")

# Those pull in extra dependencies
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDros3\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDhdfs\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDmirror\\.c$")
list(FILTER HDF5_SRCS EXCLUDE REGEX "H5FDdirect\\.c$")

add_library(_hdf5
    ${HDF5_SRCS}
    "${CMAKE_CURRENT_SOURCE_DIR}/H5build_settings.c"
)

target_include_directories(_hdf5 SYSTEM PUBLIC
    "${HDF5_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${HDF5_SOURCE_DIR}/H5FDsubfiling"
)

target_compile_definitions(_hdf5 PRIVATE
    H5_BUILT_AS_STATIC_LIB
    _GNU_SOURCE
    $<$<CONFIG:Release>:NDEBUG>
)

# Suppress all compiler warnings from vendored HDF5 code.
target_compile_options(_hdf5 PRIVATE -w)

# Link ClickHouse's zlib for DEFLATE filter support.
if (TARGET ch_contrib::zlib)
    target_link_libraries(_hdf5 PRIVATE ch_contrib::zlib)
endif()

target_link_libraries(_hdf5 PRIVATE pthread)

add_library(ch_contrib::hdf5 ALIAS _hdf5)
