/* H5pubconf.h - hand-crafted */

#ifndef H5_CONFIG_H_
#define H5_CONFIG_H_

/* We build only the C library, static, no thread safety. */
#define H5_BUILT_AS_STATIC_LIB 1

/* Platform features - Linux and macOS both have these. */
#define H5_HAVE_ALARM 1
#define H5_HAVE_ASPRINTF 1
#define H5_HAVE_ATTRIBUTE 1
#define H5_HAVE_CLOCK_GETTIME 1
#define H5_HAVE_DIRENT_H 1
/* Deliberately off: its only use is pulling <dlfcn.h> into H5private.h, and the dynamic plugin
 * loader that needed it is not built - see H5PLstub.c. */
#undef H5_HAVE_DLFCN_H
#define H5_HAVE_FCNTL 1
#ifdef __linux__
#define H5_HAVE_FEATURES_H 1
#endif
#define H5_HAVE_FLOCK 1
#define H5_HAVE_FORK 1
#define H5_HAVE_FSEEKO 1
#define H5_HAVE_GETHOSTNAME 1
#define H5_HAVE_GETRUSAGE 1
#define H5_HAVE_GETTIMEOFDAY 1
#define H5_HAVE_IOCTL 1
#define H5_HAVE_LIBM 1
#define H5_HAVE_LIBPTHREAD 1
#define H5_HAVE_PREADWRITE 1
#define H5_HAVE_PTHREAD_H 1
#ifdef __linux__
#define H5_HAVE_PTHREAD_CONDATTR_SETCLOCK 1
#endif
#define H5_HAVE_PWD_H 1
#define H5_HAVE_STAT_ST_BLOCKS 1
#define H5_HAVE_STRCASESTR 1
#define H5_HAVE_STRDUP 1
#define H5_HAVE_SYMLINK 1
#define H5_HAVE_SYS_FILE_H 1
#define H5_HAVE_SYS_IOCTL_H 1
#define H5_HAVE_SYS_RESOURCE_H 1
#define H5_HAVE_SYS_SOCKET_H 1
#define H5_HAVE_SYS_STAT_H 1
#define H5_HAVE_SYS_TIME_H 1
#define H5_HAVE_SYS_TYPES_H 1
#define H5_HAVE_TIMEZONE 1
#define H5_HAVE_TMPFILE 1
#define H5_HAVE_TM_GMTOFF 1
#define H5_HAVE_UNISTD_H 1
#define H5_HAVE_VASPRINTF 1
#define H5_HAVE_WAITPID 1
#define H5_HAVE_BUILTIN_EXPECT 1
#define H5_HAVE_ARPA_INET_H 1
#define H5_HAVE_NETDB_H 1
#define H5_HAVE_NETINET_IN_H 1
#define H5_HAVE_STDATOMIC_H 1
#define H5_HAVE_QSORT_REENTRANT 1

/* No complex number support needed. */
/* #undef H5_HAVE_COMPLEX_NUMBERS */
/* #undef H5_HAVE_C99_COMPLEX_NUMBERS */

#ifdef __APPLE__
#define H5_HAVE_DARWIN 1
#endif

/* Deflate (zlib) filter support: H5_HAVE_FILTER_DEFLATE, H5_HAVE_ZLIB_H and H5_HAVE_LIBZ are
 * defined by CMakeLists.txt, but only when ch_contrib::zlib exists to be linked against. */

/* No szip. */
/* #undef H5_HAVE_FILTER_SZIP */
/* #undef H5_HAVE_SZLIB_H */
/* #undef H5_HAVE_LIBSZ */

/* No thread safety - we manage our own mutex. */
/* #undef H5_HAVE_THREADSAFE */
/* #undef H5_HAVE_CONCURRENCY */
/* #undef H5_HAVE_WIN_THREADS */
/* #undef H5_HAVE_C11_THREADS */
/* #undef H5_HAVE_THREADS */

/* No parallel/MPI. */
/* #undef H5_HAVE_PARALLEL */

/* No special VFDs. */
/* #undef H5_HAVE_DIRECT */
/* #undef H5_HAVE_MIRROR_VFD */
/* #undef H5_HAVE_ROS3_VFD */
/* #undef H5_HAVE_SUBFILING_VFD */
/* #undef H5_HAVE_IOC_VFD */
/* #undef H5_HAVE_MAP_API */

/* No embedded lib info. */
/* #undef H5_HAVE_EMBEDDED_LIBINFO */

/* dev_t is a scalar on Linux. */
#define H5_DEV_T_IS_SCALAR 1

/* Data accuracy over speed. */
#define H5_WANT_DATA_ACCURACY 1
#define H5_WANT_DCONV_EXCEPTION 1

/* Upstream decides these with a runtime probe at configure time. They hold for the 80-bit x87
   `long double` and for the IEEE 128-bit one of aarch64; on an architecture we have not checked -
   ppc64le, where `long double` is a double-double pair, among others - leaving them undefined
   selects upstream's own workaround, which is the careful path rather than the fast one. */
#if defined(__x86_64__) || defined(__aarch64__)
#define H5_LDOUBLE_TO_LLONG_ACCURATE 1
#define H5_LLONG_TO_LDOUBLE_CORRECT 1
#endif
/* #undef H5_LDOUBLE_TO_LONG_SPECIAL */
/* #undef H5_LONG_TO_LDOUBLE_SPECIAL */

/* Use file locking. */
#define H5_USE_FILE_LOCKING 1
#define H5_IGNORE_DISABLED_FILE_LOCKS 1

/* Use latest 2.0 API. */
#define H5_USE_200_API_DEFAULT 1

/* No deprecated symbols needed. */
/* #undef H5_NO_DEPRECATED_SYMBOLS */

/* Type sizes. */
#define H5_SIZEOF_BOOL 1
#define H5_SIZEOF_CHAR 1
#define H5_SIZEOF_SHORT __SIZEOF_SHORT__
#define H5_SIZEOF_INT __SIZEOF_INT__
#define H5_SIZEOF_UNSIGNED __SIZEOF_INT__
#define H5_SIZEOF_FLOAT __SIZEOF_FLOAT__
#define H5_SIZEOF_DOUBLE __SIZEOF_DOUBLE__
#define H5_SIZEOF_LONG_DOUBLE __SIZEOF_LONG_DOUBLE__
#define H5_SIZEOF_LONG __SIZEOF_LONG__
#define H5_SIZEOF_LONG_LONG __SIZEOF_LONG_LONG__
#define H5_SIZEOF_SIZE_T __SIZEOF_SIZE_T__
#define H5_SIZEOF_SSIZE_T __SIZEOF_POINTER__
#define H5_SIZEOF_PTRDIFF_T __SIZEOF_PTRDIFF_T__
#define H5_SIZEOF_INT8_T 1
#define H5_SIZEOF_UINT8_T 1
#define H5_SIZEOF_INT16_T 2
#define H5_SIZEOF_UINT16_T 2
#define H5_SIZEOF_INT32_T 4
#define H5_SIZEOF_UINT32_T 4
#define H5_SIZEOF_INT64_T 8
#define H5_SIZEOF_UINT64_T 8
#define H5_SIZEOF_INT_FAST8_T 1
#define H5_SIZEOF_UINT_FAST8_T 1
#define H5_SIZEOF_INT_LEAST8_T 1
#define H5_SIZEOF_UINT_LEAST8_T 1

/* These depend on LP64 vs ILP32 - both Linux and macOS are LP64 on 64-bit. */
#if defined(__LP64__) || defined(_LP64) || defined(__x86_64__) || defined(__aarch64__)
#define H5_SIZEOF_OFF_T 8
#define H5_SIZEOF_TIME_T 8
#define H5_SIZEOF_INT_FAST16_T 8
#define H5_SIZEOF_INT_FAST32_T 8
#define H5_SIZEOF_INT_FAST64_T 8
#define H5_SIZEOF_UINT_FAST16_T 8
#define H5_SIZEOF_UINT_FAST32_T 8
#define H5_SIZEOF_UINT_FAST64_T 8
#define H5_SIZEOF_INT_LEAST16_T 2
#define H5_SIZEOF_INT_LEAST32_T 4
#define H5_SIZEOF_INT_LEAST64_T 8
#define H5_SIZEOF_UINT_LEAST16_T 2
#define H5_SIZEOF_UINT_LEAST32_T 4
#define H5_SIZEOF_UINT_LEAST64_T 8
#else
#define H5_SIZEOF_OFF_T 4
#define H5_SIZEOF_TIME_T 4
#define H5_SIZEOF_INT_FAST16_T 4
#define H5_SIZEOF_INT_FAST32_T 4
#define H5_SIZEOF_INT_FAST64_T 8
#define H5_SIZEOF_UINT_FAST16_T 4
#define H5_SIZEOF_UINT_FAST32_T 4
#define H5_SIZEOF_UINT_FAST64_T 8
#define H5_SIZEOF_INT_LEAST16_T 2
#define H5_SIZEOF_INT_LEAST32_T 4
#define H5_SIZEOF_INT_LEAST64_T 8
#define H5_SIZEOF_UINT_LEAST16_T 2
#define H5_SIZEOF_UINT_LEAST32_T 4
#define H5_SIZEOF_UINT_LEAST64_T 8
#endif

/* _Float16 - not universally available, disable. */
#define H5_SIZEOF__FLOAT16 0
/* #undef H5_HAVE__FLOAT16 */
/* #undef H5_LDOUBLE_TO_FLOAT16_CORRECT */

/* Endianness - handled at compile time. */
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define WORDS_BIGENDIAN 1
#endif

/* Package metadata. */
#define H5_PACKAGE "hdf5"
#define H5_PACKAGE_BUGREPORT "help@hdfgroup.org"
#define H5_PACKAGE_NAME "HDF5"
#define H5_PACKAGE_STRING "HDF5 2.2.0"
#define H5_PACKAGE_TARNAME "hdf5"
#define H5_PACKAGE_URL "http://www.hdfgroup.org"
#define H5_PACKAGE_VERSION "2.2.0"
#define H5_VERSION "2.2.0"

/* Max real precision for conversions. */
#define H5_PAC_C_MAX_REAL_PRECISION 33
#define H5_PAC_FC_MAX_REAL_PRECISION 33

/* Search path for dynamically loaded plugins. Its only user, H5PLpkg.h, is not built - see
 * H5PLstub.c - but keep it empty rather than a real directory so that reintroducing the loader
 * cannot silently start scanning the filesystem. */
#define H5_DEFAULT_PLUGINDIR ""

#endif /* H5_CONFIG_H_ */
