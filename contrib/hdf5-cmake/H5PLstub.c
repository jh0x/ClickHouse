/* Replacement for the HDF5 dynamic plugin loader (H5PL.c, H5PLint.c, H5PLpath.c,
 * H5PLplugin_cache.c), which are excluded from the build - see CMakeLists.txt.
 *
 * The real loader resolves an unknown filter id, virtual file driver name or VOL connector name by
 * scanning $HDF5_PLUGIN_PATH (or a hardcoded /usr/local/hdf5/lib/plugin) and dlopen'ing every shared
 * object it finds. For a filter that lookup happens in H5Z_pipeline, on the read path, driven by the
 * filter pipeline message of the file being read. ClickHouse only ever reads plain local files, and
 * only in clickhouse-local, but the content of such a file is still not trusted, so a filter id,
 * driver name or connector name taken from it must never be able to load a shared library.
 *
 * Reporting "no plugin found" is a supported outcome for every caller: H5Z_pipeline reports
 * "required filter is not registered", and the driver and connector lookups fail with their own
 * errors. Only the three functions below are used by the rest of the library; if a submodule bump
 * adds another one the link will fail, which is the intended way to find out.
 */

#include "H5private.h"
#include "H5PLprivate.h"

const void *
H5PL_load(H5PL_type_t H5_ATTR_UNUSED plugin_type, const H5PL_key_t H5_ATTR_UNUSED *key)
{
    return NULL;
}

herr_t
H5PL_iterate(H5PL_iterate_type_t H5_ATTR_UNUSED iter_type, H5PL_iterate_t H5_ATTR_UNUSED iter_op,
             void H5_ATTR_UNUSED *op_data)
{
    /* No plugins to iterate over - not an error. */
    return SUCCEED;
}

int
H5PL_term_package(void)
{
    /* Nothing was ever initialized, so nothing changed for other interfaces. */
    return 0;
}
