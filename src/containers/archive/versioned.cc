#include "containers/archive/versioned.hpp"

cluster_version_result_t deserialize_cluster_version(
        read_stream_t *s,
        cluster_version_t *out) noexcept {
    // Initialize `out` to *something* because GCC 4.6.3 thinks that `thing`
    // could be used uninitialized, even when the return value of this function
    // is checked through `guarantee_deserialization()`.
    // See https://github.com/rethinkdb/rethinkdb/issues/2640
    *out = cluster_version_t::LATEST_OVERALL;
    int8_t raw;
    archive_result_t res = deserialize_universal(s, &raw);
    if (bad(res)) {
        return static_cast<cluster_version_result_t>(res);
    }
    if (raw == static_cast<int8_t>(obsolete_cluster_version_t::v1_13)
        || raw == static_cast<int8_t>(obsolete_cluster_version_t::v1_13_2_is_latest)) {
        return cluster_version_result_t::OBSOLETE_VERSION;
    } else {
        // This is the same rassert in `ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE`.
        if (raw >= static_cast<int8_t>(cluster_version_t::v1_14)
            && raw <= static_cast<int8_t>(cluster_version_t::v2_5_is_latest)) {
            *out = static_cast<cluster_version_t>(raw);
            return cluster_version_result_t::SUCCESS;
        } else {
            return cluster_version_result_t::UNRECOGNIZED_VERSION;
        }
    }
}

