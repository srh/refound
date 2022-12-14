#ifndef VERSION_HPP_
#define VERSION_HPP_

// An enumeration of the current and previous recognized versions.  Every
// serialization / deserialization happens relative to one of these versions.
enum class obsolete_cluster_version_t {
    v1_13 = 0,
    v1_13_2 = 1,

    v1_13_2_is_latest = v1_13_2
};
enum class cluster_version_t {
    // The versions are _currently_ contiguously numbered.  If this becomes untrue,
    // be sure to carefully replace the ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE line
    // that implements serialization.
    v1_14 = 2,
    v1_15 = 3,
    v1_16 = 4,
    v2_0 = 5,
    v2_1 = 6,
    v2_2 = 7,
    v2_3 = 8,
    v2_4 = 9,
    v2_5 = 10,

    // This is used in places where _something_ needs to change when a new cluster
    // version is created.  (Template instantiations, switches on version number,
    // etc.)
    v2_5_is_latest = v2_5,

    // We no longer allow different cluster versions and disk versions.  These
    // names are vestigial.

    v2_5_is_latest_disk = v2_5_is_latest,
    LATEST_OVERALL = v2_5_is_latest,
    LATEST_DISK = v2_5_is_latest,
    CLUSTER = LATEST_OVERALL,
    // Code using LATEST was added after we removed CLUSTER_AND_DISK_VERSIONS_ARE_SAME.
    LATEST = LATEST_OVERALL,
};

// We will not (barring a bug) even attempt to deserialize a version number that we
// do not support.  Cluster nodes connecting to us make sure that they are
// communicating with a version number that we also support (the max of our two
// versions) and we don't end up deserializing version numbers in that situation
// anyway (yet).  Files on disk will not (barring corruption, or a bug) cause us to
// try to use a version number that we do not support -- we'll see manually whether
// we can read the serializer file by looking at the disk_format_version field in the
// metablock.
//
// At some point the set of cluster versions and disk versions that we support might
// diverge.  It's likely that we'd support a larger window of serialization versions
// in the on-disk format.
//
// Also, note: it's possible that versions will not be linearly ordered: Suppose we
// release v1.17 and then v1.18.  Perhaps v1.17 supports v1_16 and v1_17 and v1.18
// supports v1_17 and v1_18.  Suppose we then need to make a point-release, v1.17.1,
// that changes a serialization format, and we add a new cluster version v1_17_1.
// Then, v1.18 would still only support v1_17 and v1_18.  However, v1.18.1 might
// support v1_17, v1_17_1, and v1_18 (and v1_18_1 if that needs to be created).


enum class obsolete_reql_version_t {
    v1_13 = 0,
    v1_14 = 1,
    v1_15 = v1_14,

    v1_15_is_latest = v1_15,

    EARLIEST = v1_13,
    LATEST = v1_15_is_latest
};

// This describes reql versions whose sindex descriptions can be imported (via
// r.index_create with a binary function description), but which cannot possibly
// appear in storage.  A superset of reql_version_t.
enum class importable_reql_version_t {
    v1_16 = 2,
    v2_0 = 3,
    v2_1 = 4,
    v2_2 = 5,
    v2_3 = 6,
    v2_4 = 7,
    v2_4_is_latest = v2_4,

    EARLIEST = v1_16,
    LATEST = v2_4_is_latest,
};

// Reql versions define how secondary index functions should be evaluated.  Older
// versions have bugs that are fixed in newer versions.  They also define how secondary
// index keys are generated.
enum class reql_version_t {
    v2_4 = 7,

    // Code that uses _is_latest may need to be updated when the
    // version changes
    v2_4_is_latest = v2_4,

    EARLIEST = v2_4,
    LATEST = v2_4_is_latest
};

inline importable_reql_version_t to_importable(reql_version_t r) {
    return static_cast<importable_reql_version_t>(r);
}

// Serialization of reql_version_t is defined in protocol_api.hpp.

#endif  // VERSION_HPP_
