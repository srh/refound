// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_SECONDARY_OPERATIONS_HPP_
#define RDB_PROTOCOL_SECONDARY_OPERATIONS_HPP_

#include <map>
#include <string>
#include <vector>

#include "btree/keys.hpp"
#include "write_durability.hpp"
#include "containers/archive/archive.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/wire_func.hpp"
#include "rpc/serialize_macros.hpp"

class real_superblock_lock;

// The query evaluation reql version information that we store with each secondary
// index function.
struct sindex_reql_version_info_t {
    // Generally speaking, original_reql_version <= latest_compatible_reql_version <=
    // latest_checked_reql_version.  When a new sindex is created, the values are the
    // same.  When a new version of RethinkDB gets run, latest_checked_reql_version
    // will get updated, and latest_compatible_reql_version will get updated if the
    // sindex function is compatible with a later version than the original value of
    // `latest_checked_reql_version`.

    // The original ReQL version of the sindex function.  The value here never
    // changes.  This might become useful for tracking down some bugs or fixing them
    // in-place, or performing a desperate reverse migration.
    reql_version_t original_reql_version;

    // This is the latest version for which evaluation of the sindex function remains
    // compatible.
    reql_version_t latest_compatible_reql_version;

    // If this is less than the current server version, we'll re-check
    // opaque_definition for compatibility and update this value and
    // `latest_compatible_reql_version` accordingly.
    reql_version_t latest_checked_reql_version;

    // To be used for new secondary indexes.
    static sindex_reql_version_info_t LATEST() {
        sindex_reql_version_info_t ret = { reql_version_t::LATEST,
                                           reql_version_t::LATEST,
                                           reql_version_t::LATEST };
        return ret;
    }
};

RDB_DECLARE_SERIALIZABLE(sindex_reql_version_info_t);

enum class sindex_multi_bool_t { SINGLE = 0, MULTI = 1};
enum class sindex_geo_bool_t { REGULAR = 0, GEO = 1};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(sindex_multi_bool_t, int8_t,
        sindex_multi_bool_t::SINGLE, sindex_multi_bool_t::MULTI);
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(sindex_geo_bool_t, int8_t,
        sindex_geo_bool_t::REGULAR, sindex_geo_bool_t::GEO);

// TODO: Fix that gross copy/refrobulation into this type and remove.
struct sindex_disk_info_t {
    sindex_disk_info_t() { }
    sindex_disk_info_t(const ql::deterministic_func &_mapping,
                       const sindex_reql_version_info_t &_mapping_version_info,
                       sindex_multi_bool_t _multi,
                       sindex_geo_bool_t _geo) :
        mapping(_mapping), mapping_version_info(_mapping_version_info),
        multi(_multi), geo(_geo) { }
    ql::deterministic_func mapping;
    sindex_reql_version_info_t mapping_version_info;
    sindex_multi_bool_t multi;
    sindex_geo_bool_t geo;
};

RDB_DECLARE_SERIALIZABLE(sindex_disk_info_t);

#endif /* RDB_PROTOCOL_SECONDARY_OPERATIONS_HPP_ */
