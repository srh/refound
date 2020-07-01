#ifndef RETHINKDB_FDB_BTREE_UTILS_HPP_
#define RETHINKDB_FDB_BTREE_UTILS_HPP_

// This file is named ironically after the btree/ directory.

#include <string>

#include "btree/keys.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "rdb_protocol/context.hpp"  // TODO: Remove when the sindex_disk_info_t function is removed.
#include "rdb_protocol/serialize_datum.hpp"
#include "rdb_protocol/secondary_operations.hpp"  // TODO: Remove when the sindex_disk_info_t function is removed.

namespace rfdb {

struct value_view {
    const uint8_t *data;
    int length;
};

inline std::string table_key_prefix(const namespace_id_t &table_id) {
    // TODO: Use binary uuid's.  This is on a fast path...
    // Or don't even use uuid's.
    std::string ret = "tables/";
    uuid_onto_str(table_id.value, &ret);
    ret += '/';
    return ret;
}

inline std::string table_index_prefix(
        const namespace_id_t &table_id,
        const sindex_id_t &index_id) {
    std::string ret = table_key_prefix(table_id);
    uuid_onto_str(index_id.value, &ret);
    ret += '/';
    return ret;
}

inline std::string table_pkey_prefix(
        const namespace_id_t &table_id) {
    std::string ret = table_key_prefix(table_id);
    ret += '/';
    return ret;
}

inline std::string table_primary_key(
        const namespace_id_t &table_id, const store_key_t &key) {
    std::string ret = table_pkey_prefix(table_id);
    ret += key.str();
    return ret;
}

inline std::string index_key_concat(const std::string &kv_prefix, const store_key_t &key) {
    return kv_prefix + key.str();
}

// key should really be a store_key_t.  TODO: Remove
inline std::string index_key_concat_str(const std::string &kv_prefix, const std::string &key) {
    return kv_prefix + key;
}

// QQQ: Of course, at some point, this will not be a raw fdb_future, or not one we get
// values off of, so we'll want to hard-wrap the future type.
struct datum_fut : public fdb_future {
    explicit datum_fut(fdb_future &&ff) : fdb_future{std::move(ff)} {}
};

// QQQ: Likewise here -- it may be a range-based single fdb_future, sure.
struct datum_range_fut : public fdb_future {
    using fdb_future::fdb_future;
    explicit datum_range_fut(fdb_future &&ff) : fdb_future{std::move(ff)} {}
};

// QQQ: And here, there are no values.
struct secondary_range_fut : public fdb_future {
    using fdb_future::fdb_future;
    explicit secondary_range_fut(fdb_future &&ff) : fdb_future{std::move(ff)} {}
};

MUST_USE ql::serialization_result_t
kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const ql::datum_t &data);

void kv_location_delete(FDBTransaction *txn, const std::string &kv_location);

datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location);

enum class lower_bound {
    open,
    closed,
};

datum_range_fut kv_prefix_get_range(FDBTransaction *txn, const std::string &kv_prefix,
    const store_key_t &lower, lower_bound lower_bound_closed,
    const store_key_t *upper_or_null,
    int limit, int target_bytes, FDBStreamingMode mode, int iteration,
    fdb_bool_t snapshot, fdb_bool_t reverse);

// Uses lower and upper_or_null should be store_key_t's, but they're std::strings.
// TODO: Remove.
secondary_range_fut secondary_prefix_get_range_str(FDBTransaction *txn, const std::string &prefix,
    const std::string &lower, lower_bound lower_bound_closed,
    const std::string *upper_or_null,
    int limit, int target_bytes, FDBStreamingMode mode, int iteration,
    fdb_bool_t snapshot, fdb_bool_t reverse);

// TODO: Making this copy is gross, this function shouldn't exist.
inline sindex_disk_info_t sindex_config_to_disk_info(const sindex_config_t &sindex_config) {
    sindex_disk_info_t sindex_info{
        sindex_config.func,
        sindex_reql_version_info_t{sindex_config.func_version,sindex_config.func_version,sindex_config.func_version},  // TODO: Verify we just dumbly use latest_compatible_reql_version.
        sindex_config.multi,
        sindex_config.geo};
    return sindex_info;
}

}  // namespace rfdb

#endif  // RETHINKDB_FDB_BTREE_UTILS_HPP_
