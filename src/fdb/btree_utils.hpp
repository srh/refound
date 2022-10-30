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

inline std::string table_count_location(
        const namespace_id_t &table_id) {
    std::string ret = table_key_prefix(table_id);
    ret += "_count";
    return ret;
}


inline const std::string &table_pkey_keystr(const store_key_t &key) {
    return key.str();
}

inline std::string table_primary_key(
        const namespace_id_t &table_id, const store_key_t &key) {
    std::string ret = table_pkey_prefix(table_id);
    ret += table_pkey_keystr(key);
    return ret;
}

inline std::string index_key_concat(const std::string &kv_prefix, const store_key_t &key) {
    return kv_prefix + key.str();
}

// key should really be a store_key_t.  TODO: Remove
inline std::string index_key_concat_str(const std::string &kv_prefix, const std::string &key) {
    return kv_prefix + key;
}

struct datum_fut {
    datum_fut() = default;
    datum_fut(FDBFuture *range_fut, std::string _prefix, std::string _upper_key)
        : future{range_fut}, prefix{std::move(_prefix)}, upper_key{std::move(_upper_key)} {}
    fdb_future future;
    std::string prefix;
    std::string upper_key;
};

struct datum_range_fut {
    explicit datum_range_fut(FDBFuture *fut) : future{fut} {}
    fdb_future future;
};

struct secondary_range_fut {
    secondary_range_fut() = default;
    explicit secondary_range_fut(FDBFuture *fut) : future{fut} {}
    fdb_future future;
};



approx_txn_size kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const write_message_t &serialized_datum);

approx_txn_size kv_location_delete(FDBTransaction *txn, const std::string &kv_location);

datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location);

optional<std::vector<uint8_t>> block_and_read_unserialized_datum(
        FDBTransaction *txn,
        rfdb::datum_fut &&fut, const signal_t *interruptor);

std::string kv_prefix(std::string &&kv_location);
std::string kv_prefix_end(std::string &&kv_location);

enum class lower_bound {
    open,
    closed,
};

struct unique_pkey_suffix {
    static constexpr size_t size = 16;
    char data[size];

    static unique_pkey_suffix copy(const uint8_t *buf, size_t count) {
        guarantee(count == size);
        unique_pkey_suffix ret;
        memcpy(ret.data, buf, size);
        return ret;
    }

    int compare(const unique_pkey_suffix& other) const {
        return memcmp(data, other.data, size);
    }
};

std::string datum_range_lower_bound(const std::string &pkey_prefix, const store_key_t &lower);
std::string datum_range_upper_bound(const std::string &pkey_prefix, const store_key_t *upper_or_null);

struct datum_range_iterator {
    std::string pkey_prefix_;
    // If forward-iterating, lower is either a large value prefix with its '\0' suffix
    // or with a '\1' suffix (for an open bound).
    std::string lower_;
    std::string upper_;
    fdb_bool_t snapshot_;
    fdb_bool_t reverse_;

    // Iterating forward, it's num_parts_.  Iterating in reverse, it's the last seen
    // counter number (possibly zero).
    uint32_t number_;
    // These parts are in _iteration order_.  If iterating in reverse, that means they
    // need to be reversed before concatenation.
    std::vector<std::vector<uint8_t>> partial_document_;

    // Only valid when partial_document_ non-empty.
    unique_pkey_suffix last_seen_suffix_;
    bool split_across_txns_ = false;

    // TODO: Maybe split into prep_for_step() and block_for_step().
    // It is possible to have no results and bool = true.
    std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool>
    query_and_step(FDBTransaction *txn, const signal_t *interruptor, FDBStreamingMode mode,
            int target_bytes = 0,
            size_t *bytes_read_out = nullptr);

    void mark_split_across_txns() {
        split_across_txns_ = true;
    }
};

datum_range_iterator primary_prefix_make_iterator(const std::string &kv_prefix,
    const store_key_t &lower, const store_key_t *upper_or_null,
    fdb_bool_t snapshot, fdb_bool_t reverse);

secondary_range_fut secondary_prefix_get_range(FDBTransaction *txn,
        const std::string &kv_prefix,
        const store_key_t &lower, lower_bound lower_bound_closed,
        const store_key_t *upper_or_null,
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

void transaction_increment_count(
    FDBTransaction *txn, const namespace_id_t &table_id, int64_t count_delta);

}  // namespace rfdb

#endif  // RETHINKDB_FDB_BTREE_UTILS_HPP_
