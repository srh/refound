#include "fdb/btree_utils.hpp"

#include "rdb_protocol/btree.hpp"

namespace rfdb {

// TODO: Large value impl will rewrite kv_location_set and kv_location_get (and any
// other kv_location funcs).

// The signature will need to change with large values because we'll need to wipe out the old value (if it's larger and uses more keys).
MUST_USE ql::serialization_result_t
kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const ql::datum_t &data) {
    std::string str;
    ql::serialization_result_t res = datum_serialize_to_string(data, &str);
    if (bad(res)) {
        return res;
    }

    transaction_set_std_str(txn, kv_location, str);
    return res;
}

void kv_location_delete(
        FDBTransaction *txn, const std::string &kv_location) {
    transaction_clear_std_str(txn, kv_location);
}

rfdb::datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location) {
    return datum_fut{transaction_get_std_str(txn, kv_location)};
}

datum_range_fut kv_prefix_get_range(FDBTransaction *txn, const std::string &kv_prefix,
        const store_key_t &lower, lower_bound lower_bound_closed,
        const store_key_t *upper_or_null,
        int limit, int target_bytes, FDBStreamingMode mode, int iteration,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    std::string lower_key = index_key_concat(kv_prefix, lower);
    std::string upper_key = upper_or_null ? index_key_concat(kv_prefix, *upper_or_null)
        : prefix_end(kv_prefix);

    return datum_range_fut{fdb_transaction_get_range(txn,
        as_uint8(lower_key.data()), int(lower_key.size()), lower_bound_closed == lower_bound::open, 1,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_key.data()), int(upper_key.size())),
        limit,
        target_bytes,
        mode,
        iteration,
        snapshot,
        reverse)};
}

// TODO: Remove.
datum_range_fut kv_prefix_get_range_str(FDBTransaction *txn,
        const std::string &kv_prefix,
        const std::string &lower, lower_bound lower_bound_closed,
        const std::string *upper_or_null,
        int limit, int target_bytes, FDBStreamingMode mode, int iteration,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    std::string lower_key = index_key_concat_str(kv_prefix, lower);
    std::string upper_key = upper_or_null ? index_key_concat_str(kv_prefix, *upper_or_null)
        : prefix_end(kv_prefix);

    return datum_range_fut{fdb_transaction_get_range(txn,
        as_uint8(lower_key.data()), int(lower_key.size()), lower_bound_closed == lower_bound::open, 1,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_key.data()), int(upper_key.size())),
        limit,
        target_bytes,
        mode,
        iteration,
        snapshot,
        reverse)};
}

}  // namespace rfdb
