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

}  // namespace rfdb
