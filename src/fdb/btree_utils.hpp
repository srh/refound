#ifndef RETHINKDB_FDB_BTREE_UTILS_HPP_
#define RETHINKDB_FDB_BTREE_UTILS_HPP_

#include <string>

#include "btree/keys.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "rdb_protocol/serialize_datum.hpp"

namespace rfdb {

// This file is named ironically after the btree/ directory.

inline std::string table_primary_key(
        const namespace_id_t &table_id, const store_key_t &key) {
    std::string ret = table_pkey_prefix(table_id);
    ret += key.str();
    return ret;
}

// QQQ: Of course, at some point, this will not be a raw fdb_future, or not one we get
// values off of, so we'll want to hard-wrap the future type.
struct datum_fut : public fdb_future {
    explicit datum_fut(fdb_future &&ff) : fdb_future{std::move(ff)} {}
};

MUST_USE ql::serialization_result_t
kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const ql::datum_t &data);

void kv_location_delete(FDBTransaction *txn, const std::string &kv_location);

datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location);

}  // namespace rfdb


#endif  // RETHINKDB_FDB_BTREE_UTILS_HPP_
