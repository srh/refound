#ifndef RETHINKDB_FDB_BTREE_UTILS_HPP_
#define RETHINKDB_FDB_BTREE_UTILS_HPP_

#include "btree/keys.hpp"
#include "fdb/reql_fdb_utils.hpp"

namespace rfdb {

// This file is named ironically after the btree/ directory.

inline std::string table_primary_key(
        const namespace_id_t &table_id, const store_key_t &key) {
    std::string ret = table_pkey_prefix(table_id);
    ret += key.str();
    return ret;
}

}  // namespace rfdb


#endif  // RETHINKDB_FDB_BTREE_UTILS_HPP_
