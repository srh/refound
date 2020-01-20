#ifndef RETHINKDB_FDB_INDEX_HPP_
#define RETHINKDB_FDB_INDEX_HPP_

#include "fdb/reql_fdb.hpp"

fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *prefix, const std::string &index_key);

fdb_future transaction_lookup_pkey_index(
    FDBTransaction *txn, const char *prefix, const std::string &index_key);

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key,
    const std::string &value);

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key,
    const std::string &value);


#endif  // RETHINKDB_FDB_INDEX_HPP_
