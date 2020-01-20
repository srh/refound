#ifndef RETHINKDB_FDB_INDEX_HPP_
#define RETHINKDB_FDB_INDEX_HPP_

#include "fdb/reql_fdb.hpp"

fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *index_prefix, const std::string &index_key);


#endif  // RETHINKDB_FDB_INDEX_HPP_
