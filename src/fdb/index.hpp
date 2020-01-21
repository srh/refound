#ifndef RETHINKDB_FDB_INDEX_HPP_
#define RETHINKDB_FDB_INDEX_HPP_

#include "fdb/reql_fdb.hpp"

fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *prefix, const std::string &index_key);

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key,
    const std::string &value);

void transaction_erase_unique_index(FDBTransaction *txn, const char *prefix,
        const std::string &index_key);

fdb_future transaction_lookup_pkey_index(
    FDBTransaction *txn, const char *prefix, const std::string &index_key);

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key,
    const std::string &value);

void transaction_erase_pkey_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key);


// Beware: The set of index_key values for the index must survive lexicographic ordering
// when combined with a pkey.
void transaction_set_plain_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key, const std::string &pkey,
    const std::string &value);

void transaction_erase_plain_index(FDBTransaction *txn, const char *prefix,
    const std::string &index_key, const std::string &pkey);


inline std::string uuid_sindex_key(const uuid_u& u) {
    // Any fixed-width string will do.
    // TODO: At some point make this binary.
    return uuid_to_str(u);
}

#endif  // RETHINKDB_FDB_INDEX_HPP_
