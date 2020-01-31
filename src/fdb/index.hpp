#ifndef RETHINKDB_FDB_INDEX_HPP_
#define RETHINKDB_FDB_INDEX_HPP_

#include "fdb/reql_fdb.hpp"
#include "fdb/typed.hpp"
#include "containers/optional.hpp"

// ukey_string and skey_string are type safety wrappers to lower the chance of improper
// conversion of data types to keys.

// A ukey_string just has to be serialized uniformly, in a (big-endian) way that
// preserves ordering.
struct ukey_string {
    std::string ukey;
};

// An skey_string has to preserve lexicographic ordering even when something has been
// appended to it!  So not all strings can be valid skey strings.  (Namely, when a
// ukey_string has been.)
struct skey_string {
    std::string skey;
};

std::string unique_index_fdb_key(const char *prefix, const ukey_string &index_key);
std::string plain_index_fdb_key(const char *prefix, const skey_string &index_key,
    const ukey_string &pkey);


fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *prefix, const ukey_string &index_key);

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key,
    const std::string &value);

void transaction_erase_unique_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key);

// TODO: Take string_view, key_view.
fdb_future transaction_lookup_pkey_index(
    FDBTransaction *txn, const char *prefix, const ukey_string &index_key);

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key,
    const std::string &value);

void transaction_erase_pkey_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key);


// Beware: The set of index_key values for the index must survive lexicographic ordering
// when combined with a pkey.
void transaction_set_plain_index(FDBTransaction *txn, const char *prefix,
    const skey_string &index_key, const ukey_string &pkey,
    const std::string &value);

void transaction_erase_plain_index(FDBTransaction *txn, const char *prefix,
    const skey_string &index_key, const ukey_string &pkey);




inline skey_string uuid_sindex_key(const uuid_u& u) {
    // Any fixed-width string will do.
    // TODO: At some point make this binary.
    return skey_string{uuid_to_str(u)};
}

inline ukey_string uuid_primary_key(const uuid_u &u) {
    return ukey_string{uuid_to_str(u)};
}

// TODO: Any of these typed functions performing serialization could do less string
// concatenation/allocation by serializing onto instead of serialize-then-concat.

template <class index_traits>
fdb_value_fut<typename index_traits::value_type>
transaction_lookup_uq_index(
        FDBTransaction *txn,
        const typename index_traits::ukey_type &index_key) {
    fdb_value_fut<typename index_traits::value_type> ret{
        transaction_lookup_unique_index(txn, index_traits::prefix,
            index_traits::ukey_str(index_key))};
    return ret;
}

template <class index_traits>
void transaction_erase_uq_index(
        FDBTransaction *txn,
        const typename index_traits::ukey_type &index_key) {
    transaction_erase_unique_index(txn, index_traits::prefix,
        index_traits::ukey_str(index_key));
}



#endif  // RETHINKDB_FDB_INDEX_HPP_
