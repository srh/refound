#include "fdb/index.hpp"

#include "utils.hpp"

/* How indexes are encoded

Unique indexes are encoded a differently than non-unique.

Unique index keys:  <prefix><serialized sindex key>

Non-unique index keys:  <prefix><serialized sindex key><primary index key>

Of course, it is the responsibility of secondary index key serialization to
maintain ordering in the face of having something attached to its suffix.

E.g. "a" < "aa" must be maintained, even when the primary keys "b" and "c" get attached.
*/

/*
It turns out this file's functions are only used for system indexes, not RethinkDB table
primary key and secondary indexes, but that's just because we want to reuse existing
code as much as possible.
*/


std::string unique_index_fdb_key(std::string prefix, const ukey_string &index_key) {
    std::string ret = std::move(prefix);
    ret += index_key.ukey;
    return ret;
}

std::string unique_index_fdb_key(const char *prefix, const ukey_string &index_key) {
    return unique_index_fdb_key(std::string(prefix), index_key);
}

std::string plain_index_skey_prefix(const char *prefix, const skey_string &index_key) {
    std::string ret = prefix;
    ret += index_key.skey;
    return ret;
}

std::string plain_index_fdb_key(const char *prefix, const skey_string &index_key,
    const ukey_string &pkey) {
    std::string ret = plain_index_skey_prefix(prefix, index_key);
    ret += pkey.ukey;
    return ret;
}

// Returns an fdb_future of a point-lookup with the value (or not).
fdb_future transaction_lookup_unique_index(FDBTransaction *txn, const char *prefix, const ukey_string &index_key) {
    std::string key = unique_index_fdb_key(prefix, index_key);
    return transaction_get_std_str(txn, key);
}

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key,
        const std::string &value) {
    std::string key = unique_index_fdb_key(prefix, index_key);
    fdb_transaction_set(
        txn, as_uint8(key.data()), int(key.size()),
        as_uint8(value.data()), int(value.size()));
    // TODO: Chase down every non-assurance that keys and values aren't too big.
}

void transaction_erase_unique_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key) {
    std::string key = unique_index_fdb_key(prefix, index_key);
    fdb_transaction_clear(
        txn, as_uint8(key.data()), int(key.size()));
}

fdb_future transaction_uq_index_get_range(FDBTransaction *txn, const std::string &prefix,
        const ukey_string &lower, const ukey_string *upper_or_null,
        int limit, int target_bytes, FDBStreamingMode mode, int iteration,
        fdb_bool_t snapshot, fdb_bool_t reverse) {
    std::string lower_key = unique_index_fdb_key(prefix, lower);
    std::string upper_key = upper_or_null == nullptr ?
        prefix_end(lower_key) :
        unique_index_fdb_key(prefix, *upper_or_null);

    return fdb_future{fdb_transaction_get_range(txn,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(lower_key.data()), int(lower_key.size())),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper_key.data()), int(upper_key.size())),
        limit,
        target_bytes,
        mode,
        iteration,
        snapshot,
        reverse)};
}



fdb_future transaction_lookup_pkey_index(FDBTransaction *txn, const char *prefix, const ukey_string &index_key) {
    // These work the same.
    return transaction_lookup_unique_index(txn, prefix, index_key);
}

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key,
        const std::string &value) {
    // These work the same.
    transaction_set_unique_index(txn, prefix, index_key, value);
}

void transaction_erase_pkey_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key) {
    transaction_erase_unique_index(txn, prefix, index_key);
}


void transaction_set_plain_index(FDBTransaction *txn, const char *prefix,
        const skey_string &index_key, const ukey_string &pkey,
        const std::string &value) {
    std::string key = plain_index_fdb_key(prefix, index_key, pkey);
    fdb_transaction_set(
        txn, as_uint8(key.data()), int(key.size()),
        as_uint8(value.data()), int(value.size()));
    // TODO: Chase down every non-assurance that keys and values aren't too big.
}

void transaction_erase_plain_index(FDBTransaction *txn, const char *prefix,
        const skey_string &index_key, const ukey_string &pkey) {
    std::string key = plain_index_fdb_key(prefix, index_key, pkey);
    fdb_transaction_clear(txn, as_uint8(key.data()), int(key.size()));
}
