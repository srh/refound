#include "fdb/index.hpp"

/* How indexes are encoded

Unique indexes are encoded a differently than non-unique.

Unique index keys:  <prefix><serialized sindex key>

Non-unique index keys:  <prefix><serialized sindex key><primary index key>

Of course, it is the responsibility of secondary index key serialization to
maintain ordering in the face of having something attached to its suffix.

E.g. "a" < "aa" must be maintained, even when the primary keys "b" and "c" get attached.


*/





// Returns an fdb_future of a point-lookup with the value (or not).
fdb_future transaction_lookup_unique_index(FDBTransaction *txn, const char *prefix, const std::string &index_key) {
    std::string key = prefix;
    key.append(index_key);
    return transaction_get_std_str(txn, key);
}

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
        const std::string &index_key,
        const std::string &value) {
    std::string key = prefix;
    key.append(index_key);
    fdb_transaction_set(
        txn, as_uint8(key.data()), int(key.size()),
        as_uint8(value.data()), int(value.size()));
    // TODO: Chase down every non-assurance that keys and values aren't too big.
}

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
        const std::string &index_key,
        const std::string &value) {
    // These work the same.
    transaction_set_unique_index(txn, prefix, index_key, value);
}
