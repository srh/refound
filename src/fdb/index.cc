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
fdb_future transaction_lookup_unique_index(FDBTransaction *txn, const char *index_prefix, const std::string &index_key) {
    std::string key = index_prefix;
    key.append(index_key);
    return transaction_get_std_str(txn, key);
}
