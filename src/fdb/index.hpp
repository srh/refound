#ifndef RETHINKDB_FDB_INDEX_HPP_
#define RETHINKDB_FDB_INDEX_HPP_

#include "btree/keys.hpp"  // TODO: store_key_t, ugh.
#include "containers/archive/string_stream.hpp"  // TODO: Probably vector stream, use of reserve, serialized_size and such, is better.
#include "fdb/reql_fdb.hpp"
#include "fdb/typed.hpp"
#include "containers/optional.hpp"
#include "rpc/serialize_macros.hpp"

// ukey_string and skey_string are type safety wrappers to lower the chance of improper
// conversion of data types to keys.

// A ukey_string just has to be serialized uniformly, in a (big-endian) way that
// preserves ordering.
struct ukey_string {
    std::string ukey;
};
RDB_MAKE_SERIALIZABLE_1(ukey_string, ukey);

// An skey_string has to preserve lexicographic ordering even when something has been
// appended to it!  So not all strings can be valid skey strings.  (Namely, when a
// ukey_string has been.)
struct skey_string {
    std::string skey;
};

std::string unique_index_fdb_key(const char *prefix, const ukey_string &index_key);
std::string unique_index_fdb_key(std::string prefix, const ukey_string &index_key);
std::string plain_index_skey_prefix(const char *prefix, const skey_string &index_key);
std::string plain_index_fdb_key(const char *prefix, const skey_string &index_key,
    const ukey_string &pkey);

inline void rdbtable_sindex_fdb_key_onto(std::string *prefix, const store_key_t &secondary_key) {
    *prefix += secondary_key.str();
}

fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *prefix, const ukey_string &index_key);

fdb_future transaction_lookup_unique_index(
    FDBTransaction *txn, const char *prefix, const ukey_string &index_key, bool snapshot);

void transaction_set_unique_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key,
    const std::string &value);

void transaction_erase_unique_index(FDBTransaction *txn, const char *prefix,
        const ukey_string &index_key);

// TODO: Remove this unused function?
fdb_future transaction_uq_index_get_range(FDBTransaction *txn, const std::string &prefix,
    const ukey_string &lower, const ukey_string *upper_or_null,
    int limit, int target_bytes, FDBStreamingMode mode, int iteration,
    fdb_bool_t snapshot, fdb_bool_t reverse);

// TODO: Take string_view, key_view.
fdb_future transaction_lookup_pkey_index(
    FDBTransaction *txn, const char *prefix, const ukey_string &index_key);

void transaction_set_pkey_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key,
    const std::string &value);

void transaction_erase_pkey_index(FDBTransaction *txn, const char *prefix,
    const ukey_string &index_key);


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

inline uuid_u parse_uuid_primary_key(key_view k) {
    uuid_u ret;
    bool success = str_to_uuid(as_char(k.data), size_t(k.length), &ret);
    guarantee(success, "Bad uuid primary key");  // TODO: fdb, msg, etc.
    return ret;
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
fdb_value_fut<typename index_traits::value_type>
transaction_lookup_uq_index(
        FDBTransaction *txn,
        const typename index_traits::ukey_type &index_key,
        bool snapshot) {
    fdb_value_fut<typename index_traits::value_type> ret{
        transaction_lookup_unique_index(txn, index_traits::prefix,
            index_traits::ukey_str(index_key), snapshot)};
    return ret;
}

template <class index_traits>
fdb_value_fut<typename index_traits::value_type>
transaction_lookup_uq_index_raw(
        FDBTransaction *txn,
        const ukey_string &index_raw_key) {
    fdb_value_fut<typename index_traits::value_type> ret{
        transaction_lookup_unique_index(txn, index_traits::prefix,
            index_raw_key)};
    return ret;
}

template <class index_traits>
void transaction_erase_uq_index(
        FDBTransaction *txn,
        const typename index_traits::ukey_type &index_key) {
    transaction_erase_unique_index(txn, index_traits::prefix,
        index_traits::ukey_str(index_key));
}

template <class index_traits>
void transaction_set_uq_index(
        FDBTransaction *txn,
        const typename index_traits::ukey_type &index_key,
        const typename index_traits::value_type &value) {
    std::string valstr = serialize_for_cluster_to_string(value);
    transaction_set_unique_index(txn, index_traits::prefix,
        index_traits::ukey_str(index_key), valstr);
}

template <class index_traits>
std::vector<std::pair<typename index_traits::ukey_type, typename index_traits::value_type>>
transaction_read_range_uq_index_coro(const signal_t *interruptor, FDBTransaction *txn) {
    std::vector<std::pair<typename index_traits::ukey_type, typename index_traits::value_type>> ret;
    std::string prefix = index_traits::prefix;
    transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
        [&](const FDBKeyValue &kv) {
            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view ukey = whole_key.guarantee_without_prefix(prefix);
            fdb_node_id key = index_traits::parse_ukey(ukey);
            typename index_traits::value_type value;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &value);
            ret.emplace_back(key, value);
            return true;
        });
    return ret;
}


template <class index_traits>
void transaction_erase_plain_index(
        FDBTransaction *txn,
        const typename index_traits::skey_type &sindex_key,
        const typename index_traits::pkey_type &pkey) {
    transaction_erase_plain_index(txn,
        index_traits::prefix,
        index_traits::skey_str(sindex_key),
        index_traits::pkey_str(pkey));
}

// At some point, we might have a value_type instead of the std::string value.  (Right
// now it's only used as an empty string, so I don't care to bother.)
template <class index_traits>
void transaction_set_plain_index(
        FDBTransaction *txn,
        const typename index_traits::skey_type &sindex_key,
        const typename index_traits::pkey_type &pkey,
        const std::string &value) {
    transaction_set_plain_index(txn,
        index_traits::prefix,
        index_traits::skey_str(sindex_key),
        index_traits::pkey_str(pkey),
        value);
}

template <class index_traits>
std::string plain_index_skey_prefix(
        const typename index_traits::skey_type &sindex_key) {
    return plain_index_skey_prefix(
        index_traits::prefix, index_traits::skey_str(sindex_key));
}


#endif  // RETHINKDB_FDB_INDEX_HPP_
