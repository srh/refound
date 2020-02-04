#ifndef RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
#define RETHINKDB_FDB_REQL_FDB_UTILS_HPP_

#include <limits.h>

#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/vector_stream.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"
#include "fdb/reql_fdb.hpp"


template <class T>
void deserialize_off_fdb(const uint8_t *value, int value_length, T *out) {
    buffer_read_stream_t stream(as_char(value), size_t(value_length));
    // TODO: serialization versioning.
    archive_result_t res = deserialize<cluster_version_t::LATEST_DISK>(&stream, out);
    guarantee(!bad(res), "bad deserialization from db value");  // TODO: pass error
    guarantee(size_t(stream.tell()) == stream.size());  // TODO: Pass error.
    // TODO: Cleanup error messages in every new fdb guarantee.
}

template <class T>
MUST_USE bool deserialize_off_fdb_value(const fdb_value &value, T *out) {
    if (!value.present) {
        return false;
    }

    deserialize_off_fdb(value.data, value.length, out);
    return true;
}

template <class T>
void get_and_deserialize(FDBTransaction *txn, const char *key, const signal_t *interruptor, T *out) {
    fdb_future value_fut = transaction_get_c_str(txn, key);
    fdb_value value = future_block_on_value(value_fut.fut, interruptor);
    bool value_present = deserialize_off_fdb_value(value, out);
    guarantee(value_present);  // TODO: pass error
}

template <class T>
void serialize_and_set(FDBTransaction *txn, const char *key, const T &value) {
    std::vector<char> data = serialize_for_cluster_to_vector(value);
    guarantee(data.size() <= INT_MAX);
    int data_size = data.size();

    fdb_transaction_set(
        txn,
        as_uint8(key),
        strlen(key),
        as_uint8(data.data()),
        data_size);
}


template <class Callable>
void transaction_read_whole_range_coro(FDBTransaction *txn,
        std::string begin, const std::string &end,
        const signal_t *interruptor,
        Callable &&cb) {
    bool or_equal = true;
    // We need to get elements greater than (or_equal, if true) begin, less than end.
    for (;;) {
        fdb_future fut{
            fdb_transaction_get_range(txn,
                    as_uint8(begin.data()), int(begin.size()), !or_equal, 1,
                    FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(end.data()), int(end.size())),
                    0,
                    0,
                    FDB_STREAMING_MODE_WANT_ALL,
                    0,
                    false,
                    false)};
        fut.block_coro(interruptor);

        const FDBKeyValue *kvs;
        int kv_count;
        fdb_bool_t more;
        fdb_error_t err = fdb_future_get_keyvalue_array(fut.fut, &kvs, &kv_count, &more);
        check_for_fdb_transaction(err);
        or_equal = false;
        for (int i = 0; i < kv_count; ++i) {
            if (!cb(kvs[i])) {
                return;
            }
        }
        if (more) {
            if (kv_count > 0) {
                const FDBKeyValue &kv = kvs[kv_count - 1];
                begin = std::string(void_as_char(kv.key), size_t(kv.key_length));
            }
        } else {
            return;
        }
    }
}

inline std::string table_key_prefix(const namespace_id_t &table_id) {
    // TODO: Use binary uuid's.  This is on a fast path...
    // Or don't even use uuid's.
    std::string ret = "tables/";
    uuid_onto_str(table_id.value, &ret);
    ret += '/';
    return ret;
}

inline std::string table_index_prefix(
        const namespace_id_t &table_id,
        const sindex_id_t &index_id) {
    std::string ret = table_key_prefix(table_id);
    uuid_onto_str(index_id.value, &ret);
    ret += '/';
    return ret;
}

inline std::string table_pkey_prefix(
        const namespace_id_t &table_id) {
    std::string ret = table_key_prefix(table_id);
    ret += '/';
    return ret;
}



#endif  // RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
