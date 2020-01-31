#ifndef RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
#define RETHINKDB_FDB_REQL_FDB_UTILS_HPP_

#include <limits.h>

#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/vector_stream.hpp"
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


#endif  // RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
