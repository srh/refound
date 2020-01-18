#ifndef RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
#define RETHINKDB_FDB_REQL_FDB_UTILS_HPP_

#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/vector_stream.hpp"
#include "fdb/reql_fdb.hpp"

template <class T>
void get_and_deserialize(FDBTransaction *txn, const char *key, T *out) {
    fdb_future value_fut = get_c_str(txn, key);
    value_fut.block_coro();

    fdb_bool_t present;
    const uint8_t *value;
    int value_length;
    fdb_error_t err = fdb_future_get_value(value_fut.fut,
        &present, &value, &value_length);
    guarantee_fdb_TODO(err, "fdb_future_get_value failed");
    guarantee(present, "fdb_future_get_value did not find value");  // TODO

    buffer_read_stream_t stream(reinterpret_cast<const char *>(value), value_length);
    // TODO: serialization versioning.
    archive_result_t res = deserialize<cluster_version_t::LATEST_DISK>(&stream, out);
    guarantee(!bad(res), "bad deserialization from db value");  // TODO: pass error
    // TODO: Cleanup error messages in every new fdb guarantee.
}

template <class T>
void serialize_and_set(FDBTransaction *txn, const char *key, const T &value) {
    std::vector<char> data = serialize_for_cluster_to_vector(value);
    guarantee(data.size() <= INT_MAX);
    int data_size = data.size();

    fdb_transaction_set(
        txn,
        reinterpret_cast<const uint8_t *>(key),
        strlen(key),
        reinterpret_cast<const uint8_t *>(data.data()),
        data_size);
}


#endif  // RETHINKDB_FDB_REQL_FDB_UTILS_HPP_
