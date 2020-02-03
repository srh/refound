#ifndef RETHINKDB_FDB_TYPED_HPP_
#define RETHINKDB_FDB_TYPED_HPP_

#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"

// A future that holds a serialized value.
template <class T>
class fdb_value_fut : public fdb_future {
public:
    using fdb_future::fdb_future;
    explicit fdb_value_fut(fdb_future &&movee)
        : fdb_future(std::move(movee)) {}

    MUST_USE bool block_and_deserialize(const signal_t *interruptor, T *out)
            THROWS_ONLY(interrupted_exc_t) {
        fdb_value value = future_block_on_value(fut, interruptor);
        return deserialize_off_fdb_value(value, out);
    }

    T block_and_deserialize(const signal_t *interruptor)
            THROWS_ONLY(interrupted_exc_t) {
        T ret;
        bool value_present = block_and_deserialize(interruptor, &ret);
        guarantee(value_present);  // TODO: Pass error.
        return ret;
    }
};

class fdb_key_fut : public fdb_future {
public:
    using fdb_future::fdb_future;
    explicit fdb_key_fut(fdb_future &&movee)
        : fdb_future(std::move(movee)) {}

    key_view block_and_get_key(const signal_t *interruptor) {
        return future_block_on_key(fut, interruptor);
    }
};

inline fdb_value_fut<reqlfdb_clock> transaction_get_clock(FDBTransaction *txn) {
    return fdb_value_fut<reqlfdb_clock>(transaction_get_c_str(txn, REQLFDB_CLOCK_KEY));
}

inline MUST_USE fdb_value_fut<reqlfdb_config_version> transaction_get_config_version(
        FDBTransaction *txn) {
    return fdb_value_fut<reqlfdb_config_version>(transaction_get_c_str(
        txn, REQLFDB_CONFIG_VERSION_KEY));
}

inline void transaction_set_config_version(FDBTransaction *txn, reqlfdb_config_version cv) {
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
}



#endif  // RETHINKDB_FDB_TYPED_HPP_
