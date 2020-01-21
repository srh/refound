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

    T block_and_deserialize(const signal_t *interruptor) {
        T ret;
        fdb_value value = future_block_on_value(fut, interruptor);
        bool value_present = deserialize_off_fdb_value(value, &ret);
        guarantee(value_present);  // TODO: Pass error.
        return ret;
    }
};

inline fdb_value_fut<reqlfdb_clock> transaction_get_clock(FDBTransaction *txn) {
    return fdb_value_fut<reqlfdb_clock>(transaction_get_c_str(txn, REQLFDB_CLOCK_KEY));
}

#endif  // RETHINKDB_FDB_TYPED_HPP_
