#ifndef RETHINKDB_FDB_TYPED_HPP_
#define RETHINKDB_FDB_TYPED_HPP_

#include "fdb/reql_fdb.hpp"

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


#endif  // RETHINKDB_FDB_TYPED_HPP_
