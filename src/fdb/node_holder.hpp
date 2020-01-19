#ifndef RETHINKDB_FDB_NODE_HOLDER_HPP_
#define RETHINKDB_FDB_NODE_HOLDER_HPP_

#include "concurrency/auto_drainer.hpp"
#include "concurrency/signal.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"

class fdb_node_holder {
public:
    explicit fdb_node_holder(FDBDatabase *fdb, const signal_t *interruptor);
    ~fdb_node_holder();

    // Shuts down the node holder coro.
    void shutdown(const signal_t *interruptor);

private:
    FDBDatabase *fdb_;
    // TODO: Remove server_id_t entirely, or pass it in.
    const uuid_u node_id_;
    bool initiated_shutdown_ = false;
    auto_drainer_t drainer_;
    DISABLE_COPYING(fdb_node_holder);
};



#endif  // RETHINKDB_FDB_NODE_HOLDER_HPP_
