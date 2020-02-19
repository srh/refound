#ifndef RETHINKDB_FDB_NODE_HOLDER_HPP_
#define RETHINKDB_FDB_NODE_HOLDER_HPP_

#include "concurrency/auto_drainer.hpp"
#include "concurrency/signal.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"
#include "fdb/reql_fdb.hpp"

class fdb_node_holder {
public:
    explicit fdb_node_holder(FDBDatabase *fdb, const signal_t *interruptor);
    ~fdb_node_holder();

    // Shuts down the node holder coro.
    void shutdown(const signal_t *interruptor);

    fdb_node_id get_node_id() const { return node_id_; }

private:
    FDBDatabase *fdb_;
    // TODO: Remove server_id_t entirely, or pass it in.
    const fdb_node_id node_id_;
    bool initiated_shutdown_ = false;
    auto_drainer_t drainer_;
    DISABLE_COPYING(fdb_node_holder);
};



#endif  // RETHINKDB_FDB_NODE_HOLDER_HPP_
