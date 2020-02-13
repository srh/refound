// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_IMMEDIATE_CONSISTENCY_LOCAL_REPLICATOR_HPP_
#define CLUSTERING_IMMEDIATE_CONSISTENCY_LOCAL_REPLICATOR_HPP_

#include "clustering/immediate_consistency/version.hpp"
#include "clustering/immediate_consistency/primary_dispatcher.hpp"
#include "clustering/immediate_consistency/replica.hpp"
#include "store_view.hpp"

/* `local_replicator_t` receives writes from a `primary_dispatcher_t` on the same server
and applies them directly to a `store_t` on the same server. It doesn't receive a
back-fill at startup; instead, it asserts that the store's state is equal to the starting
state of the `primary_dispatcher_t`'s branch, and then sets the metainfo to point at the
starting state of the branch. So it must be created before any writes have happened on
the `primary_dispatcher_t`.

There is one `local_replicator_t` on the primary replica server of each shard.
`primary_execution_t` constructs it. */

// NNN: Remove the whole type, no?
class local_replicator_t {
public:
    local_replicator_t(
        primary_dispatcher_t *primary,
        store_view_t *store,
        branch_history_manager_t *bhm,
        const signal_t *interruptor);

    /* This destructor can block */
    ~local_replicator_t();

private:
    store_view_t *const store;
    branch_id_t const branch_id;
};

#endif /* CLUSTERING_IMMEDIATE_CONSISTENCY_LOCAL_REPLICA_HPP_ */

