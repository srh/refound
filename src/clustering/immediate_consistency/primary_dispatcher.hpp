// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_IMMEDIATE_CONSISTENCY_PRIMARY_DISPATCHER_HPP_
#define CLUSTERING_IMMEDIATE_CONSISTENCY_PRIMARY_DISPATCHER_HPP_

#include "clustering/immediate_consistency/history.hpp"
#include "concurrency/coro_pool.hpp"
#include "concurrency/queue/unlimited_fifo.hpp"
#include "concurrency/watchable.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rpc/connectivity/server_id.hpp"

/* The job of the `primary_dispatcher_t` is:
- Take in reads and writes
- Assign timestamps to the reads and writes
- Distribute the reads and writes to the replicas
- Collect the results and return them to the caller

It takes in reads and writes via its `read()` and `spawn_write()` methods. Replicas
register for reads and writes by subclassing `dispatchee_t` and constructing a
`dispatchee_registration_t` object.

There is one `primary_dispatcher_t` for each shard; it's located on the primary
replica server. `primary_execution_t` constructs it. */

class primary_dispatcher_t : public home_thread_mixin_debug_only_t {
private:
    class incomplete_write_t;

public:
    /* There is a 1:1 relationship between `primary_dispatcher_t`s and branches. When the
    `primary_dispatcher_t` is constructed, it constructs a branch using the given region
    map as the origin. You can get the branch's ID and birth certificate via the
    `get_branch_id()` and `get_branch_birth_certificate()` methods. */

    primary_dispatcher_t(
        perfmon_collection_t *parent_perfmon_collection,
        const region_map_t<version_t> &base_version);

    branch_id_t get_branch_id() {
        return branch_id;
    }
    branch_birth_certificate_t get_branch_birth_certificate() {
        return branch_bc;
    }

    clone_ptr_t<watchable_t<std::set<server_id_t> > > get_ready_dispatchees() {
        return ready_dispatchees_as_set.get_watchable();
    }

private:

    void refresh_ready_dispatchees_as_set();

    branch_id_t branch_id;
    branch_birth_certificate_t branch_bc;

    perfmon_collection_t perfmon_collection;
    perfmon_membership_t perfmon_membership;

    mutex_assertion_t mutex;

    state_timestamp_t current_timestamp;
    order_checkpoint_t order_checkpoint;

    /* Once we ack a write, we must make sure that every read that's initiated after that
    will see the result of the write. We use this timestamp to keep track of the most
    recent acked write and produce `min_timestamp_token_t`s from it whenever we send a
    read to a listener. */
    state_timestamp_t most_recent_acked_write_timestamp;

    /* This is just a set that contains the peer ID of each dispatchee in `dispatchees`
    that's readable. We store it separately so we can expose it to code that needs to
    know which replicas are available. */
    watchable_variable_t<std::set<server_id_t> > ready_dispatchees_as_set;

    DISABLE_COPYING(primary_dispatcher_t);
};

#endif /* CLUSTERING_IMMEDIATE_CONSISTENCY_PRIMARY_QUERY_ROUTER_HPP_ */

