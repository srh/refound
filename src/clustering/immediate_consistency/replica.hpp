// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_IMMEDIATE_CONSISTENCY_REPLICA_HPP_
#define CLUSTERING_IMMEDIATE_CONSISTENCY_REPLICA_HPP_

#include "buffer_cache/types.hpp"
#include "concurrency/timestamp_enforcer.hpp"
#include "containers/uuid.hpp"

class order_token_t;
struct read_t;
struct read_response_t;
class store_view_t;
struct write_t;
struct write_response_t;

/* `replica_t` represents a replica (the local replica) of a shard which is currently
tracking changes to a given branch. `local_replicator_t` and
`remote_replicator_client_t` construct a `replica_t` after they finish initializing the
state of the `store_t` to match the ongoing writes from the
`primary_dispatcher_t`. `replica_t` takes care of actually applying those ongoing writes
and reads to the `store_t`. */

class replica_t : public home_thread_mixin_debug_only_t {
public:
    replica_t(
        store_view_t *store,
        const branch_id_t &branch_id,
        state_timestamp_t timestamp);

    void do_read(
        const read_t &read,
        state_timestamp_t token,
        const signal_t *interruptor,
        read_response_t *response_out);

    /* Warning: If you interrupt `do_write()`, the `replica_t` will be left in an
    undefined state, and you should destroy the `replica_t` soon after. */
    void do_write(
        const write_t &write,
        state_timestamp_t timestamp,
        order_token_t order_token,
        write_durability_t durability,
        const signal_t *interruptor,
        write_response_t *response_out);

    void do_dummy_write(
        const signal_t *interruptor,
        write_response_t *response_out);

private:
    store_view_t *const store;
    branch_id_t const branch_id;

    /* A timestamp is completed in `start_enforcer` when the corresponding write has
    acquired a token from the store, and in `end_enforcer` when the corresponding write
    has completed. */
    timestamp_enforcer_t start_enforcer, end_enforcer;
};

#endif /* CLUSTERING_IMMEDIATE_CONSISTENCY_REPLICA_HPP_ */

