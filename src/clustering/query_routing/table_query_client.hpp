// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_QUERY_ROUTING_TABLE_QUERY_CLIENT_HPP_
#define CLUSTERING_QUERY_ROUTING_TABLE_QUERY_CLIENT_HPP_

#include <math.h>

#include <map>
#include <string>
#include <vector>
#include <set>

#include "clustering/administration/auth/permission_error.hpp"
#include "containers/clone_ptr.hpp"
#include "concurrency/fifo_enforcer.hpp"
#include "concurrency/watchable_map.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/protocol.hpp"

#if RDB_CF
class primary_query_client_t;

/* `table_query_client_t` is responsible for sending queries to the cluster. It
instantiates `primary_query_client_t` and `direct_query_client_t` internally; it covers
the entire table whereas they cover single shards. */

class table_query_client_t : public namespace_interface_t {
public:
    table_query_client_t(
            const namespace_id_t &table_id);

    /* Returns a signal that will be pulsed when we have either successfully connected
    or tried and failed to connect to every primary replica that was present at the time
    that the constructor was called. This is to avoid the case where we get errors like
    "lost contact with primary replica" when really we just haven't finished connecting
    yet. */
    signal_t *get_initial_ready_signal() {
        return &start_cond;
    }

private:
    namespace_id_t const table_id;
    /* `start_cond` will be pulsed when we have either successfully connected to
    or tried and failed to connect to every peer present when the constructor
    was called. `start_count` is the number of peers we're still waiting for.
    `starting_up` is true during the constructor so `update_registrants()` knows to
    increment `start_count` when it finds a peer. */
    cond_t start_cond;

    DISABLE_COPYING(table_query_client_t);
};
#endif  // RDB_CF

#endif /* CLUSTERING_QUERY_ROUTING_TABLE_QUERY_CLIENT_HPP_ */
