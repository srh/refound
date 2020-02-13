// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/query_routing/table_query_client.hpp"

#include <functional>

#include "clustering/id_types.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/fifo_enforcer.hpp"
#include "concurrency/watchable.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/env.hpp"
#include "rpc/connectivity/peer_id.hpp"

#if RDB_CF
table_query_client_t::table_query_client_t(
        const namespace_id_t &_table_id)
    : table_id(_table_id) {
    start_cond.pulse();
}
#endif  // RDB_CF

