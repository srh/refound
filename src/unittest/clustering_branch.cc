// Copyright 2010-2013 RethinkDB, all rights reserved.
#include "unittest/gtest.hpp"

#include "clustering/immediate_consistency/local_replicator.hpp"
#include "clustering/immediate_consistency/primary_dispatcher.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/protocol.hpp"
#include "unittest/branch_history_manager.hpp"
#include "unittest/clustering_utils.hpp"
#include "unittest/mock_store.hpp"
#include "unittest/simple_mailbox_cluster.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

namespace {

void run_with_primary(
        std::function<void(simple_mailbox_cluster_t *,
                           primary_dispatcher_t *,
                           mock_store_t *,
                           local_replicator_t *,
                           order_source_t *)> fun) {
    order_source_t order_source;
    simple_mailbox_cluster_t cluster;
    cond_t interruptor;

    primary_dispatcher_t primary_dispatcher(
        &get_global_perfmon_collection(),
        region_map_t<version_t>(region_t::universe(), version_t::zero()));

    mock_store_t initial_store(version_t::zero());
    in_memory_branch_history_manager_t branch_history_manager;
    local_replicator_t local_replicator(
        server_id_t::generate_server_id(),
        &primary_dispatcher,
        &initial_store,
        &branch_history_manager,
        &interruptor);

    fun(&cluster,
        &primary_dispatcher,
        &initial_store,
        &local_replicator,
        &order_source);
}

}   /* anonymous namespace */

/* The `ReadWrite` test just sends some reads and writes via the dispatcher to the single
local replica. */

void run_read_write_test(
        UNUSED simple_mailbox_cluster_t *cluster,
        primary_dispatcher_t *dispatcher,
        UNUSED mock_store_t *store,
        UNUSED local_replicator_t *local_replicator,
        order_source_t *order_source) {
    /* Send some writes via the broadcaster to the mirror. */
    std::map<std::string, std::string> values_inserted;
    for (int i = 0; i < 10; i++) {
        std::string key = std::string(1, 'a' + randint(26));
        values_inserted[key] = strprintf("%d", i);
        write_t w = mock_overwrite(key, strprintf("%d", i));
        simple_write_callback_t write_callback;
        dispatcher->spawn_write(
            w,
            order_source->check_in("run_read_write_test(write)"),
            &write_callback);
        write_callback.wait_lazily_unordered();
    }

    /* Now send some reads. */
    for (std::map<std::string, std::string>::iterator it = values_inserted.begin();
            it != values_inserted.end(); it++) {
        fifo_enforcer_source_t fifo_source;
        fifo_enforcer_sink_t fifo_sink;
        fifo_enforcer_sink_t::exit_read_t exiter(&fifo_sink, fifo_source.enter_read());

        read_t r = mock_read(it->first);
        cond_t non_interruptor;
        read_response_t resp;
        dispatcher->read(
            r,
            &exiter,
            order_source->check_in("run_read_write_test(read)").with_read_mode(),
            &non_interruptor,
            &resp);
        EXPECT_EQ(it->second, mock_parse_read_response(resp));
    }
}

TPTEST(ClusteringBranch, ReadWrite) {
    run_with_primary(&run_read_write_test);
}


}   /* namespace unittest */
