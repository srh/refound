// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/tables/calculate_status.hpp"

#include "clustering/administration/tables/table_metadata.hpp"
#include "clustering/administration/servers/config_client.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "clustering/table_manager/table_metadata.hpp"
#include "concurrency/exponential_backoff.hpp"

bool wait_for_table_readiness(
        UNUSED const namespace_id_t &table_id,
        UNUSED table_readiness_t readiness,
        UNUSED table_meta_client_t *table_meta_client,
        UNUSED const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t) {
    // QQQ: we used to, say, wait for tables' readiness.
    return true;
}

size_t wait_for_many_tables_readiness(
        UNUSED const std::set<namespace_id_t> &original_tables,
        UNUSED table_readiness_t readiness,
        UNUSED table_meta_client_t *table_meta_client,
        UNUSED const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t) {
    // QQQ: we used to do something here.
    return true;
}

void get_table_status(
        const namespace_id_t &table_id,
        const table_config_and_shards_t &config,
        table_meta_client_t *table_meta_client,
        server_config_client_t *server_config_client,
        const signal_t *interruptor,
        table_status_t *status_out)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t) {
    status_out->total_loss = false;
    *status_out->config = config;

    /* Get the Raft leader for this table. */
    table_meta_client->get_raft_leader(table_id, interruptor, &status_out->raft_leader);

    /* Send the status query to every server for the table. */
    bool all_replicas_ready;
    try {
        table_meta_client->get_shard_status(
            table_id, all_replicas_ready_mode_t::EXCLUDE_RAFT_TEST, interruptor,
            &status_out->server_shards, &all_replicas_ready);
    } catch (const failed_table_op_exc_t &) {
        all_replicas_ready = false;
        status_out->server_shards.clear();
    }

    /* We need to pay special attention to servers that appear in the config but not in
    the status response. There are two possible reasons: they might be disconnected, or
    they might not be hosting the server currently. In the former case we insert them
    into `disconnected`, and in the latter case we set their state to `transitioning` in
    `server_shards`. */
    {
        server_id_t server = config.config.the_shard.primary_replica;
        if (status_out->server_shards.count(server) == 0 &&
                status_out->disconnected.count(server) == 0) {
            if (server_config_client->
                    get_server_to_peer_map()->get_key(server).has_value()) {
                table_shard_status_t status;
                status.transitioning = true;
                status_out->server_shards.insert(std::make_pair(server,
                    range_map_t<key_range_t::right_bound_t,
                                table_shard_status_t>(
                        key_range_t::right_bound_t(store_key_t()),
                        key_range_t::right_bound_t::make_unbounded(),
                        std::move(status))));

            } else {
                status_out->disconnected.insert(server);
            }
        }
    }

    /* Collect server names. We need the name of every server in the config, every
    server in `server_shards`, and the Raft leader. */
    std::set<server_id_t> server_ids;
    for (const auto &server_shard : status_out->server_shards) {
        server_ids.insert(server_shard.first);
    }
    if (static_cast<bool>(status_out->raft_leader)) {
        server_ids.insert(status_out->raft_leader.get());
    }

    /* Compute the overall level of table readiness by sending probe queries. */
    // QQQ: We used to use all_replicas_ready or call namespace_interface_t::check_readiness here.
    status_out->readiness = table_readiness_t::finished;
}

