// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/emergency_repair.hpp"

/* Returns `true` if `servers` is a subset of `dead`. */
bool all_dead(
        const server_id_t &server, const std::set<server_id_t> &dead) {
    return dead.count(server) == 1;
}

/* Returns `true` if half or more of the servers in `servers` are also in `dead`. */
bool quorum_dead(
        const server_id_t &server, const std::set<server_id_t> &dead) {
    return dead.count(server) == 1;
}

/* Returns `true` if any of the servers in `servers` is also in `dead`. */
bool any_dead(
        const server_id_t &server, const std::set<server_id_t> &dead) {
    return dead.count(server) == 1;
}

// TODO: Move dead_servers assertion to caller.
void calculate_emergency_repair(
        const table_raft_state_t &old_state,
        const std::set<server_id_t> &dead_servers,
        emergency_repair_mode_t mode,
        table_raft_state_t *new_state_out,
        bool *rollback_found_out,
        bool *erase_found_out) {
    *rollback_found_out = false;
    *erase_found_out = false;
    if (dead_servers.size() > 0) {
        crash("There are dead servers?  But we are the only server.");
    }

    /* If we're in `"_debug_recommit"` mode we simply copy the old state and are done */
    if (mode == emergency_repair_mode_t::DEBUG_RECOMMIT) {
        *new_state_out = old_state;
        return;
    }

    /* Pick the server we'll use as a replacement for shards that we end up erasing */
    server_id_t erase_replacement = server_id_t::from_server_uuid(nil_uuid());
    for (const auto &pair : old_state.member_ids) {
        erase_replacement = pair.first;
        break;
    }
    guarantee(!erase_replacement.get_uuid().is_nil(),
        "calculate_emergency_repair() should not be called if all servers are dead");

    /* Calculate the new contracts. This is the guts of the repair operation. */
    new_state_out->contracts.clear();
    for (const auto &pair : old_state.contracts) {
        contract_t contract = pair.second.second;

        /* We generate a new contract ID for the new contract, whether or not it's
        identical to the old contract. In practice this doesn't matter since contract IDs
        from the old epoch will never be compared to contract IDs from the new epoch. */
        new_state_out->contracts.insert(std::make_pair(
            generate_uuid(),
            std::make_pair(pair.second.first, contract)));
    }

    /* Calculate the new config to reflect the new contracts as closely as possible. The
    details here aren't critical for correctness purposes. */
    {
        /* Copy all the simple fields of the old config onto the new config */
        new_state_out->config.config.basic = old_state.config.config.basic;
        new_state_out->config.config.sindexes = old_state.config.config.sindexes;
        new_state_out->config.config.write_ack_config =
            old_state.config.config.write_ack_config;
        new_state_out->config.config.durability = old_state.config.config.durability;
        new_state_out->config.config.user_data = old_state.config.config.user_data;

        /* We first calculate all the voting and nonvoting replicas for each range in a
        `range_map_t`. */
        range_map_t<key_range_t::right_bound_t, table_config_t::shard_t> config(
            key_range_t::right_bound_t(store_key_t::min()),
            key_range_t::right_bound_t::make_unbounded(),
            table_config_t::shard_t());
        for (const auto &pair : new_state_out->contracts) {
            config.visit_mutable(
                key_range_t::right_bound_t(pair.second.first.left),
                pair.second.first.right,
                [&](const key_range_t::right_bound_t &,
                        const key_range_t::right_bound_t &,
                        table_config_t::shard_t *shard) {
                    {
                        server_id_t server = pair.second.second.the_replica;
                        shard->primary_replica = server;
                    }
                });
        }

        /* Convert each section of the `range_map_t` into a new user-level shard. */
        config.visit(
            key_range_t::right_bound_t(store_key_t::min()),
            key_range_t::right_bound_t::make_unbounded(),
            [&](const key_range_t::right_bound_t &,
                    const key_range_t::right_bound_t &right,
                    const table_config_t::shard_t &shard) {
                new_state_out->config.config.the_shard = shard;
                guarantee(right.unbounded);
            });

        /* Copy over server names for all servers that are in the new config, into the
        server names map for the config. (Note that there are two separate server name
        maps, one for the config and one for the contracts.) */
        {
            table_config_t::shard_t shard = new_state_out->config.config.the_shard;
            {
                server_id_t server = shard.primary_replica;
                /* Since the new config is derived from the new contracts which are
                derived from the old contracts, every server present in the new config
                must appear in `old_state.server_names`. */
                new_state_out->config.server_names.names.insert(
                    std::make_pair(server, old_state.server_names.names.at(server)));
            }
        }
    }

    /* Copy over the branch history without modification. In theory we could do some
    pruning here, but there's not much that we could prune and so it's easier to just let
    the branch history GC do it. */
    new_state_out->branch_history = old_state.branch_history;

    /* Copy over the current branch map as well. Where the new contracts specify
    a primary that doesn't have the current branch due to the hard override, the
    current branch will be reset for its range once it registers its new branch. */
    new_state_out->current_branches = old_state.current_branches;

    /* Find all the servers that appear in the new contracts, and put entries for those
    servers in `member_ids` and `server_names` */
    for (const auto &pair : new_state_out->contracts) {
        {
            server_id_t server = pair.second.second.the_replica;
            if (new_state_out->member_ids.count(server) == 0) {
                /* We generate a new member ID for every server. This shouldn't matter
                because member IDs from the new epoch should never be compared to member
                IDs from the old epoch, but it slightly reduces the risk of bugs. */
                new_state_out->member_ids.insert(
                    std::make_pair(server, raft_member_id_t(generate_uuid())));

                /* Since the new contracts are derived from the old contracts, every
                server present in the new contracts must appear in
                `old_state.server_names`. */
                new_state_out->server_names.names.insert(
                    std::make_pair(server, old_state.server_names.names.at(server)));
            }
        }
    }
}

