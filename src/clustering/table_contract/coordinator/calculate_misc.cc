// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/coordinator/calculate_misc.hpp"

/* `calculate_member_ids_and_raft_config()` figures out when servers need to be added to
or removed from the `table_raft_state_t::member_ids` map or the Raft configuration. The
goals are as follows:
- A server ought to be in `member_ids` and the Raft configuration if it appears in the
    current `table_config_t` or in a contract.
- If it is a voter in any contract, it should be a voter in the Raft configuration;
    otherwise, it should be a non-voting member.

There is constraint on how we approach those goals: Every server in the Raft
configuration must also be in `member_ids`. So we add new servers to `member_ids` before
adding them to the Raft configuration, and we remove obsolete servers from the Raft
configuration before removing them from `member_ids`. Note that `calculate_member_ids()`
assumes that if it returns changes for both `member_ids` and the Raft configuration, the
`member_ids` changes will be applied first. */
void calculate_member_ids_and_raft_config(
        const raft_member_t<table_raft_state_t>::state_and_config_t &sc,
        std::set<server_id_t> *remove_member_ids_out,
        std::map<server_id_t, raft_member_id_t> *add_member_ids_out,
        raft_config_t *new_config_out) {
    /* Assemble a set of all of the servers that ought to be in `member_ids`. */
    std::set<server_id_t> members_goal;
    members_goal.insert(sc.state.config.config.the_shard.primary_replica);
    for (const auto &pair : sc.state.contracts) {
        members_goal.insert(pair.second.the_server);
    }
    /* Assemble a set of all of the servers that ought to be Raft voters. */
    std::set<server_id_t> voters_goal;
    for (const auto &pair : sc.state.contracts) {
        voters_goal.insert(pair.second.the_server);
    }
    /* Create entries in `add_member_ids_out` for any servers in `goal` that don't
    already have entries in `member_ids` */
    for (const server_id_t &server : members_goal) {
        if (sc.state.member_ids.count(server) == 0) {
            add_member_ids_out->insert(std::make_pair(
                server, raft_member_id_t(generate_uuid())));
        }
    }
    /* For any servers in the current `member_ids` that aren't in `servers`, add an entry
    in `remove_member_ids_out`, unless they are still in the Raft configuration. (If
    they're still in the Raft configuration, we'll remove them soon.) */
    for (const auto &existing : sc.state.member_ids) {
        if (members_goal.count(existing.first) == 0 &&
                !sc.config.is_member(existing.second)) {
            remove_member_ids_out->insert(existing.first);
        }
    }
    /* Set up `new_config_out`. */
    for (const server_id_t &server : members_goal) {
        raft_member_id_t member_id = (sc.state.member_ids.count(server) == 1)
            ? sc.state.member_ids.at(server)
            : add_member_ids_out->at(server);
        if (voters_goal.count(server) == 1) {
            new_config_out->voting_members.insert(member_id);
        } else {
            new_config_out->non_voting_members.insert(member_id);
        }
    }
}

