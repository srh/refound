// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/contract_metadata.hpp"

#include "containers/archive/boost_types.hpp"
#include "stl_utils.hpp"

#ifndef NDEBUG
void contract_t::sanity_check() const {
}
#endif /* NDEBUG */

RDB_IMPL_EQUALITY_COMPARABLE_1(
    contract_t, the_server);

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(
    contract_t, the_server);

#ifndef NDEBUG
void contract_ack_t::sanity_check(
        const server_id_t &server,
        const contract_id_t &contract_id,
        const table_raft_state_t &raft_state) const {
    const region_t region = key_range_t::universe();
    const contract_t &contract = raft_state.contracts.at(contract_id);

    guarantee(contract.the_server == server,
        "A server sent an ack for a contract that it wasn't a replica for.");

    bool ack_says_primary = state == state_t::primary_need_branch ||
        state == state_t::primary_in_progress || state == state_t::primary_ready;
    bool contract_says_primary = contract.the_server == server;
    guarantee(ack_says_primary == contract_says_primary,
        "The contract says a server should be primary, but it sent a non-primary ack.");

    guarantee((state == state_t::primary_need_branch) == static_cast<bool>(branch),
        "branch_id should be present iff state is primary_need_branch");
    guarantee(false == static_cast<bool>(version),
        "version should be present iff state is secondary_need_primary");
    guarantee(!static_cast<bool>(version) || region_t::universe() == region,
        "version has wrong region");

    bool is_voter = contract.the_server == server;
    if (true && is_voter) {
        try {
            if (state == state_t::primary_need_branch) {
                branch_history_combiner_t combiner(
                    &raft_state.branch_history, &branch_history);
                version_t branch_initial_version(
                    *branch,
                    branch_history.get_branch(*branch).initial_timestamp);
                raft_state.current_branches.visit(region,
                [&](const region_t &, const branch_id_t &cur_branch) {
                    version_find_branch_common(
                        &combiner, branch_initial_version, cur_branch);
                });
            }
        } catch (const missing_branch_exc_t &) {
            crash("Branch history is missing pieces");
        }
    }
}
#endif /* NDEBUG */

RDB_IMPL_SERIALIZABLE_4_FOR_CLUSTER(
    contract_ack_t, state, version, branch, branch_history);
RDB_IMPL_EQUALITY_COMPARABLE_4(
    contract_ack_t, state, version, branch, branch_history);

void table_raft_state_t::apply_change(const table_raft_state_t::change_t &change) {
    class visitor_t : public boost::static_visitor<void> {
    public:
        void operator()(const change_t::set_table_config_t &set_config_change) {
            state->config = set_config_change.new_config;
        }
        void operator()(const change_t::new_contracts_t &new_contracts_change) {
            for (const contract_id_t &cid : new_contracts_change.remove_contracts) {
                state->contracts.erase(cid);
            }
            state->contracts.insert(
                new_contracts_change.add_contracts.begin(),
                new_contracts_change.add_contracts.end());
            for (const branch_id_t &bid : new_contracts_change.remove_branches) {
                state->branch_history.branches.erase(bid);
            }
            state->branch_history.branches.insert(
                new_contracts_change.add_branches.branches.begin(),
                new_contracts_change.add_branches.branches.end());
            for (const auto &region_branch :
                new_contracts_change.register_current_branches) {
                state->current_branches.update(region_branch.first, region_branch.second);
            }
        }
        void operator()(const change_t::new_member_ids_t &new_member_ids_change) {
            for (const server_id_t &sid : new_member_ids_change.remove_member_ids) {
                state->member_ids.erase(sid);
            }
            state->member_ids.insert(
                new_member_ids_change.add_member_ids.begin(),
                new_member_ids_change.add_member_ids.end());
        }
        table_raft_state_t *state;
    } visitor;
    visitor.state = this;
    boost::apply_visitor(visitor, change.v);
    DEBUG_ONLY_CODE(sanity_check());
}

#ifndef NDEBUG
void table_raft_state_t::sanity_check() const {
    for (const auto &pair : contracts) {
        pair.second.sanity_check();
    }
}
#endif /* NDEBUG */

RDB_IMPL_EQUALITY_COMPARABLE_1(
    table_raft_state_t::change_t::set_table_config_t, new_config);
RDB_IMPL_EQUALITY_COMPARABLE_5(
    table_raft_state_t::change_t::new_contracts_t,
    remove_contracts, add_contracts, register_current_branches, add_branches,
    remove_branches);
RDB_IMPL_EQUALITY_COMPARABLE_2(
    table_raft_state_t::change_t::new_member_ids_t, remove_member_ids, add_member_ids);
RDB_IMPL_EQUALITY_COMPARABLE_1(
    table_raft_state_t::change_t, v);
RDB_IMPL_EQUALITY_COMPARABLE_5(
    table_raft_state_t, config, contracts, branch_history, current_branches,
    member_ids);

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(
    table_raft_state_t::change_t::set_table_config_t, new_config);
RDB_IMPL_SERIALIZABLE_5_FOR_CLUSTER(
    table_raft_state_t::change_t::new_contracts_t,
    remove_contracts, add_contracts, remove_branches, add_branches,
    register_current_branches);
RDB_IMPL_SERIALIZABLE_2_SINCE_v2_5(
    table_raft_state_t::change_t::new_member_ids_t,
    remove_member_ids, add_member_ids);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    table_raft_state_t::change_t, v);
RDB_IMPL_SERIALIZABLE_5_FOR_CLUSTER(
    table_raft_state_t, config, contracts, branch_history, current_branches,
    member_ids);

table_raft_state_t make_new_table_raft_state(
        const table_config_and_shards_t &config) {
    table_raft_state_t state;
    state.config = config;
    {
        contract_t contract;
        contract.the_server = server_id_t();
        state.contracts.insert(std::make_pair(contract_id_t{generate_uuid()},
            contract));
        {
            server_id_t server_id = server_id_t();
            if (state.member_ids.count(server_id) == 0) {
                state.member_ids[server_id] = raft_member_id_t(generate_uuid());
            }
        }
    }
    DEBUG_ONLY_CODE(state.sanity_check());
    return state;
}

RDB_IMPL_EQUALITY_COMPARABLE_5(table_shard_status_t,
    primary, secondary, need_primary, need_quorum, transitioning);
RDB_IMPL_SERIALIZABLE_5_FOR_CLUSTER(table_shard_status_t,
    primary, secondary, need_primary, need_quorum, transitioning);

void debug_print(printf_buffer_t *buf, const contract_t &contract) {
    buf->appendf("contract_t { the_server = ");
    debug_print(buf, contract.the_server);
    buf->appendf(" }");
}

const char *contract_ack_state_to_string(contract_ack_t::state_t state) {
    typedef contract_ack_t::state_t state_t;
    switch (state) {
        case state_t::primary_need_branch: return "primary_need_branch";
        case state_t::primary_in_progress: return "primary_in_progress";
        case state_t::primary_ready: return "primary_ready";
        default: unreachable();
    }
}

void debug_print(printf_buffer_t *buf, const contract_ack_t &ack) {
    buf->appendf("contract_ack_t { state = %s",
        contract_ack_state_to_string(ack.state));
    buf->appendf(" version = ");
    debug_print(buf, ack.version);
    buf->appendf(" branch = ");
    debug_print(buf, ack.branch);
    buf->appendf(" branch_history.size = ");
    debug_print(buf, ack.branch_history.branches.size());
    buf->appendf(" }");
}

void debug_print(printf_buffer_t *buf, const table_raft_state_t &state) {
    buf->appendf("table_raft_state_t { config = ...");
    buf->appendf(" contracts = ");
    debug_print(buf, state.contracts);
    buf->appendf(" branch_history.size = ");
    debug_print(buf, state.branch_history.branches.size());
    buf->appendf(" current_branches = ");
    debug_print(buf, state.current_branches);
    buf->appendf(" member_ids = ");
    debug_print(buf, state.member_ids);
    buf->appendf(" }");
}

