// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/coordinator/calculate_contracts.hpp"

#include "clustering/table_contract/branch_history_gc.hpp"
#include "logger.hpp"

/* A `contract_ack_t` is not necessarily homogeneous. It may have different `version_t`s
for different regions, and a region with a single `version_t` may need to be split
further depending on the branch history. Since `calculate_contract()` assumes it's
processing a homogeneous input, we need to break the `contract_ack_t` into homogeneous
pieces. `contract_ack_frag_t` is like a homogeneous version of `contract_ack_t`; in place
of the `region_map_t<version_t>` it has a single `state_timestamp_t`. Use
`break_ack_into_fragments()` to convert a `contract_ack_t` into a
`region_map_t<contract_ack_frag_t>`. */

class contract_ack_frag_t {
public:
    bool operator==(const contract_ack_frag_t &x) const {
        return state == x.state && version == x.version &&
            common_ancestor == x.common_ancestor && branch == x.branch;
    }
    bool operator!=(const contract_ack_frag_t &x) const {
        return !(*this == x);
    }

    /* `state` and `branch` are the same as the corresponding fields of the original
    `contract_ack_t` */
    contract_ack_t::state_t state;
    optional<branch_id_t> branch;

    /* `version` is the value of the `version` field of the original `contract_ack_t` for
    the specific sub-region this fragment applies to. */
    optional<version_t> version;

    /* `common_ancestor` is the timestamp of the last common ancestor of the original
    `contract_ack_t` and the `current_branch` in the Raft state for the specific
    sub-region this fragment applies to. If `version` is blank, this will always be
    blank; if `version` is present, `common_ancestor` might or might not be blank
    depending on whether we expect to use the value. */
    optional<state_timestamp_t> common_ancestor;
};

region_map_t<contract_ack_frag_t> break_ack_into_fragments(
        const region_t &region,
        const contract_ack_t &ack,
        const region_map_t<branch_id_t> &current_branches,
        const branch_history_reader_t *raft_branch_history,
        bool compute_common_ancestor) {
    contract_ack_frag_t base_frag;
    base_frag.state = ack.state;
    base_frag.branch = ack.branch;
    if (!static_cast<bool>(ack.version)) {
        return region_map_t<contract_ack_frag_t>(region, base_frag);
    } else if (compute_common_ancestor) {
        branch_history_combiner_t combined_branch_history(
            raft_branch_history, &ack.branch_history);
        /* Fragment over branches and then over versions within each branch. */
        {
            const region_t &ack_version_reg = region;
            const version_t vers = *ack.version;
            base_frag.version = make_optional(vers);
            return current_branches.map_multi(ack_version_reg,
            [&](const region_t &branch_reg, const branch_id_t &branch) {
                version_t points_on_canonical_branch;
                try {
                    points_on_canonical_branch =
                        version_find_branch_common(&combined_branch_history,
                            vers, branch);
                } catch (const missing_branch_exc_t &) {
#ifndef NDEBUG
                    crash("Branch history is incomplete");
#else
                    logERR("The branch history is incomplete. This probably means "
                           "that there is a bug in RethinkDB. Please report this "
                           "at https://github.com/rethinkdb/rethinkdb/issues/ .");
                    /* Recover by using the root branch */
                    points_on_canonical_branch = version_t::zero();
#endif
                }
                base_frag.common_ancestor =
                    make_optional(points_on_canonical_branch.timestamp);
                return region_map_t<contract_ack_frag_t>(branch_reg, base_frag);
            });
        }
    } else {
        base_frag.version = make_optional(*ack.version);
        return region_map_t<contract_ack_frag_t>(region, base_frag);
    }
}

/* `invisible_to_majority_of_set()` returns `true` if `target` definitely cannot be seen
by a majority of the servers in `judges`. If we can't see one of the servers in `judges`,
we'll assume it can see `target` to reduce spurious failoves. */
bool invisible_to_majority_of_set(
        const server_id_t &target,
        const std::set<server_id_t> &judges,
        watchable_map_t<std::pair<server_id_t, server_id_t>, empty_value_t> *
            connections_map) {
    size_t count = 0;
    for (const server_id_t &s : judges) {
        if (connections_map->get_key(std::make_pair(s, target)).has_value() ||
                !connections_map->get_key(std::make_pair(s, s)).has_value()) {
            ++count;
        }
    }
    return !(count > judges.size() / 2);
}

/* A small helper function for `calculate_contract()` to test whether a given
replica is currently streaming. */
bool is_streaming(
        const contract_t &old_c,
        const std::map<server_id_t, contract_ack_frag_t> &acks,
        server_id_t server) {
    // TODO: Nobody is ever in secondary_streaming state.
    auto it = acks.find(server);
    if (it != acks.end() && (old_c.the_server == server)) {
        return true;
    } else {
        return false;
    }
}

/* `calculate_contract()` calculates a new contract for a region. Whenever any of the
inputs changes, the coordinator will call `update_contract()` to compute a contract for
each range of keys. The new contract will often be the same as the old, in which case it
doesn't get a new contract ID. */
contract_t calculate_contract(
        /* The old contract that contains this region. */
        const contract_t &old_c,
        /* Contract acks from replicas regarding `old_c`. If a replica hasn't sent us an
        ack *specifically* for `old_c`, it won't appear in this map; we don't include
        acks for contracts that were in the same region before `old_c`. */
        const std::map<server_id_t, contract_ack_frag_t> &acks) {
     (void)acks;  // TODO: Remove unused params.


    contract_t new_c = old_c;

    /* If there are new servers in `config.all_replicas`, add them to `c.replicas` */
    new_c.the_server = server_id_t();

    return new_c;
}

/* `calculate_all_contracts()` is sort of like `calculate_contract()` except that it
applies to the whole set of contracts instead of to a single contract. It takes the
inputs that `calculate_contract()` needs, but in sharded form; then breaks the key space
into small enough chunks that the inputs are homogeneous across each chunk; then calls
`calculate_contract()` on each chunk.

The output is in the form of a diff instead of a set of new contracts. We need a diff to
put in the `table_raft_state_t::change_t::new_contracts_t`, and we need to compute the
diff anyway in order to reuse contract IDs for contracts that haven't changed, so it
makes sense to combine those two diff processes. */
void calculate_all_contracts(
        const table_raft_state_t &old_state,
        const std::map<contract_id_t, std::map<server_id_t, contract_ack_t> > &acks,
        std::set<contract_id_t> *remove_contracts_out,
        std::map<contract_id_t, contract_t> *add_contracts_out,
        std::map<region_t, branch_id_t> *register_current_branches_out,
        std::set<branch_id_t> *remove_branches_out,
        branch_history_t *add_branches_out) {

    ASSERT_FINITE_CORO_WAITING;

    /* Initially, we put every branch into `remove_branches_out`. Then as we process
    contracts, we "mark branches live" by removing them from `remove_branches_out`. */
    for (const auto &pair : old_state.branch_history.branches) {
        remove_branches_out->insert(pair.first);
    }

    std::vector<region_t> new_contract_region_vector;
    std::vector<contract_t> new_contract_vector;

    /* We want to break the key-space into sub-regions small enough that the contract,
    table config, and ack versions are all constant across the sub-region. First we
    iterate over all contracts: */
    for (const std::pair<const contract_id_t, contract_t> &cpair :
            old_state.contracts) {
        /* Next iterate over all shards of the table config and find the ones that
        overlap the contract in question: */
        {
            /* Find acks for this contract. If there aren't any acks for this contract,
            then `acks` might not even have an empty map, so we need to construct an
            empty map in that case. */
            const std::map<server_id_t, contract_ack_t> *this_contract_acks;
            {
                static const std::map<server_id_t, contract_ack_t> empty_ack_map;
                auto it = acks.find(cpair.first);
                this_contract_acks = (it == acks.end()) ? &empty_ack_map : &it->second;
            }

            /* Now collect the acks for this contract into `ack_frags`. `ack_frags` is
            homogeneous at first and then it gets fragmented as we iterate over `acks`.
            */
            region_map_t<std::map<server_id_t, contract_ack_frag_t> > frags_by_server(
                key_range_t::universe());
            for (const auto &pair : *this_contract_acks) {
                /* Sanity-check the ack */
                DEBUG_ONLY_CODE(pair.second.sanity_check(
                    pair.first, cpair.first, old_state));

                /* There are two situations where we don't compute the common ancestor:
                1. If the server sending the ack is not in `voters` or `temp_voters`
                2. If the contract has the `after_emergency_repair` flag set
                In these situations, we don't need the common ancestor, but computing it
                might be dangerous because the branch history might be incomplete. */
                bool compute_common_ancestor =
                    (cpair.second.the_server == pair.first);

                // TODO: No need for fragments?
                region_map_t<contract_ack_frag_t> frags = break_ack_into_fragments(
                    key_range_t::universe(), pair.second, old_state.current_branches,
                    &old_state.branch_history, compute_common_ancestor);

                frags.visit(key_range_t::universe(),
                [&](const region_t &reg, const contract_ack_frag_t &frag) {
                    frags_by_server.visit_mutable(reg,
                    [&](const region_t &,
                            std::map<server_id_t, contract_ack_frag_t> *acks_map) {
                        auto res = acks_map->insert(
                            std::make_pair(pair.first, frag));
                        guarantee(res.second);
                    });
                });
            }

            frags_by_server.visit(key_range_t::universe(),
            [&](const region_t &reg,
                    const std::map<server_id_t, contract_ack_frag_t> &acks_map) {
                /* We've finally collected all the inputs to `calculate_contract()` and
                broken the key space into regions across which the inputs are
                homogeneous. So now we can actually call it. */

                const contract_t &old_contract = cpair.second;

                contract_t new_contract = calculate_contract(
                    old_contract,
                    acks_map);

                /* Register a branch if a primary is asking us to */
                optional<branch_id_t> registered_new_branch;
                if (true &&
                        true &&
                        true &&
                        acks_map.count(old_contract.the_server) == 1 &&
                        acks_map.at(old_contract.the_server).state ==
                            contract_ack_t::state_t::primary_need_branch) {
                    branch_id_t to_register =
                        *acks_map.at(old_contract.the_server).branch;
                    bool already_registered = true;
                    old_state.current_branches.visit(reg,
                    [&](const region_t &, const branch_id_t &cur_branch) {
                        already_registered &= (cur_branch == to_register);
                    });
                    if (!already_registered) {
                        auto res = register_current_branches_out->insert(
                            std::make_pair(reg, to_register));
                        guarantee(res.second);
                        /* Due to branch garbage collection on the executor,
                        the branch history in the contract_ack might be incomplete.
                        Usually this isn't a problem, because the executor is
                        only going to garbage collect branches when it is sure
                        that the current branches are already present in the Raft
                        state. In that case `copy_branch_history_for_branch()` is
                        not going to traverse to the GCed branches.
                        However this assumption no longer holds if the Raft state
                        has just been overwritten by an emergency repair operation.
                        Hence we ignore missing branches in the copy operation. */
                        copy_branch_history_for_branch(
                            to_register,
                            this_contract_acks->at(
                                old_contract.the_server).branch_history,
                            old_state,
                            false /* ignore_missing_branches */,
                            add_branches_out);
                        registered_new_branch = make_optional(to_register);
                    }
                }

                /* Check to what extent we can confirm that the replicas are on
                `current_branch`. We'll use this to determine when it's safe to GC and
                whether we can switch off the `after_emergency_repair` flag (if it was
                on). */
                bool can_gc_branch_history = true;
                {
                    server_id_t server = new_contract.the_server;
                    auto it = this_contract_acks->find(server);
                    if (it == this_contract_acks->end() || (
                            it->second.state !=
                                contract_ack_t::state_t::primary_ready)) {
                        /* At least one replica can't be confirmed to be on
                        `current_branch`, so we should keep the branch history around in
                        order to make it easy for that replica to rejoin later. */
                        can_gc_branch_history = false;
                    }
                }

                /* Branch history GC. The key decision is whether we should only keep
                `current_branch`, or whether we need to keep all of its ancestors too. */
                old_state.current_branches.visit(reg,
                [&](const region_t &subregion, const branch_id_t &current_branch) {
                    if (!current_branch.is_nil()) {
                        if (can_gc_branch_history) {
                            remove_branches_out->erase(current_branch);
                        } else {
                            mark_all_ancestors_live(current_branch, subregion,
                                &old_state.branch_history, remove_branches_out);
                        }
                    }
                });

                new_contract_region_vector.push_back(reg);
                new_contract_vector.push_back(new_contract);
            });
        }
    }

    // TODO: There is just one region, just one contract.
    /* Put the new contracts into a `region_map_t` to coalesce adjacent regions that have
    identical contracts */
    region_map_t<contract_t> new_contract_region_map =
        region_map_t<contract_t>::from_unordered_fragments(
            std::move(new_contract_region_vector), std::move(new_contract_vector));

    /* Slice the new contracts by shard, so that no contract spans
    more than one shard. */
    // TODO: Contract map/region map just has universe key range now.
    std::map<region_t, contract_t> new_contract_map;
    {
        new_contract_region_map.visit(key_range_t::universe(),
        [&](const region_t &reg, const contract_t &contract) {
            new_contract_map.insert(std::make_pair(reg, contract));
        });
    }

    /* Diff the new contracts against the old contracts */
    for (const auto &cpair : old_state.contracts) {
        auto it = new_contract_map.find(key_range_t::universe());
        if (it != new_contract_map.end() && it->second == cpair.second) {
            /* The contract was unchanged. Remove it from `new_contract_map` to signal
            that we don't need to assign it a new ID. */
            new_contract_map.erase(it);
        } else {
            /* The contract was changed. So delete the old one. */
            remove_contracts_out->insert(cpair.first);
        }
    }
    for (const auto &pair : new_contract_map) {
        /* The contracts remaining in `new_contract_map` are actually new; whatever
        contracts used to cover their region have been deleted. So assign them contract
        IDs and export them. */
        add_contracts_out->insert(
            std::make_pair(contract_id_t{generate_uuid()}, pair.second));
    }
}

