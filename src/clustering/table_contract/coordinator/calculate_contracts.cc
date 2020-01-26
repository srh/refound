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
        return ack.version->map_multi(region,
        [&](const region_t &ack_version_reg, const version_t &vers) {
            base_frag.version = make_optional(vers);
            return current_branches.map_multi(ack_version_reg,
            [&](const region_t &branch_reg, const branch_id_t &branch) {
                region_map_t<version_t> points_on_canonical_branch;
                try {
                    points_on_canonical_branch =
                        version_find_branch_common(&combined_branch_history,
                            vers, branch, branch_reg);
                } catch (const missing_branch_exc_t &) {
#ifndef NDEBUG
                    crash("Branch history is incomplete");
#else
                    logERR("The branch history is incomplete. This probably means "
                           "that there is a bug in RethinkDB. Please report this "
                           "at https://github.com/rethinkdb/rethinkdb/issues/ .");
                    /* Recover by using the root branch */
                    points_on_canonical_branch =
                        region_map_t<version_t>(region_t::universe(),
                                                version_t::zero());
#endif
                }
                return points_on_canonical_branch.map(branch_reg,
                [&](const version_t &common_vers) {
                    base_frag.common_ancestor =
                        make_optional(common_vers.timestamp);
                    return base_frag;
                });
            });
        });
    } else {
        return ack.version->map(region,
        [&](const version_t &vers) {
            base_frag.version = make_optional(vers);
            return base_frag;
        });
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
    auto it = acks.find(server);
    if (it != acks.end() &&
            (it->second.state == contract_ack_t::state_t::secondary_streaming ||
            (static_cast<bool>(old_c.primary) &&
                old_c.primary->server == server))) {
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
        /* The user-specified configuration for the shard containing this region. */
        const table_config_t::shard_t &config,
        /* Contract acks from replicas regarding `old_c`. If a replica hasn't sent us an
        ack *specifically* for `old_c`, it won't appear in this map; we don't include
        acks for contracts that were in the same region before `old_c`. */
        const std::map<server_id_t, contract_ack_frag_t> &acks) {

    contract_t new_c = old_c;

    /* If there are new servers in `config.all_replicas`, add them to `c.replicas` */
    new_c.the_replica = config.primary_replica;

    /* If there is a mismatch between `config.voting_replicas()` and `c.voters`, then
    add and/or remove voters until both sets match.
    There are three important restrictions we have to consider here:
    1. We should not remove voters if that would reduce the total number of voters
       below the minimum of the size of the current voter set and the size of the
       configured voter set. Consider the case where a user wants to replace
       a few replicas by different ones. Without this restriction, we would often
       remove all the old replicas from the voters set before enough of the new
       replicas would become streaming to replace them.
    2. To increase availability in some scenarios, we also don't remove replicas
       if that would mean being left with less than a majority of streaming
       replicas.
    3. Only replicas that are streaming are guaranteed to have a complete branch
       history. Once we make a replica voting, some parts of our logic assume that
       the branch history is intact (most importantly our call to
       `break_ack_into_fragments()` in `calculate_all_contracts()`).
       To avoid this condition, we only add a replica to the voters list after it
       has started streaming.
       See https://github.com/rethinkdb/rethinkdb/issues/4866 for more details. */
    std::set<server_id_t> config_voting_replicas;
    config_voting_replicas.insert(config.primary_replica);

    /* If we already initiated a voter change by setting `temp_voters`, it might be time
    to commit that change by setting `voters` to `temp_voters`. */
    if (static_cast<bool>(old_c.temp_voters)) {
        /* Before we change `voters`, we have to make sure that we'll preserve the
        invariant that every acked write is on a majority of `voters`. This is mostly the
        job of the primary; it will not report `primary_ready` unless it is requiring
        acks from a majority of both `voters` and `temp_voters` before acking writes to
        the client, *and* it has ensured that every write that was acked before that
        policy was implemented has been backfilled to a majority of `temp_voters`. So we
        can't switch voters unless the primary reports `primary_ready`.
        See `primary_execution_t::is_contract_ackable` for the detailed semantics of
        the `primary_ready` state. */
        if (static_cast<bool>(old_c.primary) &&
                acks.count(old_c.primary->server) == 1 &&
                acks.at(old_c.primary->server).state ==
                    contract_ack_t::state_t::primary_ready) {
            /* The `acks` we just checked are based on `old_c`, so we really shouldn't
            commit any different set of `temp_voters`. */
            guarantee(new_c.temp_voters == old_c.temp_voters);
            /* OK, it's safe to commit. */
            guarantee(new_c.temp_voters->size() == 1);
            new_c.the_voter = *new_c.temp_voters->begin();
            new_c.temp_voters.reset();
        }
    }

    /* `visible_voters` includes all members of `voters` and `temp_voters` which could be
    visible to a majority of `voters` (and `temp_voters`, if `temp_voters` exists). Note
    that if the coordinator can't see server X, it will assume server X can see every
    other server; this reduces spurious failovers when the coordinator loses contact with
    other servers. */
    std::set<server_id_t> visible_voters;
    {
        server_id_t server = new_c.the_replica;
        visible_voters.insert(server);
    }

    /* If a server was removed from `config.replicas` and `c.voters` but it's still in
    `c.replicas`, then remove it. And if it's primary, then make it not be primary. */
    bool should_kill_primary = false;

    /* If we don't have a primary, choose a primary. Servers are not eligible to be a
    primary unless they are carrying every acked write. There will be at least one
    eligible server if and only if we have reports from a majority of `new_c.voters`.

    In addition, we must choose `config.primary_replica` if it is eligible. If
    `config.primary_replica` has not sent an ack, we must wait for the failover timeout
    to elapse before electing a different replica. This is to make sure that we won't
    elect the wrong replica simply because the user's designated primary took a little
    longer to send the ack. */
    if (!static_cast<bool>(old_c.primary) && !old_c.after_emergency_repair) {
        /* We have an invariant that every acked write must be on the path from the root
        of the branch history to `old_c.branch`. So we project each voter's state onto
        that path, then sort them by position along the path. Any voter that is at least
        as up to date, according to that metric, as more than half of the voters
        (including itself) is eligible. We also take into account whether a server is
        visible to its peers when deciding which server to select. */

        /* First, collect the states from the servers, and sort them by how up-to-date
        they are. Note that we use the server ID as a secondary sorting key. This mean we
        tend to pick the same server if we run the algorithm twice; this helps to reduce
        unnecessary fragmentation. */
        std::vector<std::pair<state_timestamp_t, server_id_t> > sorted_candidates;
        {
            const server_id_t server = new_c.the_voter;
            if (acks.count(server) == 1 && acks.at(server).state ==
                    contract_ack_t::state_t::secondary_need_primary) {
                sorted_candidates.push_back(
                    std::make_pair(*(acks.at(server).common_ancestor), server));
            }
        }
        std::sort(sorted_candidates.begin(), sorted_candidates.end());

        /* Second, determine which servers are eligible to become primary on the basis of
        their data and their visibility to their peers. */
        std::vector<server_id_t> eligible_candidates;
        for (size_t i = 0; i < sorted_candidates.size(); ++i) {
            server_id_t server = sorted_candidates[i].second;
            /* If the server is not visible to more than half of its peers, then it is
            not eligible to be primary */
            if (visible_voters.count(server) == 0) {
                continue;
            }
            /* `up_to_date_count` is the number of servers that `server` is at least as
            up-to-date as. We know `server` must be at least as up-to-date as itself and
            all of the servers that are earlier in the list. */
            size_t up_to_date_count = i + 1;
            /* If there are several servers with the same timestamp, they will appear
            together in the list. So `server` may be at least as up-to-date as some of
            the servers that appear after it in the list. */
            while (up_to_date_count < sorted_candidates.size() &&
                    sorted_candidates[up_to_date_count].first ==
                        sorted_candidates[i].first) {
                ++up_to_date_count;
            }
            /* OK, now `up_to_date_count` is the number of servers that this server is
            at least as up-to-date as. */
            if (up_to_date_count > 0 /* new_c.voters.size() / 2 */) {
                eligible_candidates.push_back(server);
            }
        }

        /* OK, now we can pick a primary. */
        auto it = std::find(eligible_candidates.begin(), eligible_candidates.end(),
                config.primary_replica);
        if (it != eligible_candidates.end()) {
            /* The user's designated primary is eligible, so use it. */
            contract_t::primary_t p;
            p.server = config.primary_replica;
            new_c.primary.set(p);
        } else if (!eligible_candidates.empty()) {
            /* The user's designated primary is ineligible. We have to decide if we'll
            wait for the user's designated primary to become eligible, or use one of the
            other eligible candidates. */
            if (!config.primary_replica.get_uuid().is_nil() &&
                    visible_voters.count(config.primary_replica) == 1 &&
                    acks.count(config.primary_replica) == 0) {
                /* The user's designated primary is visible to a majority of its peers,
                and the only reason it was disqualified is because we haven't seen an ack
                from it yet. So we'll wait for it to send in an ack rather than electing
                a different primary. */
            } else {
                /* We won't wait for it. */
                contract_t::primary_t p;
                /* `eligible_candidates` is ordered by how up-to-date they are */
                p.server = eligible_candidates.back();
                new_c.primary.set(p);
            }
        }

    } else if (!static_cast<bool>(old_c.primary) && old_c.after_emergency_repair) {
        /* If we recently completed an emergency repair operation, we use a different
        procedure to pick the primary: we require all voters to be present, and then we
        pick the voter with the highest version, regardless of `current_branch`. The
        reason we can't take `current_branch` into account is that the emergency repair
        might create gaps in the branch history, so we can't reliably compute each
        replica's position on `current_branch`. The reason we require all replicas to be
        present is to make the emergency repair safer. For example, imagine if a table
        has three voters, A, B, and C. Suppose that voter B is lagging behind the others,
        and voter C is removed by emergency repair. Then the normal algorithm could pick
        voter B as the primary, even though it's missing some data. */
        server_id_t best_primary = server_id_t::from_server_uuid(nil_uuid());
        state_timestamp_t best_timestamp = state_timestamp_t::zero();
        bool all_present = true;
        {
            server_id_t server = new_c.the_voter;
            if (acks.count(server) == 1 && acks.at(server).state ==
                    contract_ack_t::state_t::secondary_need_primary) {
                state_timestamp_t timestamp = acks.at(server).version->timestamp;
                if (best_primary.get_uuid().is_nil() || timestamp > best_timestamp) {
                    best_primary = server;
                    best_timestamp = timestamp;
                }
            } else {
                all_present = false;
            }
        }
        if (all_present) {
            contract_t::primary_t p;
            p.server = best_primary;
            new_c.primary.set(p);
        }
    }

    /* Sometimes we already have a primary, but we need to pick a different one. There
    are three such situations:
    - The existing primary is disconnected
    - The existing primary isn't `config.primary_replica`, and `config.primary_replica`
        is ready to take over the role
    - `config.primary_replica` isn't ready to take over the role, but the existing
        primary isn't even supposed to be a replica anymore.
    In the first situation, we'll simply remove `c.primary`. In the second and third
    situations, we'll first set `c.primary->warm_shutdown` to `true`, and then only
    once the primary acknowledges that, we'll remove `c.primary`. Either way, once the
    replicas acknowledge the contract in which we removed `c.primary`, the logic earlier
    in this function will select a new primary. Note that we can't go straight from the
    old primary to the new one; we need a majority of replicas to promise to stop
    receiving updates from the old primary before it's safe to elect a new one. */
    if (static_cast<bool>(old_c.primary)) {
        /* Note we already checked for the case where the old primary wasn't supposed to
        be a replica. If this is so, then `should_kill_primary` will already be set to
        `true`. */

        /* Check if we need to do an auto-failover. The precise form of this condition
        isn't important for correctness. If we do an auto-failover when the primary isn't
        actually dead, or don't do an auto-failover when the primary is actually dead,
        the worst that will happen is we'll lose availability. */
        if (!should_kill_primary && visible_voters.count(old_c.primary->server) == 0) {
            should_kill_primary = true;
        }

        if (should_kill_primary) {
            new_c.primary.reset();
        } else if (old_c.primary->server != config.primary_replica) {
            /* The old primary is still a valid replica, but it isn't equal to
            `config.primary_replica`. So we have to do a hand-over to ensure that after
            we kill the primary, `config.primary_replica` will be a valid candidate. */

            if (old_c.primary->hand_over !=
                    make_optional(config.primary_replica)) {
                /* We haven't started the hand-over yet, or we're in the middle of a
                hand-over to a different primary. */
                if (acks.count(config.primary_replica) == 1 &&
                        acks.at(config.primary_replica).state ==
                            contract_ack_t::state_t::secondary_streaming &&
                        visible_voters.count(config.primary_replica) == 1) {
                    /* The new primary is ready, so begin the hand-over. */
                    new_c.primary->hand_over.set(config.primary_replica);
                } else {
                    /* We're not ready to switch to the new primary yet. */
                    if (static_cast<bool>(old_c.primary->hand_over)) {
                        /* We were in the middle of a hand over to a different primary,
                        and then the user changed `config.primary_replica`. But the new
                        primary isn't ready yet, so cancel the old hand-over. (This is
                        very uncommon.) */
                        new_c.primary->hand_over.reset();
                    }
                }
            } else {
                /* We're already in the process of handing over to the new primary. */
                if (acks.count(old_c.primary->server) == 1 &&
                        acks.at(old_c.primary->server).state ==
                            contract_ack_t::state_t::primary_ready) {
                    /* The hand over is complete. Now it's safe to stop the old primary.
                    The new primary will be started later, after a majority of the
                    replicas acknowledge that they are no longer listening for writes
                    from the old primary.
                    See `primary_execution_t::is_contract_ackable` for a detailed
                    explanation of what the `primary_ready` state implies. */
                    new_c.primary.reset();
                } else if (visible_voters.count(config.primary_replica) == 0) {
                    /* Something went wrong with the new primary before the hand-over was
                    complete. So abort the hand-over. */
                    new_c.primary->hand_over.reset();
                }
            }
        } else {
            if (static_cast<bool>(old_c.primary->hand_over)) {
                /* We were in the middle of a hand over, but then the user changed
                `config.primary_replica` back to what it was before. (This is very
                uncommon.) */
                new_c.primary->hand_over.reset();
            }
        }
    }

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
                    (cpair.second.the_voter == pair.first ||
                        (static_cast<bool>(cpair.second.temp_voters) &&
                            cpair.second.temp_voters->count(pair.first) == 1)) &&
                    !cpair.second.after_emergency_repair;

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
                    old_state.config.config.the_shard,
                    acks_map);

                /* Register a branch if a primary is asking us to */
                optional<branch_id_t> registered_new_branch;
                if (static_cast<bool>(old_contract.primary) &&
                        static_cast<bool>(new_contract.primary) &&
                        old_contract.primary->server ==
                            new_contract.primary->server &&
                        acks_map.count(old_contract.primary->server) == 1 &&
                        acks_map.at(old_contract.primary->server).state ==
                            contract_ack_t::state_t::primary_need_branch) {
                    branch_id_t to_register =
                        *acks_map.at(old_contract.primary->server).branch;
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
                        bool ignore_missing_branches
                            = old_contract.after_emergency_repair;
                        copy_branch_history_for_branch(
                            to_register,
                            this_contract_acks->at(
                                old_contract.primary->server).branch_history,
                            old_state,
                            ignore_missing_branches,
                            add_branches_out);
                        registered_new_branch = make_optional(to_register);
                    }
                }

                /* Check to what extent we can confirm that the replicas are on
                `current_branch`. We'll use this to determine when it's safe to GC and
                whether we can switch off the `after_emergency_repair` flag (if it was
                on). */
                bool can_gc_branch_history = true, can_end_after_emergency_repair = true;
                {
                    server_id_t server = new_contract.the_replica;
                    auto it = this_contract_acks->find(server);
                    if (it == this_contract_acks->end() || (
                            it->second.state !=
                                contract_ack_t::state_t::primary_ready &&
                            it->second.state !=
                                contract_ack_t::state_t::secondary_streaming)) {
                        /* At least one replica can't be confirmed to be on
                        `current_branch`, so we should keep the branch history around in
                        order to make it easy for that replica to rejoin later. */
                        can_gc_branch_history = false;

                        if (new_contract.the_voter == server ||
                                (static_cast<bool>(new_contract.temp_voters) &&
                                    new_contract.temp_voters->count(server) == 1)) {
                            /* If the `after_emergency_repair` flag is set to `true`, we
                            need to leave it set to `true` until we can confirm that
                            the branch history is intact. */
                            can_end_after_emergency_repair = false;
                        }
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

                if (can_end_after_emergency_repair) {
                    new_contract.after_emergency_repair = false;
                }

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

