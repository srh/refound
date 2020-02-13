// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/immediate_consistency/primary_dispatcher.hpp"

/* Limits how many writes should be sent to a dispatchee at once. */
const size_t DISPATCH_WRITES_CORO_POOL_SIZE = 64;

primary_dispatcher_t::primary_dispatcher_t(
        perfmon_collection_t *parent_perfmon_collection,
        const region_map_t<version_t> &base_version) :
    perfmon_membership(parent_perfmon_collection, &perfmon_collection, "broadcaster"),
    ready_dispatchees_as_set(std::set<server_id_t>())
{
    current_timestamp = state_timestamp_t::zero();
    base_version.visit(base_version.get_domain(),
        [&](const region_t &, const version_t &v) {
            current_timestamp = std::max(v.timestamp, current_timestamp);
        });
    most_recent_acked_write_timestamp = current_timestamp;

    branch_id = branch_id_t{generate_uuid()};
    branch_bc.origin = base_version;
    branch_bc.initial_timestamp = current_timestamp;
}

void primary_dispatcher_t::refresh_ready_dispatchees_as_set() {
    /* Note that it's possible that we'll have multiple dispatchees with the same server
    ID. This won't happen during normal operation, but it can happen temporarily during
    a transition. For example, a secondary might disconnect and reconnect as a different
    dispatchee before we realize that its original dispatchee is no longer valid. */
    std::set<server_id_t> ready;
    ready_dispatchees_as_set.set_value(ready);
}

