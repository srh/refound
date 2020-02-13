// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/immediate_consistency/primary_dispatcher.hpp"

/* Limits how many writes should be sent to a dispatchee at once. */
primary_dispatcher_t::primary_dispatcher_t(
        const region_map_t<version_t> &base_version)
{
    state_timestamp_t current_timestamp = state_timestamp_t::zero();
    base_version.visit(base_version.get_domain(),
        [&](const region_t &, const version_t &v) {
            current_timestamp = std::max(v.timestamp, current_timestamp);
        });

    branch_id = branch_id_t{generate_uuid()};
    branch_bc.origin = base_version;
    branch_bc.initial_timestamp = current_timestamp;
}
