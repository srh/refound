// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/immediate_consistency/primary_dispatcher.hpp"

/* Limits how many writes should be sent to a dispatchee at once. */
primary_dispatcher_t::primary_dispatcher_t(
        const version_t &base_version)
{
    branch_id = branch_id_t{generate_uuid()};
    branch_bc.origin = base_version;
    branch_bc.initial_timestamp = base_version.timestamp;
}
