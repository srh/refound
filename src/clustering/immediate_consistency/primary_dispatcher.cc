// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/immediate_consistency/primary_dispatcher.hpp"

/* Limits how many writes should be sent to a dispatchee at once. */
const size_t DISPATCH_WRITES_CORO_POOL_SIZE = 64;

primary_dispatcher_t::dispatchee_registration_t::dispatchee_registration_t(
        primary_dispatcher_t *_parent,
        dispatchee_t *_dispatchee,
        const server_id_t &_server_id,
        state_timestamp_t *first_timestamp_out) :
    parent(_parent),
    dispatchee(_dispatchee),
    server_id(_server_id),
    is_ready(false),
    queue_count(get_num_threads()),
    queue_count_membership(
        &parent->perfmon_collection,
        &queue_count,
        uuid_to_str(server_id.get_uuid()) + "_broadcast_queue_count"),
    background_write_queue(&queue_count),
    background_write_workers(
        DISPATCH_WRITES_CORO_POOL_SIZE,
        &background_write_queue,
        &background_write_caller),
    latest_acked_write(state_timestamp_t::zero())
{
    parent->assert_thread();

    /* Grab mutex so we don't race with writes that are starting or finishing. */
    DEBUG_VAR mutex_assertion_t::acq_t acq(&parent->mutex);
    ASSERT_FINITE_CORO_WAITING;
    parent->dispatchees[this] = auto_drainer_t::lock_t(&drainer);
    *first_timestamp_out = parent->current_timestamp;
}

primary_dispatcher_t::dispatchee_registration_t::~dispatchee_registration_t() {
    DEBUG_VAR mutex_assertion_t::acq_t acq(&parent->mutex);
    ASSERT_FINITE_CORO_WAITING;
    parent->assert_thread();
    parent->dispatchees.erase(this);
    if (is_ready) {
        parent->refresh_ready_dispatchees_as_set();
    }
}

void primary_dispatcher_t::dispatchee_registration_t::mark_ready() {
    DEBUG_VAR mutex_assertion_t::acq_t acq(&parent->mutex);
    ASSERT_FINITE_CORO_WAITING;
    guarantee(!is_ready);
    is_ready = true;
    parent->refresh_ready_dispatchees_as_set();
}

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
    for (const auto &pair : dispatchees) {
        if (pair.first->is_ready) {
            ready.insert(pair.first->server_id);
        }
    }
    ready_dispatchees_as_set.set_value(ready);
}

