// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/immediate_consistency/local_replicator.hpp"

#include "concurrency/cross_thread_signal.hpp"

local_replicator_t::local_replicator_t(
        primary_dispatcher_t *primary,
        store_view_t *_store,
        branch_history_manager_t *bhm,
        const signal_t *interruptor) :
    store(_store)
{
    order_source_t order_source;

#ifndef NDEBUG
    /* Make sure the store's initial value matches the initial value in the branch birth
    certificate (just as a sanity check) */
    read_token_t read_token;
    store->new_read_token(&read_token);
    version_t origin = store->get_metainfo(
        order_source.check_in("local_replica_t(read)").with_read_mode(),
        &read_token, interruptor);
    guarantee(origin == primary->get_branch_birth_certificate().origin);
    guarantee(region_t::universe() ==
        primary->get_branch_birth_certificate().get_region());
#endif

    /* Store the new branch in the branch history manager. We have to do this before we
    put the branch in the store's metainfo so that if we crash, the branch will never be
    in the metainfo but not the branch history manager. */
    {
        on_thread_t thread_switcher(bhm->home_thread());
        bhm->create_branch(
            primary->get_branch_id(),
            primary->get_branch_birth_certificate());
    }

    /* Initialize the metainfo to the new branch. Note we use hard durability, to avoid
    the potential for subtle bugs. */
    write_token_t write_token;
    store->new_write_token(&write_token);
    store->set_metainfo(
        version_t(
            primary->get_branch_id(),
            primary->get_branch_birth_certificate().initial_timestamp),
        order_source.check_in("local_replica_t(write)"),
        &write_token,
        write_durability_t::HARD,
        interruptor);

    state_timestamp_t first_timestamp;
    guarantee(first_timestamp ==
        primary->get_branch_birth_certificate().initial_timestamp);
}

local_replicator_t::~local_replicator_t() {
#if RDB_CF
    /* Since `local_replicator_t` is the primary dispatchee, changefeeds are always
    routed here. But when the primary changes we need to shut off the changefeed. This
    destructor is a good place to do it. */
    store->note_reshard();
#endif  // RDB_CF
}

