// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/executor/exec_primary.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/immediate_consistency/local_replicator.hpp"
#include "clustering/immediate_consistency/primary_dispatcher.hpp"
#include "clustering/table_contract/contract_metadata.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/promise.hpp"
#include "rpc/mailbox/mailbox.hpp"
#include "store_view.hpp"

void ack_counter_t::note_ack(const server_id_t &server) {
    if (true) {
        primary_ack |= (server == contract.the_server);
    }
    voter_acks += contract.the_server == server ? 1 : 0;
}

bool ack_counter_t::is_safe() const {
    return primary_ack &&
        voter_acks * 2 > 1 /* contract.voters.size() */;
}

class primary_execution_t::contract_info_t
    : public slow_atomic_countable_t<primary_execution_t::contract_info_t> {
public:
    contract_info_t(const contract_id_t &_contract_id,
                    const contract_t &_contract) :
            contract_id(_contract_id),
            contract(_contract) {
    }
    bool equivalent(const contract_info_t &other) const {
        /* This method is called `equivalent` rather than `operator==` to avoid
        confusion, because it doesn't actually compare every member */
        return contract_id == other.contract_id;
    }
    contract_id_t contract_id;
    contract_t contract;
    cond_t obsolete;
};

primary_execution_t::primary_execution_t(
        const execution_t::context_t *_context,
        execution_t::params_t *_params,
        const contract_id_t &contract_id,
        const table_raft_state_t &raft_state) :
    execution_t(_context, _params), our_dispatcher(nullptr)
{
    const contract_t &contract = raft_state.contracts.at(contract_id);
    guarantee(contract.the_server == context->server_id);
    latest_contract_home_thread = make_counted<contract_info_t>(
        contract_id, contract);
    latest_contract_store_thread = latest_contract_home_thread;
    begin_write_mutex_assertion.rethread(store->home_thread());
    coro_t::spawn_sometime(std::bind(&primary_execution_t::run, this, drainer.lock()));
}

primary_execution_t::~primary_execution_t() {
    drainer.drain();
    begin_write_mutex_assertion.rethread(home_thread());
}

void primary_execution_t::update_contract_or_raft_state(
        const contract_id_t &contract_id,
        const table_raft_state_t &raft_state) {
    assert_thread();
    ASSERT_NO_CORO_WAITING;

    /* Has our branch ID been registered yet? */
    if (!branch_registered.is_pulsed()) {
        bool branch_matches = true;
        raft_state.current_branches.visit(region_t::universe(),
        [&](const region_t &, const branch_id_t &b) {
            if (make_optional(b) != our_branch_id) {
                branch_matches = false;
            }
        });
        if (branch_matches) {
            rassert(!branch_registered.is_pulsed());
            /* This raft state update just confirmed our branch ID */
            branch_registered.pulse();
            /* Change `latest_ack` immediately so we don't keep sending the branch
            registration request */
            latest_ack = make_scoped<contract_ack_t>(
                contract_ack_t::state_t::primary_in_progress);
            params->send_ack(contract_id, *latest_ack);
        }
    }

    const contract_t &contract = raft_state.contracts.at(contract_id);
    guarantee(contract.the_server == context->server_id);

    counted_t<contract_info_t> new_contract = make_counted<contract_info_t>(
        contract_id,
        contract);

    /* Exit early if there aren't actually any changes. This is for performance reasons.
    */
    if (new_contract->equivalent(*latest_contract_home_thread)) {
        return;
    }

    /* Mark the old contract as obsolete, and record the new one */
    latest_contract_home_thread->obsolete.pulse();
    latest_contract_home_thread = new_contract;

    /* If we were acking `primary_ready`, go back to acking `primary_in_progress` until
    we sync with the replicas according to the new contract. */
    if (latest_ack.has() &&
            latest_ack->state == contract_ack_t::state_t::primary_ready) {
        latest_ack = make_scoped<contract_ack_t>(
            contract_ack_t::state_t::primary_in_progress);
    }

    /* Deliver the new contract to the other thread. If the broadcaster exists, we'll
    also do sync with the replicas and ack `primary_in_ready`. */
    coro_t::spawn_sometime(std::bind(
        &primary_execution_t::update_contract_on_store_thread, this,
        latest_contract_home_thread,
        drainer.lock(),
        new new_mutex_in_line_t(&update_contract_mutex)));

    /* Send an ack for the new contract */
    if (latest_ack.has()) {
        params->send_ack(contract_id, *latest_ack);
    }
}

void primary_execution_t::run(auto_drainer_t::lock_t keepalive) {
    assert_thread();
    order_source_t order_source(store->home_thread());
    cross_thread_signal_t interruptor_store_thread(
        keepalive.get_drain_signal(), store->home_thread());

    try {
        /* Create the `primary_dispatcher_t`, establishing our branch in the process. */
        on_thread_t thread_switcher_1(store->home_thread());

        read_token_t token;
        store->new_read_token(&token);
        region_map_t<version_t> initial_version = store->get_metainfo(
            order_source.check_in("primary_t").with_read_mode(),
            &token, region_t::universe(), &interruptor_store_thread);

        primary_dispatcher_t primary_dispatcher(initial_version);

        on_thread_t thread_switcher_2(home_thread());

        /* Put an entry in the global directory so clients can find us for outdated reads */

        /* Send a request for the coordinator to register our branch */
        {
            our_branch_id = make_optional(primary_dispatcher.get_branch_id());
            contract_ack_t ack(contract_ack_t::state_t::primary_need_branch);
            ack.branch.set(*our_branch_id);
            context->branch_history_manager->export_branch_history(
                primary_dispatcher.get_branch_birth_certificate().origin,
                &ack.branch_history);
            ack.branch_history.branches.insert(std::make_pair(
                *our_branch_id,
                primary_dispatcher.get_branch_birth_certificate()));
            latest_ack = make_scoped<contract_ack_t>(ack);
            params->send_ack(latest_contract_home_thread->contract_id, ack);
        }

        /* Wait until we get our branch registered */
        wait_interruptible(&branch_registered, keepalive.get_drain_signal());

        /* Acquire the mutex so that instances of `update_contract_on_store_thread()`
        will be blocked out during the transitional period when we're setting up the
        local replica. At the same time as we acquire the mutex, make a local copy of
        `latest_contract_on_home_thread`. `sync_contract_with_replicas()` won't have
        happened for this contract because `our_dispatcher` was `nullptr` when
        `update_contract_on_store_thread()` ran for it. So we have to manually call
        `sync_contract_with_replicas()` for it. */
        counted_t<contract_info_t> pre_replica_contract = latest_contract_home_thread;
        scoped_ptr_t<new_mutex_in_line_t> pre_replica_mutex_in_line(
            new new_mutex_in_line_t(&update_contract_mutex));
        wait_interruptible(
            pre_replica_mutex_in_line->acq_signal(), keepalive.get_drain_signal());

        /* Set up the `local_replicator_t`, `remote_replicator_server_t`, and
        `primary_query_server_t`. */
        on_thread_t thread_switcher_3(store->home_thread());

        local_replicator_t local_replicator(
            &primary_dispatcher,
            store,
            context->branch_history_manager,
            &interruptor_store_thread);

        auto_drainer_t primary_dispatcher_drainer;
        assignment_sentry_t<auto_drainer_t *> our_dispatcher_drainer_assign(
            &our_dispatcher_drainer, &primary_dispatcher_drainer);
        assignment_sentry_t<primary_dispatcher_t *> our_dispatcher_assign(
            &our_dispatcher, &primary_dispatcher);

        on_thread_t thread_switcher_4(home_thread());

        /* OK, now we have to make sure that `sync_contract_with_replicas()` gets called
        for `pre_broadcaster_contract` by spawning an extra copy of
        `update_contract_on_store_thread()` manually. A copy was already spawned for the
        contract when it first came in, but that copy saw `our_broadcaster` as `nullptr`
        so it didn't call `sync_contract_with_replicas()`. */
        coro_t::spawn_sometime(std::bind(
            &primary_execution_t::update_contract_on_store_thread, this,
            pre_replica_contract,
            keepalive,
            pre_replica_mutex_in_line.release()));

        /* The `local_replicator_t` constructor set the metainfo to `*our_branch_id`, so
        it's now safe to call `enable_gc()`. */
        params->enable_gc(*our_branch_id);

        /* Put an entry in the minidir so the replicas can find us */
        contract_execution_bcard_t ce_bcard;
        ce_bcard.peer = context->mailbox_manager->get_me();
        watchable_map_var_t<std::pair<server_id_t, branch_id_t>,
            contract_execution_bcard_t>::entry_t minidir_entry(
                context->local_contract_execution_bcards,
                std::make_pair(context->server_id, *our_branch_id),
                ce_bcard);

        /* Put an entry in the global directory so clients can find us for up-to-date
        writes and reads */
        // TODO: Nope.  Not anymore

        /* Wait until we are no longer the primary, or it's time to shut down */
        keepalive.get_drain_signal()->wait_lazily_unordered();

    } catch (const interrupted_exc_t &) {
        /* do nothing */
    }
}


void primary_execution_t::update_contract_on_store_thread(
        counted_t<contract_info_t> contract,
        auto_drainer_t::lock_t keepalive,
        new_mutex_in_line_t *sync_mutex_in_line_ptr) {
    scoped_ptr_t<new_mutex_in_line_t> sync_mutex_in_line(sync_mutex_in_line_ptr);
    assert_thread();

    try {
        wait_any_t interruptor_home_thread(
            &contract->obsolete, keepalive.get_drain_signal());

        /* Wait until we hold `sync_mutex`, so that we don't reorder contracts if several
        new contracts come in quick succession */
        wait_interruptible(sync_mutex_in_line->acq_signal(), &interruptor_home_thread);

        bool should_ack;
        {
            /* Go to the store's thread */
            cross_thread_signal_t interruptor_store_thread(
                &interruptor_home_thread, store->home_thread());
            on_thread_t thread_switcher(store->home_thread());

            /* We can only send a `primary_ready` response to the contract coordinator
            once we're sure that both all incoming writes will be executed under the new
            contract, and all previous writes are safe under the conditions of the new
            contract. We ensure the former condition by setting
            `latest_contract_store_thread`, so that incoming writes will pick up the new
            contract. We ensure the latter condition by running sync writes until we get
            one to succeed under the new contract. The sync writes must enter the
            dispatcher's queue after any writes that were run under the old contract, so
            that the sync writes can't succeed under the new contract until the old
            writes are also safe under the new contract. */

            /* Deliver the latest contract */
            {
                /* We acquire `begin_write_mutex_assertion` to assert that we're not
                interleaving with a call to `on_write()`. This way, we can be sure that
                any writes that saw the old contract will enter the primary dispatcher's
                queue before we call `sync_contract_with_replicas()`. */
                mutex_assertion_t::acq_t begin_write_mutex_acq(
                    &begin_write_mutex_assertion);
                ASSERT_FINITE_CORO_WAITING;
                latest_contract_store_thread = contract;
            }

            /* If we have a broadcaster, then try to sync with replicas so we can ack the
            contract. */
            if (our_dispatcher != nullptr) {
                /* We're going to hold a pointer to the `primary_dispatcher_t` that lives
                on the stack in `run()`, so we need to make sure we'll stop before
                `run()` stops. We do this by acquiring a lock on the `auto_drainer_t` on
                the stack in `run()`. */
                auto_drainer_t::lock_t keepalive2 = our_dispatcher_drainer->lock();
                wait_any_t combiner(
                    &interruptor_store_thread, keepalive2.get_drain_signal());
                should_ack = true;
            } else {
                should_ack = false;
            }
        }

        if (should_ack) {
            /* OK, time to ack the contract */
            latest_ack = make_scoped<contract_ack_t>(contract_ack_t::state_t::primary_ready);
            params->send_ack(contract->contract_id, *latest_ack);
        }

    } catch (const interrupted_exc_t &) {
        /* Either the contract is obsolete or we are being destroyed. In either case,
        stop trying to ack the contract. */
    }
}

