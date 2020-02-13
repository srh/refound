// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_TABLE_CONTRACT_EXECUTOR_EXEC_PRIMARY_HPP_
#define CLUSTERING_TABLE_CONTRACT_EXECUTOR_EXEC_PRIMARY_HPP_

#include "clustering/table_contract/executor/exec.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/new_mutex.hpp"
#include "containers/counted.hpp"
#include "containers/optional.hpp"

struct admin_err_t;
class contract_t;
class io_backender_t;
class primary_dispatcher_t;

// TODO: Remove?
/* `ack_counter_t` computes whether a write is guaranteed to be preserved
even after a failover or other reconfiguration. */
class ack_counter_t {
public:
    explicit ack_counter_t(const contract_t &_contract) :
        contract(_contract), primary_ack(false), voter_acks(0) { }
    void note_ack(const server_id_t &server);
    bool is_safe() const;
private:
    const contract_t &contract;
    bool primary_ack;
    size_t voter_acks;

    DISABLE_COPYING(ack_counter_t);
};

class primary_execution_t :
    public execution_t,
    public home_thread_mixin_t {
public:
    primary_execution_t(
        const execution_t::context_t *context,
        execution_t::params_t *params,
        const contract_id_t &cid,
        const table_raft_state_t &raft_state);
    ~primary_execution_t();

    void update_contract_or_raft_state(
        const contract_id_t &cid,
        const table_raft_state_t &raft_state) override;

private:
    class write_callback_t;
    /* `contract_info_t` stores a contract, its ack callback, and a condition variable
    indicating if it's obsolete. The reason this is in a struct is because we sometimes
    need to reason about old contracts, so we may keep multiple versions around. */
    class contract_info_t;

    /* This is started in a coroutine when the `primary_t` is created. It sets up the
    broadcaster, listener, etc. */
    void run(auto_drainer_t::lock_t keepalive);

    /* `update_contract_or_raft_state()` spawns `update_contract_on_store_thread()`
    to deliver the new contract to `store->home_thread()`. It has two jobs:
    1. It sets `latest_contract_store_thread` to the new contract
    2. If the broadcaster has been created, it waits until it's safe to ack
        `primary_ready`, and then does so. */
    void update_contract_on_store_thread(
        counted_t<contract_info_t> contract,
        auto_drainer_t::lock_t keepalive,
        new_mutex_in_line_t *mutex_in_line_ptr);

    /* `sync_contract_with_replicas()` blocks until it's safe to ack `primary_ready` for
    the given contract. It does this by sending a sync write to all of the replicas and
    waiting until enough of them respond; this relies on the principle that if a replica
    acknowledges the sync write, it must have also acknowledged every write initiated
    before the sync write. If the first sync write fails, it will try repeatedly until it
    succeeds or is interrupted. */
    void sync_contract_with_replicas(
        counted_t<contract_info_t> contract,
        const signal_t *interruptor);

    /* `is_contract_ackable()` is a helper function for `sync_contract_with_replicas()`
    that returns `true` if it's safe to ack `primary_ready` for the given contract, given
    that all the replicas in `servers` have performed the sync write. */
    static bool is_contract_ackable(
        counted_t<contract_info_t> contract,
        const std::set<server_id_t> &servers);

    optional<branch_id_t> our_branch_id;

    /* `latest_contract_*` stores the latest contract we've received, along with its ack
    callback. The `home_thread` version should only be accessed on `this->home_thread()`,
    and the `store_thread` version should only be accessed on `store->home_thread()`. */
    counted_t<contract_info_t> latest_contract_home_thread, latest_contract_store_thread;

    /* `latest_ack` stores the latest contract ack we've sent.  This is possibly null --
    treated as an optional<contract_ack_t>, with a pointer indirection for the sake of
    compile times. */
    scoped_ptr_t<contract_ack_t> latest_ack;

    /* `update_contract_mutex` is used to order calls to
    `update_contract_on_store_thread()`, so that we don't overwrite a newer contract with
    an older one */
    new_mutex_t update_contract_mutex;

    /* `branch_registered` is pulsed once the coordinator has committed our branch ID to
    the Raft state. */
    cond_t branch_registered;

    /* `our_dispatcher` stores the pointer to the `primary_dispatcher_t` we constructed.
    It will be `nullptr` until the `primary_dispatcher_t` actually exists and has a valid
    replica. */
    primary_dispatcher_t *our_dispatcher;

    /* Anything that might try to use `our_dispatcher` after the `primary_execution_t`
    destructor has begin should hold a lock on `*our_dispatcher_drainer`. (Specifically,
    this applies to `update_contract_on_store_thread()`.) `our_dispatcher` and
    `our_dispatcher_drainer` will always both be null or both be non-null. */
    auto_drainer_t *our_dispatcher_drainer;

    /* `begin_write_mutex_assertion` is used to ensure that we don't ack a contract until
    all past and ongoing writes are safe under the contract's conditions. */
    mutex_assertion_t begin_write_mutex_assertion;

    /* `drainer` ensures that `run()` and `update_contract_on_store_thread()` are
    stopped before the other member variables are destroyed. */
    auto_drainer_t drainer;
};

#endif /* CLUSTERING_TABLE_CONTRACT_EXECUTOR_EXEC_PRIMARY_HPP_ */

