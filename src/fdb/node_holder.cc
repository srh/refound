#include "fdb/node_holder.hpp"

#include "arch/runtime/coroutines.hpp"
#include "arch/timing.hpp"
#include "clustering/main/serve.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/jobs.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"
#include "logger.hpp"
#include "utils.hpp"

fdb_error_t read_node_count(FDBDatabase *fdb, const signal_t *interruptor, uint64_t *out) {
    fdb_error_t err = txn_retry_loop_coro(fdb, interruptor, [interruptor, out](FDBTransaction *txn) {
        fdb_future nodes_count_fut = transaction_get_c_str(txn, REQLFDB_NODES_COUNT_KEY);
        nodes_count_fut.block_coro(interruptor);
        fdb_value nodes_count;
        fdb_error_t err = future_get_value(nodes_count_fut.fut, &nodes_count);
        check_for_fdb_transaction(err);
        guarantee(nodes_count.present);
        guarantee(nodes_count.length == REQLFDB_NODES_COUNT_SIZE);   // TODO deal with problem gracefully?
        static_assert(REQLFDB_NODES_COUNT_SIZE == 8, "Expecting size 8 for uint64");
        *out = read_LE_uint64(nodes_count.data);
    });

    return err;
}


void write_body(FDBTransaction *txn, fdb_node_id node_id, const signal_t *interruptor) {
    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<node_info> old_node_fut = transaction_lookup_uq_index<node_info_by_id>(txn, node_id);

    reqlfdb_clock clock = clock_fut.block_and_deserialize(interruptor);
    node_info old_node_value;
    bool old_node_present = old_node_fut.block_and_deserialize(interruptor, &old_node_value);

    node_info info;
    info.lease_expiration = reqlfdb_clock{clock.value + REQLFDB_NODE_LEASE_DURATION};

    if (old_node_present && info.lease_expiration == old_node_value.lease_expiration) {
        // No change.
        return;
    }

    transaction_set_uq_index<node_info_by_id>(txn, node_id, info);

    if (old_node_present) {
        transaction_erase_plain_index<node_info_by_lease_expiration>(txn, old_node_value.lease_expiration, node_id);
    }

    transaction_set_plain_index<node_info_by_lease_expiration>(txn, info.lease_expiration, node_id, "");

    if (!old_node_present) {
        uint8_t value[REQLFDB_NODES_COUNT_SIZE] = { 1, 0 };
        fdb_transaction_atomic_op(txn,
            as_uint8(REQLFDB_NODES_COUNT_KEY),
            strlen(REQLFDB_NODES_COUNT_KEY),
            value,
            sizeof(value),
            FDB_MUTATION_TYPE_ADD);
    }

    commit(txn, interruptor);
}

MUST_USE fdb_error_t write_node_entry(
        FDBDatabase *fdb, fdb_node_id node_id, const signal_t *interruptor) {
    for (;;) {
        signal_timer_t timer(REQLFDB_CONNECTIVITY_COMPLAINT_TIMEOUT_MS);
        wait_any_t waiter(&timer, interruptor);
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(fdb, &waiter,
                [&node_id, &waiter](FDBTransaction *txn) {
                write_body(txn, node_id, &waiter);
            });
            if (op_indeterminate(loop_err)) {
                // Just try again.
                continue;
            }
            return loop_err;
        } catch (const interrupted_exc_t &ex) {
            if (interruptor->is_pulsed()) {
                throw;
            }
            if (timer.is_pulsed()) {
                logERR("Node registration timed out.  Is FoundationDB running?  Is the config correct?  "
                       "Retrying...\n");
            }
            continue;
        }
    }
}

void help_erase_node_entry(FDBTransaction *txn, const signal_t *interruptor, const fdb_node_id &node_id) {
    fdb_value_fut<node_info> old_node_fut = transaction_lookup_uq_index<node_info_by_id>(txn, node_id);
    node_info old_node_value;
    bool old_node_present = old_node_fut.block_and_deserialize(interruptor, &old_node_value);

    transaction_erase_uq_index<node_info_by_id>(txn, node_id);
    if (old_node_present) {
        static_assert(8 == REQLFDB_NODES_COUNT_SIZE, "array initializer must match array size");
        uint8_t value[REQLFDB_NODES_COUNT_SIZE] = {
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        };
        fdb_transaction_atomic_op(txn,
            as_uint8(REQLFDB_NODES_COUNT_KEY),
            strlen(REQLFDB_NODES_COUNT_KEY),
            value,
            sizeof(value),
            FDB_MUTATION_TYPE_ADD);

        transaction_erase_plain_index<node_info_by_lease_expiration>(txn, old_node_value.lease_expiration, node_id);
    }
}

MUST_USE fdb_error_t erase_node_entry(
        FDBDatabase *fdb, const fdb_node_id &node_id, const signal_t *interruptor) {
 top:
    fdb_error_t err = txn_retry_loop_coro(fdb, interruptor, [&node_id, interruptor](FDBTransaction *txn) {
        help_erase_node_entry(txn, interruptor, node_id);

        commit(txn, interruptor);
    });
    if (op_indeterminate(err)) {
        // The operation is trivially idempotent
        goto top;
    }
    return err;
}

MUST_USE fdb_error_t incrementally_gc_node_info(FDBDatabase *fdb, const signal_t *interruptor) {
    const std::string lower = REQLFDB_NODES_BY_LEASE_EXPIRATION;
    const std::string upper = prefix_end(lower);
    fdb_error_t err = txn_retry_loop_coro(fdb, interruptor, [&lower, &upper, interruptor](FDBTransaction *txn) {
        fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
        fdb_future fut{fdb_transaction_get_range(
                txn,
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(lower.data()), lower.size()),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper.data()), upper.size()),
                0,
                0,
                FDB_STREAMING_MODE_SMALL,
                0,
                0,
                0)};

        fut.block_coro(interruptor);
        const FDBKeyValue *kvs;
        int kv_count;
        fdb_bool_t more;
        fdb_error_t err = fdb_future_get_keyvalue_array(fut.fut, &kvs, &kv_count, &more);
        check_for_fdb_transaction(err);

        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);

        for (int i = 0; i < kv_count; ++i) {
            key_view full_key{void_as_uint8(kvs[i].key), kvs[i].key_length};
            key_view key = full_key.guarantee_without_prefix(lower);
            std::pair<reqlfdb_clock, fdb_node_id> parsed = node_info_by_lease_expiration::parse_skey(key);
            if (is_node_expired(current_clock, parsed.first)) {
                help_erase_node_entry(txn, interruptor, parsed.second);
            } else {
                break;
            }
        }

        commit(txn, interruptor);
    });
    if (op_indeterminate(err)) {
        // Don't even retry, return success.
        return 0;
    }
    return err;
}


void fdb_node_holder::run_node_coro(auto_drainer_t::lock_t lock) {
    // This doesn't exponentially backoff -- the retry loops are supposed to do actual
    // exponential backoff.
    const int64_t error_nap_value = REQLFDB_TIMESTEP_MS;

    const signal_t *const interruptor = lock.get_drain_signal();
    for (;;) {
        // Read node count.
        uint64_t node_count;
        {
            fdb_error_t err = read_node_count(fdb_, interruptor, &node_count);
            if (err != 0) {
                logERR("Node presence routine encountered FoundationDB error: %s\n", fdb_get_error(err));
                nap(error_nap_value, interruptor);
                continue;
            }
        }

        // Now we've got a node count.  Now what?
        for (uint64_t i = 0; i < node_count; ++i) {
            // TODO: Avoid having one node take _all_ the jobs (somehow).
            try_claim_and_start_job(fdb_, node_id_, lock);

            // This block is written such that we don't exit the block until the timer
            // has expired or interruptor has been pulsed.
            {
                // TODO: Should we randomize this timestep?  Yes.
                signal_timer_t timer(REQLFDB_TIMESTEP_MS);
                for (;;) {
                    new_semaphore_in_line_t sem_acq(&supplied_job_sem_, 1);
                    const signal_t *sem_signal = sem_acq.acquisition_signal();

                    wait_any(&timer, sem_signal, interruptor);
                    if (interruptor->is_pulsed()) {
                        throw interrupted_exc_t();
                    }
                    if (timer.is_pulsed()) {
                        break;
                    }
                    rassert(sem_signal->is_pulsed());

                    std::vector<fdb_job_info> job_infos;
                    {
                        ASSERT_NO_CORO_WAITING;
                        rassert(!supplied_jobs_.empty());
                        job_infos = std::move(supplied_jobs_);
                        supplied_jobs_.clear();  // Don't trust std::move (on principle).
                        supplied_job_sem_holder_.transfer_in(std::move(sem_acq));
                    }

                    try_start_supplied_jobs(fdb_, std::move(job_infos), lock);
                }
            }

            fdb_error_t write_err = write_node_entry(fdb_, node_id_, interruptor);
            if (write_err != 0) {
                logERR("Node presence registration encountered FoundationDB error: %s\n", fdb_get_error(write_err));
                nap(error_nap_value, interruptor);
                continue;
            }
        }

        fdb_error_t loop_err = txn_retry_loop_coro(fdb_, interruptor, [interruptor](FDBTransaction *txn) {
            // TODO: Put a unit test somewhere that reqlfdb_clock_t is serialized as an
            // 8-byte little-endian uint64.
            uint8_t value[REQLFDB_CLOCK_SIZE] = { 1, 0 };
            fdb_transaction_atomic_op(txn,
                as_uint8(REQLFDB_CLOCK_KEY),
                strlen(REQLFDB_CLOCK_KEY),
                value,
                sizeof(value),
                FDB_MUTATION_TYPE_ADD);
            commit(txn, interruptor);
        });

        if (loop_err != 0) {
            logERR("Node presence clock update operation encountered FoundationDB error: %s\n", fdb_get_error(loop_err));
            nap(error_nap_value, interruptor);
            continue;
        }

        fdb_error_t gc_err = incrementally_gc_node_info(fdb_, interruptor);
        if (gc_err != 0) {
            logERR("Node garbage collection operation encountered FoundationDB error: %s\n", fdb_get_error(gc_err));
            nap(error_nap_value, interruptor);
            continue;

        }

        // TODO: Maybe we should monitor the node count as we loop ^^.
    }
}

void fdb_node_holder::supply_job(fdb_job_info job) {
    assert_thread();

    // We're careful to assert (and read the code and ensure) that these updates happen
    // atomically (due to cooperative multithreading).  If the coro blocked or got
    // interrupted before supply_job_sem_holder_.change_count(0) was called, it would be
    // possible that the values got moved out of supplied_jobs_ and
    // supplied_job_sem_holder_ got set back to 1, before we set it down to 0.  And then
    // it would be out of sync.
    ASSERT_NO_CORO_WAITING;
    supplied_jobs_.push_back(std::move(job));
    supplied_job_sem_holder_.change_count(0);  // Possibly already released.
}

fdb_node_holder::fdb_node_holder(FDBDatabase *fdb, const signal_t *interruptor)
        : fdb_(fdb), node_id_{generate_uuid()},
          supplied_job_sem_(1),
          supplied_job_sem_holder_(&supplied_job_sem_, 1) {
    fdb_error_t err = write_node_entry(fdb, node_id_, interruptor);
    if (err != 0) {
        logERR("Initial node presence registration encountered FoundationDB error: %s", fdb_get_error(err));
        // TODO: This is a bit of a hack -- we just happen to know the exception handler
        // cleanly returns false.  How about a new exception type?
        throw startup_failed_exc_t();
    }


    coro_t::spawn_later_ordered([this, lock = drainer_.lock()]() {
        try {
            run_node_coro(lock);
        } catch (const interrupted_exc_t &) {
            // TODO: Do we handle other interrupted_exc_t's like this?
        }
    });
}

void fdb_node_holder::shutdown(const signal_t *interruptor) {
    guarantee(!initiated_shutdown_);
    initiated_shutdown_ = true;
    drainer_.drain();
    fdb_error_t err = erase_node_entry(fdb_, node_id_, interruptor);
    if (err != 0) {
        logERR("Node shutdown presence erasure encountered FoundationDB error: %s", fdb_get_error(err));
        // Report but ignore error.
    }
}

fdb_node_holder::~fdb_node_holder() {
    if (!initiated_shutdown_) {
        drainer_.drain();
    }
}

