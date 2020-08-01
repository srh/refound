#include "fdb/node_holder.hpp"

#include "arch/runtime/coroutines.hpp"
#include "arch/timing.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/jobs.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
#include "utils.hpp"

std::string node_key(const fdb_node_id &node_id) {
    std::string key = REQLFDB_NODES_BY_ID;
    uuid_onto_str(node_id.value, &key);  // TODO: use binary uuid?
    return key;
}
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
    std::string key = node_key(node_id);

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_future old_node_fut = transaction_get_std_str(txn, key);

    reqlfdb_clock clock = clock_fut.block_and_deserialize(interruptor);
    fdb_value old_node = future_block_on_value(old_node_fut.fut, interruptor);

    {
        reqlfdb_clock lease_expiration{clock.value + REQLFDB_NODE_LEASE_DURATION};
        std::string buf = serialize_for_cluster_to_string(lease_expiration);

        fdb_transaction_set(txn,
            as_uint8(key.data()),
            int(key.size()),
            as_uint8(buf.data()),
            int(buf.size()));
    }

    if (!old_node.present) {
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
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&node_id, interruptor](FDBTransaction *txn) {
        write_body(txn, node_id, interruptor);
    });
    return loop_err;
}

MUST_USE fdb_error_t erase_node_entry(
        FDBDatabase *fdb, fdb_node_id node_id, const signal_t *interruptor) {
    return txn_retry_loop_coro(fdb, interruptor, [&node_id, interruptor](FDBTransaction *txn) {
        std::string key = node_key(node_id);

        fdb_future old_node_fut = transaction_get_std_str(txn, key);
        fdb_value old_node = future_block_on_value(old_node_fut.fut, interruptor);

        fdb_transaction_clear(txn, as_uint8(key.data()), int(key.size()));
        if (old_node.present) {
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
        }

        commit(txn, interruptor);
    });
}

void fdb_node_holder::run_node_coro(auto_drainer_t::lock_t lock) {
    const signal_t *const interruptor = lock.get_drain_signal();
    for (;;) {
        // Read node count.
        uint64_t node_count;
        {
            fdb_error_t err = read_node_count(fdb_, interruptor, &node_count);
            guarantee_fdb_TODO(err, "read_node_count");
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
            guarantee_fdb_TODO(write_err, "write_node_entry failed in loop");
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
        guarantee_fdb_TODO(loop_err, "clock update retry loop failed");
        // TODO: Maybe we should monitor the node count as we loop ^^.

        // TODO: We need to participate in garbage collection of expired nodes, too.
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
    guarantee_fdb_TODO(err, "write_node_entry failed");

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
    guarantee_fdb_TODO(err, "erase_node_entry failed");  // We'll want to report but ignore error.
}

fdb_node_holder::~fdb_node_holder() {
    if (!initiated_shutdown_) {
        drainer_.drain();
    }
}

