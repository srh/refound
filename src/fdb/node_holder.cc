#include "fdb/node_holder.hpp"

#include "arch/runtime/coroutines.hpp"
#include "arch/timing.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/jobs.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
#include "utils.hpp"

std::string node_key(const uuid_u &node_id) {
    std::string key = REQLFDB_NODES_TABLE;
    key += '/';
    uuid_onto_str(node_id, &key);  // TODO: use binary uuid?
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


void write_body(FDBTransaction *txn, uuid_u node_id, const signal_t *interruptor) {
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
        FDBDatabase *fdb, uuid_u node_id, const signal_t *interruptor) {
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&node_id, interruptor](FDBTransaction *txn) {
        write_body(txn, node_id, interruptor);
    });
    return loop_err;
}

MUST_USE fdb_error_t erase_node_entry(FDBDatabase *fdb, uuid_u node_id, const signal_t *interruptor) {
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

void run_node_coro(FDBDatabase *fdb, uuid_u node_id, auto_drainer_t::lock_t lock) {
    const signal_t *const interruptor = lock.get_drain_signal();
    for (;;) {
        // Read node count.
        uint64_t node_count;
        {
            fdb_error_t err = read_node_count(fdb, interruptor, &node_count);
            guarantee_fdb_TODO(err, "read_node_count");
        }

        // Now we've got a node count.  Now what?
        for (uint64_t i = 0; i < node_count; ++i) {
            // TODO: Avoid having one node take _all_ the jobs (somehow).
            try_claim_and_start_job(fdb, node_id, lock);



            // TODO: Should we randomize this?  Yes.
            nap(REQLFDB_TIMESTEP_MS, interruptor);
            fdb_error_t write_err = write_node_entry(fdb, node_id, interruptor);
            guarantee_fdb_TODO(write_err, "write_node_entry failed in loop");
        }

        fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [interruptor](FDBTransaction *txn) {
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

fdb_node_holder::fdb_node_holder(FDBDatabase *fdb, const signal_t *interruptor)
        : fdb_(fdb), node_id_(generate_uuid()) {
    fdb_error_t err = write_node_entry(fdb, node_id_, interruptor);
    guarantee_fdb_TODO(err, "write_node_entry failed");

    coro_t::spawn_later_ordered([this, lock = drainer_.lock()]() {
        try {
            run_node_coro(fdb_, node_id_, lock);
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

