#include "fdb/jobs.hpp"

#include <inttypes.h>

#include "arch/runtime/coroutines.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/index.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
// TODO: I don't like this dependency order.
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "utils.hpp"

/*
This stores fdb "jobs".  A job is something that needs to get done.  Often it's a
cleanup task that can't get done as part of a single 5 second FDB transaction.  So
we leave it as a job that ourself or other nodes can pick up and operate on,
incrementally.

rethinkdb/jobs/ => table of fdb_job_info by job id
  indexed by lease expiration timestamp (see rethinkdb/clock)
  indexed by shared task id
  indexed by node?

Facts about jobs:

-1. You might say a "task" is carried out by one or more jobs, usually just one.

0. A subjob has an id (probably a uuid).

0.5. A subjob has a "shared task id" (probably a uuid).

1. A job contains a description specifying what needs to be done.

2. A job may be unclaimed, or it may be claimed by a node with a lease expiration
timestamp (a rethinkdb/clock logical timestamp).

3. A job may only be operated on by one node at a time.  (You might parallelize by
splitting it into smaller jobs, that share the same "shared task id".)

4. Every transaction that works on a job must also read the job and write the job in the
jobs table, so that it conflicts with anything else that might try to claim the job.  This might involve incrementing a counter and updating the lease expiration.

5. Some tasks can be cancelled (using the shared task id).  E.g. secondary index
creation, if the secondary index or table gets deleted.  You can cancel a task by
deleting all of its jobs.

More comments:

1. Since tasks can be cancelled, tasks are often referenced from other structures.  For
example, a table config will carry id's for active secondary index creation tasks.

2. Lease expiration is just a suggestion.  It would be wasteful to have multiple nodes
racing to complete a job.

3. There is a counter field because if the clock doesn't change, naive code might not
update the lease expiration or mutate the job, resulting in no conflict when we want
one.  Instead of a counter, we could simply destroy and recreate the job (with a new
job_id) every time we do an increment of work.  But that would be annoying in some job
monitoring system, so we have a counter.

Again:

rethinkdb/jobs/ => table of fdb_job_info by job id
  indexed by lease expiration timestamp (see rethinkdb/clock)
  indexed by shared task id
  indexed by node?


*/

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(fdb_job_description, type);
RDB_IMPL_EQUALITY_COMPARABLE_1(fdb_job_description, type);

RDB_IMPL_EQUALITY_COMPARABLE_6(fdb_job_info,
    job_id, shared_task_id, claiming_node, counter, lease_expiration, job_description);

RDB_IMPL_SERIALIZABLE_6_SINCE_v2_5(fdb_job_info,
    job_id, shared_task_id, claiming_node, counter, lease_expiration, job_description);

// TODO: Think about reusing datum sindex key format.
skey_string reqlfdb_clock_sindex_key(reqlfdb_clock clock) {
    // Just an easy fixed-width big-endian key.
    return skey_string{strprintf("%016" PRIx64 "", clock.value)};
}

std::pair<reqlfdb_clock, key_view> split_clock_key_pair(key_view key) {
    guarantee(key.length >= 16);  // TODO: fail msg

    // TODO: Avoid string construction
    std::string sindex_key{as_char(key.data), 16};
    reqlfdb_clock clock;
    bool res = strtou64_strict(sindex_key, 16, &clock.value);
    guarantee(res);  // TODO: fail msg
    return std::make_pair(clock, key_view{key.data + 16, key.length - 16});
}

struct job_sindex_keys {
    skey_string task;
    skey_string lease_expiration;
};

job_sindex_keys get_sindex_keys(const fdb_job_info &info) {
    job_sindex_keys ret;
    ret.task = uuid_sindex_key(info.shared_task_id);
    ret.lease_expiration = reqlfdb_clock_sindex_key(info.lease_expiration);
    return ret;
}

ukey_string job_id_str(uuid_u job_id) {
    return ukey_string{uuid_to_str(job_id)};
}

// TODO: Maybe caller can pass in clock.
fdb_job_info add_fdb_job(FDBTransaction *txn,
    uuid_u shared_task_id, uuid_u claiming_node /* or nil */, fdb_job_description &&desc, const signal_t *interruptor) {
    const uuid_u job_id = generate_uuid();
    ukey_string job_id_key = job_id_str(job_id);

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_future job_missing_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_JOBS_BY_ID, job_id_key);

    reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);
    fdb_value job_missing_value = future_block_on_value(job_missing_fut.fut, interruptor);
    guarantee(!job_missing_value.present, "uuid generation created a duplicate id "
        "(rng failure), job with id %s already exists", job_id_key.ukey.c_str());

    reqlfdb_clock lease_expiration{
        claiming_node.is_nil() ? 0 : current_clock.value + REQLFDB_JOB_LEASE_DURATION};

    fdb_job_info info = {
        job_id,
        shared_task_id,
        claiming_node,
        0,  // counter
        lease_expiration,
        std::move(desc),
    };

    std::string job_value = serialize_for_cluster_to_string(info);

    job_sindex_keys sindex_keys = get_sindex_keys(info);

    // TODO: Now insert the job into the table.  We already confirmed it's an insertion.
    transaction_set_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key, job_value);
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        sindex_keys.lease_expiration, job_id_key, "");
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_TASK,
        sindex_keys.task, job_id_key, "");

    return info;
}

// The caller must know the job is present in the table.
void remove_fdb_job(FDBTransaction *txn, const fdb_job_info &info) {
    ukey_string job_id_key = job_id_str(info.job_id);
    job_sindex_keys sindex_keys = get_sindex_keys(info);

    transaction_erase_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        sindex_keys.lease_expiration, job_id_key);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_TASK,
        sindex_keys.task, job_id_key);
}

fdb_value_fut<fdb_job_info> transaction_get_real_job_info(
        FDBTransaction *txn, const fdb_job_info &info) {
    ukey_string job_id_key = job_id_str(info.job_id);
    return fdb_value_fut<fdb_job_info>{
        transaction_lookup_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key)};
}

bool block_and_check_info(
        const fdb_job_info &expected_info,
        fdb_value_fut<fdb_job_info> &&real_info_fut,
        const signal_t *interruptor) {
    fdb_job_info real_info;
    if (!real_info_fut.block_and_deserialize(interruptor, &real_info)) {
        // The job is not present.  Something else must have claimed it.
        return false;
    }

    if (real_info.counter != expected_info.counter) {
        // The job is not the same.  Another node must have claimed it.
        // (Possibly ourselves?  Who knows.)
        return false;
    }

    // This is impossible, but it could happen in a scenario with duplicate
    // generate_uuid(), I guess.
    guarantee(real_info == expected_info,
        "Job info is different with the same counter value.");
    return true;
}

void replace_fdb_job(FDBTransaction *txn,
        const fdb_job_info &old_info, const fdb_job_info &new_info) {
    // Basic sanity check.
    guarantee(new_info.job_id == old_info.job_id);
    // Sanity check: we don't have to update the task index.
    guarantee(new_info.shared_task_id == old_info.shared_task_id);

    std::string new_job_info_str = serialize_for_cluster_to_string(new_info);
    skey_string old_lease_expiration_key = reqlfdb_clock_sindex_key(old_info.lease_expiration);
    skey_string new_lease_expiration_key = reqlfdb_clock_sindex_key(new_info.lease_expiration);

    ukey_string job_id_key = job_id_str(new_info.job_id);

    transaction_set_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key, new_job_info_str);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        old_lease_expiration_key, job_id_key);
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        old_lease_expiration_key, job_id_key, "");
    // Task index untouched.
}

// TODO: Look at all fdb txn's, note the read-only ones, and config them read-only.

// Returns new job info if we have re-claimed this job and want to execute it again.
MUST_USE optional<fdb_job_info> execute_db_drop_job(FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_db_drop &db_drop_info, const signal_t *interruptor) {
    // TODO: Maybe caller can pass clock.
    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<fdb_job_info> real_info_fut =
        transaction_get_real_job_info(txn, info);
    // We always make it a closed interval, because we deleted the last-used table name
    // anyway.
    fdb_future range_fut = transaction_get_table_range(
        txn, db_drop_info.database_id, db_drop_info.min_table_name, true,
        FDB_STREAMING_MODE_SMALL);

    if (!block_and_check_info(info, std::move(real_info_fut), interruptor)) {
        return r_nullopt;
    }

    range_fut.block_coro(interruptor);

    const FDBKeyValue *kv;
    int kv_count;
    fdb_bool_t more;
    fdb_error_t err = fdb_future_get_keyvalue_array(range_fut.fut, &kv, &kv_count, &more);
    check_for_fdb_transaction(err);

    std::string last_table;
    for (int i = 0; i < kv_count; ++i) {
        key_view key{void_as_uint8(kv[i].key), kv[i].key_length};
        std::string table_name
            = unserialize_table_by_name_table_name(key, db_drop_info.database_id);

        bool exists = help_remove_table_if_exists(
            txn, db_drop_info.database_id, table_name, interruptor);
        guarantee(exists, "Table was just seen to exist, now it doesn't.");

        last_table = table_name;
    }

    optional<fdb_job_info> ret;
    if (more) {
        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);

        fdb_job_info new_info = info;
        new_info.counter++;
        new_info.lease_expiration = reqlfdb_clock{current_clock.value + REQLFDB_JOB_LEASE_DURATION};

        replace_fdb_job(txn, info, new_info);

        ret.set(std::move(new_info));
    } else {
        remove_fdb_job(txn, info);
        ret = r_nullopt;
    }

    commit(txn, interruptor);
    return ret;
}

// TODO: Distribute job execution to different cores.
void execute_dummy_job(FDBTransaction *txn, const fdb_job_info &info,
        const signal_t *interruptor) {
    fdb_value_fut<fdb_job_info> real_info_fut =
        transaction_get_real_job_info(txn, info);
    if (!block_and_check_info(info, std::move(real_info_fut), interruptor)) {
        return;
    }

    // Since this is a dummy job, we do nothing with the job except to remove it.
    remove_fdb_job(txn, info);
    commit(txn, interruptor);
}

// Returns true if we have re-claimed the job and want to execute it again.
void execute_job(FDBDatabase *fdb, const fdb_job_info &info,
        const auto_drainer_t::lock_t &lock) {
    const signal_t *interruptor = lock.get_drain_signal();

    optional<fdb_job_info> reclaimed{info};
    while (reclaimed.has_value()) {
        switch (info.job_description.type) {
        case fdb_job_type::dummy_job: {
            fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&info, interruptor](FDBTransaction *txn) {
                execute_dummy_job(txn, info, interruptor);
            });
            guarantee_fdb_TODO(loop_err, "could not execute dummy job");
            reclaimed = r_nullopt;
        } break;
        case fdb_job_type::db_drop_job: {
            fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&info, interruptor, &reclaimed](FDBTransaction *txn) {
                reclaimed = execute_db_drop_job(txn, info, info.job_description.db_drop, interruptor);
            });
            guarantee_fdb_TODO(loop_err, "could not execute db drop job");
        } break;
        default:
            unreachable();
        }
    }
}

void try_claim_and_start_job(
        FDBDatabase *fdb, uuid_u self_node_id, const auto_drainer_t::lock_t &lock) {
    // TODO: Do we actually want a retry-loop?  Maybe a no-retry loop.
    const signal_t *interruptor = lock.get_drain_signal();
    optional<fdb_job_info> claimed_job;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
    [interruptor, self_node_id, &claimed_job](FDBTransaction *txn) {
        // Now.
        fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);

        const char *lease_index = REQLFDB_JOBS_BY_LEASE_EXPIRATION;
        fdb_key_fut first_key_fut{fdb_transaction_get_key(txn,
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(
                as_uint8(lease_index),
                strlen(lease_index)),
            false)};

        key_view first_fdb_key = first_key_fut.block_and_get_key(interruptor);

        // TODO: prefix_end string ctor perf.
        std::string lease_index_end = prefix_end(lease_index);

        if (sized_strcmp(first_fdb_key.data, first_fdb_key.length,
                as_uint8(lease_index_end.data()), lease_index_end.size()) >= 0) {
            // Jobs table is empty.
            return;
        }

        // There is at least one job.

        key_view spkey
            = first_fdb_key.without_prefix(strlen(REQLFDB_JOBS_BY_LEASE_EXPIRATION));

        std::pair<reqlfdb_clock, key_view> sp = split_clock_key_pair(spkey);
        // TODO: validate that sp.second is a uuid?  Or at least the right length.

        ukey_string job_id_key{std::string(as_char(sp.second.data), size_t(sp.second.length))};
        fdb_value_fut<fdb_job_info> job_info_fut{
            transaction_lookup_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key)};

        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);

        if (current_clock.value < sp.first.value) {
            // The lease hasn't expired; we do not claim any job.
            return;
        }

        // The lease has expired!  Claim the job.
        fdb_job_info old_job_info = job_info_fut.block_and_deserialize(interruptor);
        fdb_job_info job_info = old_job_info;

        job_info.claiming_node = self_node_id;
        job_info.counter++;
        job_info.lease_expiration = reqlfdb_clock{current_clock.value + REQLFDB_JOB_LEASE_DURATION};

        replace_fdb_job(txn, old_job_info, job_info);

        commit(txn, interruptor);
        claimed_job.set(std::move(job_info));
    });
    guarantee_fdb_TODO(loop_err, "try_claim_and_start_job failed");

    if (claimed_job.has_value()) {
        coro_t::spawn_later_ordered([fdb, job = std::move(claimed_job.get()), lock] {
            execute_job(fdb, job, lock);
        });
    }
}
