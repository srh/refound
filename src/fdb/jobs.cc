#include "fdb/jobs.hpp"

#include <inttypes.h>

#include "arch/runtime/coroutines.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/archive/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs/db_drop.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/jobs/job_utils.hpp"
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

RDB_IMPL_SERIALIZABLE_2_SINCE_v2_5(fdb_job_db_drop,
    database_id, last_table_name);
RDB_IMPL_EQUALITY_COMPARABLE_2(fdb_job_db_drop,
    database_id, last_table_name);
RDB_IMPL_SERIALIZABLE_3_SINCE_v2_5(fdb_job_index_create,
    table_id, sindex_name, sindex_id);
RDB_IMPL_EQUALITY_COMPARABLE_3(fdb_job_index_create,
    table_id, sindex_name, sindex_id);

RDB_IMPL_SERIALIZABLE_2_SINCE_v2_5(fdb_job_description, type, v);
RDB_IMPL_EQUALITY_COMPARABLE_2(fdb_job_description, type, v);

RDB_IMPL_SERIALIZABLE_6_SINCE_v2_5(fdb_job_info,
    job_id, shared_task_id, claiming_node_or_nil, counter, lease_expiration,
    job_description);

RDB_IMPL_EQUALITY_COMPARABLE_6(fdb_job_info,
    job_id, shared_task_id, claiming_node_or_nil, counter, lease_expiration,
    job_description);

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

skey_string job_by_task_skey(fdb_shared_task_id task) {
    return uuid_sindex_key(task.value);
}

job_sindex_keys get_sindex_keys(const fdb_job_info &info) {
    job_sindex_keys ret;
    ret.task = job_by_task_skey(info.shared_task_id);
    ret.lease_expiration = reqlfdb_clock_sindex_key(info.lease_expiration);
    return ret;
}

// TODO: Maybe caller can pass in clock.
fdb_job_info add_fdb_job(FDBTransaction *txn,
        fdb_shared_task_id shared_task_id, fdb_node_id claiming_node_or_nil,
        fdb_job_description &&desc, const signal_t *interruptor) {
    const fdb_job_id job_id{generate_uuid()};
    ukey_string job_id_key = jobs_by_id::ukey_str(job_id);

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<fdb_job_info> job_missing_fut = transaction_lookup_uq_index<jobs_by_id>(
        txn, job_id);

    reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);
    fdb_value job_missing_value = future_block_on_value(job_missing_fut.fut, interruptor);
    guarantee(!job_missing_value.present, "uuid generation created a duplicate id "
        "(rng failure), job with id %s already exists", job_id_key.ukey.c_str());

    reqlfdb_clock lease_expiration{
        claiming_node_or_nil.value.is_nil() ? 0
            : current_clock.value + REQLFDB_JOB_LEASE_DURATION};

    fdb_job_info info = {
        job_id,
        shared_task_id,
        claiming_node_or_nil,
        0,  // counter
        lease_expiration,
        std::move(desc),
    };

    job_sindex_keys sindex_keys = get_sindex_keys(info);

    // TODO: Now insert the job into the table.  We already confirmed it's an insertion.
    transaction_set_uq_index<jobs_by_id>(txn, job_id, info);
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        sindex_keys.lease_expiration, job_id_key, "");
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_TASK,
        sindex_keys.task, job_id_key, "");

    return info;
}

// The caller must know the job is present in the table.
void remove_fdb_job(FDBTransaction *txn, const fdb_job_info &info) {
    ukey_string job_id_key = jobs_by_id::ukey_str(info.job_id);
    job_sindex_keys sindex_keys = get_sindex_keys(info);

    transaction_erase_uq_index<jobs_by_id>(txn, info.job_id);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        sindex_keys.lease_expiration, job_id_key);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_TASK,
        sindex_keys.task, job_id_key);
}

// TODO: Look at all fdb txn's, note the read-only ones, and config them read-only.

// TODO: Distribute job execution to different cores.

void execute_job(FDBDatabase *fdb, const fdb_job_info &info,
        const auto_drainer_t::lock_t &lock) {
    const signal_t *interruptor = lock.get_drain_signal();

    optional<fdb_job_info> reclaimed{info};
    while (reclaimed.has_value()) {
        switch (info.job_description.type) {
        case fdb_job_type::db_drop_job: {
            fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&info, interruptor, &reclaimed](FDBTransaction *txn) {
                reclaimed = execute_db_drop_job(txn, info, boost::get<fdb_job_db_drop>(info.job_description.v), interruptor);
            });
            guarantee_fdb_TODO(loop_err, "could not execute db drop job");
        } break;
        case fdb_job_type::index_create_job: {
            fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&info, interruptor, &reclaimed](FDBTransaction *txn) {
                reclaimed = execute_index_create_job(txn, info,
                    boost::get<fdb_job_index_create>(info.job_description.v),
                    interruptor);
            });
            guarantee_fdb_TODO(loop_err, "could not execute index create job");
        } break;
        default:
            unreachable();
        }
    }
}

void try_claim_and_start_job(
        FDBDatabase *fdb, fdb_node_id self_node_id, const auto_drainer_t::lock_t &lock) {
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
            = first_fdb_key.guarantee_without_prefix(lease_index);

        std::pair<reqlfdb_clock, key_view> sp = split_clock_key_pair(spkey);
        // TODO: validate that sp.second is a uuid?  Or at least the right length.

        ukey_string job_id_key{std::string(as_char(sp.second.data), size_t(sp.second.length))};
        fdb_value_fut<fdb_job_info> job_info_fut =
            transaction_lookup_uq_index_raw<jobs_by_id>(txn, job_id_key);

        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);

        if (current_clock.value < sp.first.value) {
            // The lease hasn't expired; we do not claim any job.
            return;
        }

        // The lease has expired!  Claim the job.
        fdb_job_info old_job_info = job_info_fut.block_and_deserialize(interruptor);
        fdb_job_info job_info = old_job_info;

        job_info.claiming_node_or_nil = self_node_id;
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

void remove_fdb_task_and_jobs(FDBTransaction *txn, fdb_shared_task_id task_id,
        const signal_t *interruptor) {
    // TODO: There are two round-trips in this function -- maybe callers could clean it up.

    std::string prefix = plain_index_skey_prefix(REQLFDB_JOBS_BY_TASK, job_by_task_skey(task_id));

    // Now we have to actually look up each job and find its lease expiration so we can
    // properly remove it from all indexes.
    std::vector<fdb_value_fut<fdb_job_info>> job_futs;
    transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
            [&](const FDBKeyValue &kv) {
        key_view whole_key{void_as_uint8(kv.key), kv.key_length};
        key_view key = whole_key.guarantee_without_prefix(prefix);

        // We know the key is a UUID, fwiw; we don't even bother to parse it.
        ukey_string job_id_key{std::string(as_char(key.data), size_t(key.length))};
        job_futs.push_back(transaction_lookup_uq_index_raw<jobs_by_id>(txn, job_id_key));

        return true;
    });

    for (fdb_value_fut<fdb_job_info> &job_fut : job_futs) {
        fdb_job_info job_info = job_fut.block_and_deserialize(interruptor);
        remove_fdb_job(txn, job_info);
    }
}

optional<fdb_job_info> lookup_fdb_job(FDBTransaction *txn, fdb_job_id job_id,
        const signal_t *interruptor) {
    optional<fdb_job_info> ret;
    fdb_value_fut<fdb_job_info> fut = transaction_lookup_uq_index<jobs_by_id>(txn, job_id);
    ret.emplace();
    bool present = fut.block_and_deserialize(interruptor, &*ret);
    if (!present) {
        ret.reset();
    }
    return ret;
}

std::vector<fdb_job_info> lookup_all_fdb_jobs(FDBTransaction *txn, const signal_t *interruptor) {
    std::vector<fdb_job_info> ret;
    transaction_read_whole_range_coro(
        txn, jobs_by_id::prefix, prefix_end(jobs_by_id::prefix),
        interruptor, [&ret](const FDBKeyValue &kv) {

        ret.emplace_back();
        deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &ret.back());
        return true;
    });
    return ret;
}
