#include "fdb/jobs.hpp"

#include <inttypes.h>

#include "containers/archive/string_stream.hpp"
#include "fdb/index.hpp"
#include "fdb/typed.hpp"
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

RDB_IMPL_SERIALIZABLE_0_SINCE_v2_5(fdb_job_description);

RDB_IMPL_SERIALIZABLE_6_SINCE_v2_5(fdb_job_info,
    job_id, shared_task_id, claiming_node, counter, lease_expiration, job_description);

// TODO: Think about reusing datum sindex key format.
std::string reqlfdb_clock_sindex_key(reqlfdb_clock clock) {
    // Just an easy fixed-width big-endian key.
    return strprintf("%016" PRIx64 "", clock.value);
}

void add_fdb_job(FDBTransaction *txn,
    uuid_u shared_task_id, uuid_u claiming_node /* or nil */, fdb_job_description &&desc, const signal_t *interruptor) {
    const uuid_u job_id = generate_uuid();
    std::string job_id_key = uuid_to_str(job_id);

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_future job_missing_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_JOBS_BY_ID, job_id_key);

    reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);
    fdb_value job_missing_value = future_block_on_value(job_missing_fut.fut, interruptor);
    guarantee(!job_missing_value.present, "uuid generation created a duplicate id, "
        "job with id %s already exists", job_id_key.c_str());

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

    std::string task_sindex_key = uuid_sindex_key(shared_task_id);

    std::string expiration_sindex_key = reqlfdb_clock_sindex_key(lease_expiration);

    // TODO: Now insert the job into the table.  We already confirmed it's an insertion.
    transaction_set_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key, job_value);

    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        expiration_sindex_key, job_id_key, "");

    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_TASK,
        task_sindex_key, job_id_key, "");

    // Done.
}
