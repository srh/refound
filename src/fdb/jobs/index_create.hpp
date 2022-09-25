#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/archive/optional.hpp"
#include "containers/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_index_jobstate {
    // [unindexed_lower_bound, unindexed_upper_bound) describes the content
    // of the pkey store that has not been indexed.

    // Either the empty string or a last-seen pkey with '\0' on the end.  TODO: Might be '\1' now, see increment1.
    ukey_string unindexed_lower_bound;

    // This is the last pkey in the store, with '\0' added on the end.  TODO: Might be '\1' now, see increment1.
    // If r_nullopt, is +infinity.
    optional<ukey_string> unindexed_upper_bound;

    // TODO: Verify FDB limits on value length are at least twice that of key length.
};
RDB_MAKE_SERIALIZABLE_2(fdb_index_jobstate, unindexed_lower_bound, unindexed_upper_bound);

struct index_jobstate_by_task {
    using ukey_type = fdb_shared_task_id;
    using value_type = fdb_index_jobstate;
    static constexpr const char *prefix = REQLFDB_INDEX_JOBSTATE_BY_TASK;
    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }
};

struct index_create_retry_state {
    uint64_t retry_count = 0;
    optional<size_t> last_bytes_read;
    optional<size_t> last_key_count;
};

MUST_USE job_execution_result execute_index_create_job(
        const signal_t *interruptor,
        FDBTransaction *txn,
        const fdb_job_info &info,
        const fdb_job_index_create &index_create_info,
        index_create_retry_state *retry_state);


#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
