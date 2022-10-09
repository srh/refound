#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/archive/optional.hpp"
#include "containers/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_index_jobstate {
    /* ["", unindexed_upper_bound) describes the content of the pkey store that has not
       been indexed.

       claimed_bound, if it has a value, means that the interval [*claimed_bound,
       unindexed_upper_bound) is "prohibited" to writes.

       There is a loophole -- the write transaction may proceed if it includes in its
       transaction some changes to move sindex building forward such that sindex building
       completion is guaranteed.  The write transaction is then responsible for updating
       this jobstate value.

       If a write includes keys k1, ..., kN that intersect the prohibited interval, let
       `k` be the maximum such key.  Then the write may proceed if in the same
       transaction, it either

         (a) updates the sindex for at least all keys in [k, unindexed_upper_bound), or

         (b) updates the sindex for at least twice as many keys as it writes in the
         [*claimed_bound, unindexed_upper_bound) interval.

       In updating the sindex, it would also update the jobstate value's
       unindexed_upper_bound field, while leaving claimed_bound untouched as long as
       *claimed_bound is less than unindexed_upper_bound.

       The following invariant is maintained:

           !claimed_bound.has_value() ||
           !unindexed_upper_bound.has_value() ||
           *claimed_bound < *unindexed_upper_bound
    */

    optional<ukey_string> claimed_bound;

    // This is the open upper bound of the range of unindexed pkeys.  r_nullopt means
    // +infinity.
    optional<ukey_string> unindexed_upper_bound;

    // TODO: Verify FDB limits on value length are at least twice that of key length.
};
RDB_MAKE_SERIALIZABLE_2(fdb_index_jobstate,
    claimed_bound, unindexed_upper_bound);

struct index_jobstate_by_task {
    using ukey_type = fdb_shared_task_id;
    using value_type = fdb_index_jobstate;
    static constexpr const char *prefix = REQLFDB_INDEX_JOBSTATE_BY_TASK;
    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }
};

MUST_USE fdb_error_t execute_index_create_job(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        const fdb_job_info &info,
        const fdb_job_index_create &index_create_info,
        job_execution_result *result_out);



#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
