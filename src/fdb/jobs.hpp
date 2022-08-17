#ifndef RETHINKDB_FDB_JOBS_HPP_
#define RETHINKDB_FDB_JOBS_HPP_

#include "errors.hpp"
#include <boost/variant.hpp>

#include "concurrency/auto_drainer.hpp"
#include "containers/name_string.hpp"
#include "containers/optional.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"
#include "fdb/reql_fdb.hpp"
#include "rpc/serialize_macros.hpp"
#include "rpc/equality_macros.hpp"

struct skey_string;
struct ukey_string;

enum class fdb_job_type {
    db_drop_job,
    index_create_job,
};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(fdb_job_type, int8_t,
    fdb_job_type::db_drop_job, fdb_job_type::index_create_job);

struct fdb_job_db_drop {
    database_id_t database_id;
    // Used only for displaying the job in the jobs table.
    name_string_t database_name;

    optional<std::string> last_table_name;

    static fdb_job_db_drop make(database_id_t db_id, const name_string_t &db_name) {
        return fdb_job_db_drop{db_id, db_name, r_nullopt};
    }
};
RDB_DECLARE_SERIALIZABLE(fdb_job_db_drop);

struct fdb_job_index_create {
    namespace_id_t table_id;
    // Sindex creation/destruction holds the job's task id, so we could use that to look
    // up the sindex.  There is no strict need for sindex uuid here.
    sindex_id_t sindex_id;
};
RDB_DECLARE_SERIALIZABLE(fdb_job_index_create);


// TODO: Don't forget to turn this into a boost::variant (at some point), but who cares?
struct fdb_job_description {
    // We treat this like a tagged union... just because.
    fdb_job_type type;
    boost::variant<fdb_job_db_drop, fdb_job_index_create> v;
};

RDB_DECLARE_SERIALIZABLE(fdb_job_description);

struct fdb_job_info {
    fdb_job_id job_id;
    fdb_shared_task_id shared_task_id;
    fdb_node_id claiming_node_or_nil;  // Or the nil uuid, if unclaimed
    uint64_t counter;
    reqlfdb_clock lease_expiration;
    // Contains an error message. If non-empty, then lease_expiration is set to UINT64_MAX.
    optional<std::string> failed;
    fdb_job_description job_description;
};

RDB_DECLARE_SERIALIZABLE(fdb_job_info);
RDB_DECLARE_EQUALITY_COMPARABLE(fdb_job_info);

MUST_USE fdb_job_info add_fdb_job(FDBTransaction *txn,
    fdb_shared_task_id task_id, fdb_node_id claiming_node_or_nil,
    fdb_job_description &&desc, const signal_t *interruptor);

void remove_fdb_job(FDBTransaction *txn,
    const fdb_job_info &info);

void remove_fdb_task_and_jobs(FDBTransaction *txn, fdb_shared_task_id task_id,
    const signal_t *interruptor);

void try_claim_and_start_job(
    FDBDatabase *fdb, fdb_node_id self_node_id, const auto_drainer_t::lock_t &lock);

void try_start_supplied_jobs(
    FDBDatabase *fdb, std::vector<fdb_job_info> &&job_infos,
    const auto_drainer_t::lock_t &lock);

skey_string reqlfdb_clock_sindex_key(reqlfdb_clock clock);

// (Used by jobs table.)
optional<fdb_job_info> lookup_fdb_job(FDBTransaction *txn, fdb_job_id job_id,
    const signal_t *interruptor);
std::vector<fdb_job_info> lookup_all_fdb_jobs(FDBTransaction *txn,
    const signal_t *interruptor);

struct job_execution_result {
    // This result value has zero or one of these optionals non-empty.

    // A "reclaimed" job is one where we split the job into multiple txns (because small
    // txns are better) and we have more work to do (immediately) so we already have the
    // job claimed.
    optional<fdb_job_info> reclaimed;

    // A "failed" job has a message, and it needs its failed state to be written to the
    // db.  (For cleanliness, that is done in a fresh transaction.)
    optional<std::string> failed;
};

#endif  // RETHINKDB_FDB_JOBS_HPP_
