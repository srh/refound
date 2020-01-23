#ifndef RETHINKDB_FDB_JOBS_HPP_
#define RETHINKDB_FDB_JOBS_HPP_

#include "concurrency/auto_drainer.hpp"
#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"
#include "rpc/serialize_macros.hpp"
#include "rpc/semilattice/joins/macros.hpp"

enum class fdb_job_type {
    dummy_job,  // TODO: Remove.
    db_drop_job,
};

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(fdb_job_type, int8_t,
    fdb_job_type::dummy_job, fdb_job_type::dummy_job);

struct fdb_job_db_drop {
    database_id_t database_id;

    std::string min_table_name;

    static fdb_job_db_drop make(database_id_t db_id) {
        return fdb_job_db_drop{db_id, ""};
    }
};


struct fdb_job_description {
    fdb_job_type type;
    fdb_job_db_drop db_drop;
};

RDB_DECLARE_SERIALIZABLE(fdb_job_description);

struct fdb_job_info {
    uuid_u job_id;
    uuid_u shared_task_id;
    uuid_u claiming_node;  // Or the nil uuid, if unclaimed
    uint64_t counter;
    reqlfdb_clock lease_expiration;
    fdb_job_description job_description;
};

RDB_DECLARE_SERIALIZABLE(fdb_job_info);
RDB_DECLARE_EQUALITY_COMPARABLE(fdb_job_info);

MUST_USE fdb_job_info add_fdb_job(FDBTransaction *txn,
    uuid_u task_id, uuid_u claiming_node, fdb_job_description &&desc,
    const signal_t *interruptor);

void remove_fdb_job(FDBTransaction *txn,
    const fdb_job_info &info);

void try_claim_and_start_job(
    FDBDatabase *fdb, uuid_u self_node_id, const auto_drainer_t::lock_t &lock);

#endif  // RETHINKDB_FDB_JOBS_HPP_
