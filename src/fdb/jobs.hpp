#ifndef RETHINKDB_FDB_JOBS_HPP_
#define RETHINKDB_FDB_JOBS_HPP_

#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_job_description {

};

struct fdb_job_info {
    uuid_u job_id;
    uuid_u shared_task_id;
    uuid_u claiming_node;  // Or the nil uuid, if unclaimed
    uint64_t counter;
    reqlfdb_clock lease_expiration;
    fdb_job_description job_description;
};

RDB_DECLARE_SERIALIZABLE(fdb_job_info);

void add_fdb_job(FDBTransaction *txn,
    uuid_u task_id, uuid_u claiming_node, fdb_job_description &&desc);



#endif  // RETHINKDB_FDB_JOBS_HPP_
