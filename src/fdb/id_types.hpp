#ifndef RETHINKDB_FDB_ID_TYPES_HPP_
#define RETHINKDB_FDB_ID_TYPES_HPP_

#include "containers/uuid.hpp"
#include "rpc/semilattice/joins/macros.hpp"
#include "rpc/serialize_macros.hpp"

// TODO: Many fdb id types (any used in a key) could be a small integer instead of a
// large uuid.  Saves key prefix length.

struct fdb_job_id {
    uuid_u value;
};
RDB_MAKE_SERIALIZABLE_1(fdb_job_id, value);
RDB_MAKE_EQUALITY_COMPARABLE_1(fdb_job_id, value);

struct fdb_shared_task_id {
    uuid_u value;
};
RDB_MAKE_SERIALIZABLE_1(fdb_shared_task_id, value);
RDB_MAKE_EQUALITY_COMPARABLE_1(fdb_shared_task_id, value);

struct sindex_id_t {
    uuid_u value;
};
RDB_MAKE_SERIALIZABLE_1(sindex_id_t, value);
RDB_MAKE_EQUALITY_COMPARABLE_1(sindex_id_t, value);

// Not an id type, so maybe this file is misnamed.  Well, maybe it identifies the
// version.
struct reqlfdb_config_version {
    uint64_t value;
};
RDB_MAKE_SERIALIZABLE_1(reqlfdb_config_version, value);

class config_version_checker {
public:
    uint64_t value;
    static config_version_checker empty() {
        return { UINT64_MAX };
    }
    bool is_empty() const {
        return value == UINT64_MAX;
    }
    reqlfdb_config_version assert_nonempty() const {
        rassert(!is_empty());
        return {value};
    }
};

#endif  // RETHINKDB_FDB_ID_TYPES_HPP_
