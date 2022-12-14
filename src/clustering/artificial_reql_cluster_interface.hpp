// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_ARTIFICIAL_REQL_CLUSTER_INTERFACE_HPP_
#define CLUSTERING_ADMINISTRATION_ARTIFICIAL_REQL_CLUSTER_INTERFACE_HPP_

#include <map>
#include <set>
#include <string>

#include "containers/map_sentries.hpp"
#include "containers/name_string.hpp"
#include "containers/scoped.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"
#include "rdb_protocol/db.hpp"

namespace auth {
class permissions_artificial_table_fdb_backend_t;
class users_artificial_table_fdb_backend_t;
}
class base_table_t;
class db_config_artificial_table_fdb_backend_t;
class in_memory_artificial_table_fdb_backend_t;
class jobs_artificial_table_fdb_backend_t;
class server_config_artificial_table_fdb_backend_t;
class server_status_artificial_table_fdb_backend_t;
class stats_artificial_table_fdb_backend_t;
class table_config_artificial_table_fdb_backend_t;
class table_status_artificial_table_fdb_backend_t;
class artificial_reql_cluster_backends_t;

class artificial_reql_cluster_backends_t {
public:
    artificial_reql_cluster_backends_t();
    ~artificial_reql_cluster_backends_t();

    // Just for human sanity, are in alphabetical order by table name (with
    // "_debug_scratch" being underscore-prefixed).
    scoped<db_config_artificial_table_fdb_backend_t> db_config_backend;
    scoped<jobs_artificial_table_fdb_backend_t> jobs_backend[2];
    scoped<auth::permissions_artificial_table_fdb_backend_t>
        permissions_backend[2];
    scoped<server_config_artificial_table_fdb_backend_t> server_config_backend;
    scoped<server_status_artificial_table_fdb_backend_t> server_status_backend[2];
    scoped<stats_artificial_table_fdb_backend_t> stats_backend[2];
    scoped<table_config_artificial_table_fdb_backend_t> table_config_backend[2];
    scoped<table_status_artificial_table_fdb_backend_t> table_status_backend[2];
    scoped<auth::users_artificial_table_fdb_backend_t> users_backend;
    scoped<in_memory_artificial_table_fdb_backend_t> debug_scratch_backend;

    /* QQQ: Reimplement these backends:

    - _debug_stats - from the debug_stats_artificial_table_backend_t - request that
      servers dump their perfmon stats and whatnot to some FDB location.  (They could
      set up a watch for such requests.)

    */

    DISABLE_COPYING(artificial_reql_cluster_backends_t);
};

class artificial_reql_cluster_interface_t {
public:
    static const database_id_t database_id;
    static const name_string_t database_name;

    artificial_reql_cluster_interface_t() = default;

    static std::vector<name_string_t> table_list_sorted();
    MUST_USE bool table_find(const name_string_t &name,
            admin_identifier_format_t identifier_format,
            counted_t<const base_table_t> *table_out);

    artificial_table_fdb_backend_t *
    get_table_backend_or_null(
            name_string_t const &,
            admin_identifier_format_t) const;

    static optional<namespace_id_t> get_table_id(const name_string_t &table_name);

    static bool check_by_uuid_table_exists(const namespace_id_t &table_id);

private:
    artificial_reql_cluster_backends_t m_backends;
};

// TODO: Move to appropriate file
admin_err_t db_already_exists_error(const name_string_t &db_name);

#endif /* CLUSTERING_ADMINISTRATION_ARTIFICIAL_REQL_CLUSTER_INTERFACE_HPP_ */

