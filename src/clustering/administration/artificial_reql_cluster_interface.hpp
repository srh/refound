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

namespace auth {
class permissions_artificial_table_fdb_backend_t;
class users_artificial_table_fdb_backend_t;
}
class base_table_t;
class db_config_artificial_table_fdb_backend_t;
class in_memory_artificial_table_fdb_backend_t;
class jobs_artificial_table_fdb_backend_t;
class table_config_artificial_table_fdb_backend_t;

/* This is now just some helper methods for the system db; it holds a map of pointers to
backends.  The code evolved to this point; it's just a stateless object, and the
backends map could be replaced with a hard-coded switch. */

class artificial_reql_cluster_interface_t
    : public home_thread_mixin_t {
public:
    static const database_id_t database_id;
    static const name_string_t database_name;

    artificial_reql_cluster_interface_t();

    std::vector<name_string_t> table_list_sorted();
    MUST_USE bool table_find(const name_string_t &name,
            admin_identifier_format_t identifier_format,
            counted_t<base_table_t> *table_out,
            admin_err_t *error_out);

    artificial_table_fdb_backend_t *
    get_table_backend_or_null(
            name_string_t const &,
            admin_identifier_format_t) const;

    using table_fdb_backends_map_t = std::map<
        name_string_t,
        std::pair<artificial_table_fdb_backend_t *, artificial_table_fdb_backend_t *>>;

    table_fdb_backends_map_t *get_table_fdb_backends_map_mutable();
    table_fdb_backends_map_t const &get_table_fdb_backends_map() const;

private:
    table_fdb_backends_map_t m_table_fdb_backends;
};

class artificial_reql_cluster_backends_t {
public:
    explicit artificial_reql_cluster_backends_t(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface);
    ~artificial_reql_cluster_backends_t();

private:
    using fdb_backend_sentry_t = map_insertion_sentry_t<
        artificial_reql_cluster_interface_t::table_fdb_backends_map_t::key_type,
        artificial_reql_cluster_interface_t::table_fdb_backends_map_t::mapped_type>;

    scoped_ptr_t<auth::permissions_artificial_table_fdb_backend_t>
        permissions_backend[2];
    fdb_backend_sentry_t permissions_sentry;

    scoped_ptr_t<auth::users_artificial_table_fdb_backend_t> users_backend;
    fdb_backend_sentry_t users_sentry;

    scoped_ptr_t<db_config_artificial_table_fdb_backend_t> db_config_backend;
    fdb_backend_sentry_t db_config_sentry;

    scoped_ptr_t<table_config_artificial_table_fdb_backend_t> table_config_backend[2];
    fdb_backend_sentry_t table_config_sentry;

    scoped_ptr_t<jobs_artificial_table_fdb_backend_t> jobs_backend[2];
    fdb_backend_sentry_t jobs_sentry;

    scoped_ptr_t<in_memory_artificial_table_fdb_backend_t> debug_scratch_backend;
    fdb_backend_sentry_t debug_scratch_sentry;

    /* QQQ: Reimplement these backends:

    - server status - we could report info about what nodes are registered, and they
      could include basic config information in their report.

    - _debug_stats - from the debug_stats_artificial_table_backend_t - request that
      servers dump their perfmon stats and whatnot to some FDB location.  (They could
      set up a watch for such requests.)

    - stats - from the stats_artificial_table_backend_t.  As with _debug_stats.

    */


    DISABLE_COPYING(artificial_reql_cluster_backends_t);
};

// TODO: Move to appropriate file
admin_err_t db_already_exists_error(const name_string_t &db_name);

#endif /* CLUSTERING_ADMINISTRATION_ARTIFICIAL_REQL_CLUSTER_INTERFACE_HPP_ */

