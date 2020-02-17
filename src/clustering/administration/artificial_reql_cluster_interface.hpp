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
#include "rdb_protocol/context.hpp"

namespace auth {
class permissions_artificial_table_fdb_backend_t;
class users_artificial_table_fdb_backend_t;
}
class base_table_t;
class db_config_artificial_table_fdb_backend_t;
class in_memory_artificial_table_fdb_backend_t;
class jobs_artificial_table_fdb_backend_t;
class table_config_artificial_table_fdb_backend_t;

/* The `artificial_reql_cluster_interface_t` is responsible for handling queries to the
`rethinkdb` database. It's implemented as a proxy over the
`real_reql_cluster_interface_t`; queries go first to the `artificial_...`, and if they
aren't related to the `rethinkdb` database, they get passed on to the `real_...`. */

class artificial_reql_cluster_interface_t
    : public reql_cluster_interface_t,
      public home_thread_mixin_t {
public:
    static const database_id_t database_id;
    static const name_string_t database_name;

    artificial_reql_cluster_interface_t();

    bool db_config(
            auth::user_context_t const &user_context,
            const counted_t<const ql::db_t> &db,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;

    std::vector<name_string_t> table_list_sorted();
    MUST_USE bool table_find(const name_string_t &name,
            admin_identifier_format_t identifier_format,
            counted_t<base_table_t> *table_out,
            admin_err_t *error_out);
    bool table_config(
            auth::user_context_t const &user_context,
            counted_t<const ql::db_t> db,
            config_version_checker cv_checker,
            const namespace_id_t &table_id,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;

    void set_next_reql_cluster_interface(reql_cluster_interface_t *next);

    artificial_table_fdb_backend_t *
    get_table_backend_or_null(
            name_string_t const &,
            admin_identifier_format_t) const;

    using table_fdb_backends_map_t = std::map<
        name_string_t,
        std::pair<artificial_table_fdb_backend_t *, artificial_table_fdb_backend_t *>>;

    table_fdb_backends_map_t *get_table_fdb_backends_map_mutable();
    table_fdb_backends_map_t const &get_table_fdb_backends_map() const;

#if RDB_CF
    ql::changefeed::client_t *get_changefeed_client() override {
        guarantee(m_next != nullptr);
        return m_next->get_changefeed_client();
    }
#endif  // RDB_CF

private:
    bool next_or_error(admin_err_t *error_out) const;

    table_fdb_backends_map_t m_table_fdb_backends;
    reql_cluster_interface_t *m_next;
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

