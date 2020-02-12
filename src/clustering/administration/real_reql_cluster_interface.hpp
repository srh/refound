// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_
#define CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_

#include <set>
#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/namespace_interface_repository.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/watchable.hpp"
#include "fdb/fdb.hpp"
#include "rdb_protocol/context.hpp"
#include "rpc/semilattice/view.hpp"

class artificial_reql_cluster_interface_t;
class artificial_table_backend_t;
template <class T> class lifetime_t;
class name_resolver_t;
class server_config_client_t;

/* `real_reql_cluster_interface_t` is a concrete subclass of `reql_cluster_interface_t`
that translates the user's `table_create()`, `table_drop()`, etc. requests into specific
actions on the semilattices. By performing these actions through the abstract
`reql_cluster_interface_t`, we can keep the ReQL code separate from the semilattice code.
*/

class real_reql_cluster_interface_t :
    public reql_cluster_interface_t,
    public home_thread_mixin_t {
public:
    real_reql_cluster_interface_t(
            FDBDatabase *fdb,
            mailbox_manager_t *mailbox_manager,
            std::shared_ptr<semilattice_readwrite_view_t<
                auth_semilattice_metadata_t> > auth_semilattice_view,
            std::shared_ptr<semilattice_readwrite_view_t<
                cluster_semilattice_metadata_t> > cluster_semilattice_view,
            rdb_context_t *rdb_context,
            server_config_client_t *server_config_client,
            table_meta_client_t *table_meta_client,
            multi_table_manager_t *multi_table_manager,
            watchable_map_t<
                std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
                table_query_bcard_t> *table_query_directory,
            lifetime_t<name_resolver_t const &> name_resolver);

    bool db_drop_uuid(
            auth::user_context_t const &user_context,
            database_id_t database_id,
            const name_string_t &name,
            const signal_t *interruptor_on_home,
            ql::datum_t *result_out,
            admin_err_t *error_out);
    bool db_config(
            auth::user_context_t const &user_context,
            const counted_t<const ql::db_t> &db,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;

    bool table_config(
            auth::user_context_t const &user_context,
            counted_t<const ql::db_t> db,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;
    bool table_status(
            counted_t<const ql::db_t> db,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;

#if RDB_CF
    ql::changefeed::client_t *get_changefeed_client() override {
        return &m_changefeed_client;
    }
#endif  // RDB_CF
    // TODO: Remove this, presumably.
    namespace_repo_t *get_namespace_repo() {
        return &m_namespace_repo;
    }

    /* This is public because it needs to be set after we're created to solve a certain
    chicken-and-egg problem */
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface;

private:
    FDBDatabase *m_fdb;
    mailbox_manager_t *m_mailbox_manager;
    std::shared_ptr<semilattice_readwrite_view_t<
        auth_semilattice_metadata_t> > m_auth_semilattice_view;
    std::shared_ptr<semilattice_readwrite_view_t<
        cluster_semilattice_metadata_t> > m_cluster_semilattice_view;
    table_meta_client_t *m_table_meta_client;
    scoped_array_t< scoped_ptr_t< cross_thread_watchable_variable_t<
        databases_semilattice_metadata_t > > > m_cross_thread_database_watchables;
    rdb_context_t *m_rdb_context;

    namespace_repo_t m_namespace_repo;
#if RDB_CF
    ql::changefeed::client_t m_changefeed_client;
#endif
    server_config_client_t *m_server_config_client;

    void wait_for_cluster_metadata_to_propagate(
            const cluster_semilattice_metadata_t &metadata,
            const signal_t *interruptor);

    // This could soooo be optimized if you don't want to copy the whole thing.
    void get_databases_metadata(databases_semilattice_metadata_t *out);

    void make_single_selection(
            auth::user_context_t const &user_context,
            const name_string_t &table_name,
            const uuid_u &primary_key,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out)
            THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, admin_op_exc_t);

    void wait_internal(
            std::set<namespace_id_t> tables,
            table_readiness_t readiness,
            const signal_t *interruptor,
            ql::datum_t *result_out,
            int *count_out)
            THROWS_ONLY(interrupted_exc_t, admin_op_exc_t);

    DISABLE_COPYING(real_reql_cluster_interface_t);
};

admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name);

#endif /* CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_ */

