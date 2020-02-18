// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_
#define CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_

#include <set>
#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "fdb/fdb.hpp"
#include "rdb_protocol/context.hpp"

class artificial_reql_cluster_interface_t;
class artificial_table_backend_t;

/* `real_reql_cluster_interface_t` is a concrete subclass of `reql_cluster_interface_t`
that translates the user's `table_create()`, `table_drop()`, etc. requests into specific
actions on the semilattices. By performing these actions through the abstract
`reql_cluster_interface_t`, we can keep the ReQL code separate from the semilattice code.
*/

class real_reql_cluster_interface_t :
    public reql_cluster_interface_t,
    public home_thread_mixin_t {
public:
    real_reql_cluster_interface_t();

    // These functions are now accessible as static functions.

    static bool make_db_config_selection(
            artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
            auth::user_context_t const &user_context,
            const counted_t<const ql::db_t> &db,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out);

    bool db_config(
            auth::user_context_t const &user_context,
            const counted_t<const ql::db_t> &db,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) override;

    static bool make_table_config_selection(
            artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
            auth::user_context_t const &user_context,
            counted_t<const ql::db_t> db,
            config_version_checker cv_checker,
            const namespace_id_t &table_id,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
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

    /* This is public because it needs to be set after we're created to solve a certain
    chicken-and-egg problem */
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface = nullptr;

private:

    static void make_single_selection(
            artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
            auth::user_context_t const &user_context,
            const name_string_t &table_name,
            config_version_checker cv_checker,
            const uuid_u &primary_key,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            std::function<void(FDBTransaction *)> cfg_checker,  // OOO: Hideous!
            scoped_ptr_t<ql::val_t> *selection_out)
            THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, admin_op_exc_t);

    DISABLE_COPYING(real_reql_cluster_interface_t);
};

admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name);

#endif /* CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_ */

