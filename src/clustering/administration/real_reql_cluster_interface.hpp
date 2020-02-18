// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_
#define CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_

#include <set>
#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"

namespace auth {
class user_context_t;
}

class artificial_reql_cluster_interface_t;
class artificial_table_backend_t;
class config_version_checker;
template <class T> class counted_t;
template <class T> class scoped_ptr_t;
namespace ql {
class backtrace_id_t;
class db_t;
class env_t;
class val_t;
}

// Some helper functions for real tables, only.
namespace real_reql_cluster_interface {

bool make_db_config_selection(
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
    auth::user_context_t const &user_context,
    const counted_t<const ql::db_t> &db,
    ql::backtrace_id_t bt,
    ql::env_t *env,
    scoped_ptr_t<ql::val_t> *selection_out,
    admin_err_t *error_out);

bool make_table_config_selection(
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
}  // namespace real_reql_cluster_interface

admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name);

#endif /* CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_ */

