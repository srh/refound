// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_
#define CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_

#include <set>
#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "containers/scoped.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"

namespace auth {
class user_context_t;
}

class artificial_reql_cluster_interface_t;
class artificial_table_backend_t;
class config_version_checker;
template <class T> class counted_t;
class provisional_db_id;
class provisional_table_id;
namespace ql {
class backtrace_id_t;
class db_t;
class env_t;
class val_t;
}

// Some helper functions for real tables, only.
namespace real_reql_cluster_interface {

scoped<ql::val_t> make_db_config_selection(
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
    auth::user_context_t const &user_context,
    const provisional_db_id &db,
    ql::backtrace_id_t bt,
    ql::env_t *env);

scoped<ql::val_t> make_table_config_selection(
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
    auth::user_context_t const &user_context,
    const provisional_table_id &table,
    ql::backtrace_id_t bt,
    ql::env_t *env);
}  // namespace real_reql_cluster_interface

admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name);

#endif /* CLUSTERING_ADMINISTRATION_REAL_REQL_CLUSTER_INTERFACE_HPP_ */

