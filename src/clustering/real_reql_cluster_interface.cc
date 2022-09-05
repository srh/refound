// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/real_reql_cluster_interface.hpp"

#include "errors.hpp"
#include <boost/variant.hpp>

#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/tables/table_config.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "containers/archive/string_stream.hpp"
#include "rdb_protocol/artificial_table/artificial_table.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/terms/write_hook.hpp"
#include "rdb_protocol/val.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"

// TODO: Move elsewhere.
admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name) {
    return admin_err_t{
        strprintf("Table `%s.%s` already exists.", db_name.c_str(), table_name.c_str()),
        query_state_t::FAILED
    };
}

namespace real_reql_cluster_interface {

// Defined below.
scoped<ql::val_t> make_single_selection(
    artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
    auth::user_context_t const &user_context,
    const name_string_t &table_name,
    ql::backtrace_id_t bt,
    ql::env_t *env,
    std::function<uuid_u(FDBTransaction *)> cfg_checker);

scoped<ql::val_t> make_db_config_selection(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
        auth::user_context_t const &user_context,
        const provisional_db_id &db,
        ql::backtrace_id_t backtrace_id,
        ql::env_t *env) {
    return real_reql_cluster_interface::make_single_selection(
            artificial_reql_cluster_interface,
            user_context,
            name_string_t::guarantee_valid("db_config"),
            backtrace_id,
            env,
            [&](FDBTransaction *txn) -> uuid_u {
                // NNN: This throws, so we need to catch it and put that into *error_out.  Likewise with make_table_config_selection.
                database_id_t db_id = expect_retrieve_db(txn, db, env->interruptor);
                auth::fdb_user_fut<auth::db_config_permission> auth_fut
                    = user_context.transaction_require_db_config_permission(
                        txn,
                        db_id);
                auth_fut.block_and_check(env->interruptor);
                return db_id.value;
            });
}

// TODO: Remove sharding UI.

scoped<ql::val_t> make_table_config_selection(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
        auth::user_context_t const &user_context,
        const provisional_table_id &table,
        ql::backtrace_id_t bt,
        ql::env_t *env) {
    return make_single_selection(
            artificial_reql_cluster_interface,
            user_context,
            name_string_t::guarantee_valid("table_config"),
            bt,
            env,
            [&](FDBTransaction *txn) -> uuid_u {
                config_info<std::pair<namespace_id_t, table_config_t>> info
                    = expect_retrieve_table(txn, table, env->interruptor);

                auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
                    = user_context.transaction_require_db_and_table_config_permission(
                        txn,
                        info.ci_value.second.basic.database,
                        info.ci_value.first);
                auth_fut.block_and_check(env->interruptor);
                return info.ci_value.first.value;
            });
}

// "True" return means success, "false" means no such table, admin_err_t means we got such an error.
scoped<ql::val_t> make_single_selection(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
        auth::user_context_t const &user_context,
        const name_string_t &table_name,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        std::function<uuid_u(FDBTransaction *)> cfg_checker) {
    artificial_table_fdb_backend_t *table_backend =
        artificial_reql_cluster_interface->get_table_backend_or_null(
            table_name,
            admin_identifier_format_t::name);
    guarantee(table_backend != nullptr, "real_reql_cluster_interface::make_single_selection missing backend");
    counted_t<const ql::db_t> db = make_counted<ql::db_t>(artificial_reql_cluster_interface_t::database_id, artificial_reql_cluster_interface_t::database_name, config_version_checker::empty());

    // TODO: Do we really need to read the row up-front?

    // TODO: Verify that we didn't check read permissions on the system table pre-fdb.

    ql::datum_t row;
    fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
            [&](FDBTransaction *txn) {
        fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
        uuid_u primary_key = cfg_checker(txn);
        admin_err_t error;
        ql::datum_t tmp_row;
        if (!table_backend->read_row(
                txn,
                user_context,
                convert_uuid_to_datum(primary_key),
                env->interruptor,
                &tmp_row,
                &error)) {
            REQL_RETHROW_SRC(bt, error);
        } else if (!tmp_row.has()) {
            // This shouldn't happen because we table_backend->read_row will be looking
            // for the same primary key we just read from the database.
            //
            r_sanity_check(false, "Item `%s` missed in `%s` table lookup",
                uuid_to_str(primary_key).c_str(), table_name.c_str());
        }
        row = std::move(tmp_row);
    });
    rcheck_fdb_src(bt, loop_err, "making single selection");

    counted_t<ql::table_t> table = make_counted<ql::table_t>(
        make_counted<artificial_table_fdb_t>(table_backend),
        db,
        table_name,
        read_mode_t::SINGLE,
        bt);

    return make_scoped<ql::val_t>(
        ql::single_selection_t::from_row(table, row),
        bt);
}


}  // namespace real_reql_cluster_interface
