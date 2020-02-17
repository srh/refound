// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/real_reql_cluster_interface.hpp"

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/grant.hpp"
#include "clustering/administration/auth/user_context.hpp"
#include "clustering/administration/auth/user_fut.hpp"
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/tables/table_config.hpp"
#include "concurrency/cross_thread_signal.hpp"
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
#include "rpc/semilattice/watchable.hpp"
#include "rpc/semilattice/view/field.hpp"

real_reql_cluster_interface_t::real_reql_cluster_interface_t()
#if RDB_CF
    : m_changefeed_client()
#endif
{
}

bool real_reql_cluster_interface_t::db_config(
        auth::user_context_t const &user_context,
        const counted_t<const ql::db_t> &db,
        ql::backtrace_id_t backtrace_id,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize this function (for writing?  for making a single section?  fdb-ize artificial table?)
    try {
        make_single_selection(
            user_context,
            name_string_t::guarantee_valid("db_config"),
            db->cv,
            db->id.value,
            backtrace_id,
            env,
            [&](FDBTransaction *txn) {
                auth::fdb_user_fut<auth::db_config_permission> auth_fut = user_context.transaction_require_db_config_permission(txn, db->id);
                auth_fut.block_and_check(env->interruptor);
            },
            selection_out);
        return true;
    } catch (const no_such_table_exc_t &) {
        *error_out = admin_err_t{
            strprintf("Database `%s` does not exist.", db->name.c_str()),
            query_state_t::FAILED};
        return false;
    } catch (const admin_op_exc_t &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    }
}

// TODO: Move elsewhere.
admin_err_t table_already_exists_error(
    const name_string_t &db_name, const name_string_t &table_name) {
    return admin_err_t{
        strprintf("Table `%s.%s` already exists.", db_name.c_str(), table_name.c_str()),
        query_state_t::FAILED
    };
}

// TODO: Remove sharding UI.

bool real_reql_cluster_interface_t::table_config(
        auth::user_context_t const &user_context,
        counted_t<const ql::db_t> db,
        config_version_checker cv_checker,
        const namespace_id_t &table_id,
        const name_string_t &name,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize this or the single selection function.
    try {
        // OOO: We do need a cv check to ensure that the table id we used isn't wildly ou of date.  Then we construct a single selection on the config table.  That can't carry its own cv checker, can it?  Or can it?

        // QQQ: No more name errors to catch, probblay.

        make_single_selection(
            user_context,
            name_string_t::guarantee_valid("table_config"),
            cv_checker,
            table_id.value,
            bt,
            env,
            [&](FDBTransaction *txn) {
                auth::fdb_user_fut<auth::db_table_config_permission> auth_fut = user_context.transaction_require_db_and_table_config_permission(txn, db->id, table_id);
                auth_fut.block_and_check(env->interruptor);
            },
            selection_out);
        return true;
    } catch (const admin_op_exc_t &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    } CATCH_NAME_ERRORS(db->name, name, error_out)
}


// TODO: fdb-ize functions (for writing) below.
// TODO: fdb-ize this, somehow.  (Caller passes in txn and uses it to mutate, probably.)
void real_reql_cluster_interface_t::make_single_selection(
        auth::user_context_t const &user_context,
        const name_string_t &table_name,
        config_version_checker cv_checker,
        const uuid_u &primary_key,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        std::function<void(FDBTransaction *)> cfg_checker,  // OOO: Hideous!
        scoped_ptr_t<ql::val_t> *selection_out)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, admin_op_exc_t) {
    artificial_table_fdb_backend_t *table_backend =
        artificial_reql_cluster_interface->get_table_backend_or_null(
            table_name,
            admin_identifier_format_t::name);
    guarantee(table_backend != nullptr, "real_reql_cluster_interface_t::make_single_selection missing backend");
    counted_t<const ql::db_t> db = make_counted<ql::db_t>(artificial_reql_cluster_interface_t::database_id, artificial_reql_cluster_interface_t::database_name, config_version_checker::empty());

    // TODO: Do we really need to read the row up-front?

    // TODO: Verify that we didn't check read permissions on the system table pre-fdb.

    ql::datum_t row;
    fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
            [&](FDBTransaction *txn) {
        fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
        reqlfdb_config_version cv = cv_fut.block_and_deserialize(env->interruptor);
        check_cv(cv_checker, cv);
        cfg_checker(txn);
        admin_err_t error;
        ql::datum_t tmp_row;
        if (!table_backend->read_row(
                txn,
                user_context,
                convert_uuid_to_datum(primary_key),
                env->interruptor,
                &tmp_row,
                &error)) {
            throw admin_op_exc_t(error);
        } else if (!row.has()) {
            /* This is unlikely, but it can happen if the object is deleted between when we
            look up its name and when we call `read_row()` */
            // TODO: Ensure callers catch this.  Is this even a legit exception?  It should be a no such row exception, or something like that...  Maybe real_reql_cluster_interface_t means this to refer to the r.table() param?
            //
            // This shouldn't happen because we call check_cv, and
            // table_backend->read_row will thus be reading off the same cv we had.
            //
            // QQQ: Maybe we should have a guarantee here.
            throw no_such_table_exc_t();
        }
        row = std::move(tmp_row);
    });
    guarantee_fdb_TODO(loop_err, "real_reql_cluster_interface_t::make_single_selection retry loop");

    counted_t<ql::table_t> table = make_counted<ql::table_t>(
        make_counted<artificial_table_fdb_t>(table_backend),
        db,
        table_name,
        read_mode_t::SINGLE,
        bt);

    *selection_out = make_scoped<ql::val_t>(
        ql::single_selection_t::from_row(bt, table, row),
        bt);
}
