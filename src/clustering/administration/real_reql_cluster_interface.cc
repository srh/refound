// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/real_reql_cluster_interface.hpp"

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/grant.hpp"
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/tables/table_config.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
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
#include "rpc/semilattice/watchable.hpp"
#include "rpc/semilattice/view/field.hpp"

real_reql_cluster_interface_t::real_reql_cluster_interface_t(
        FDBDatabase *fdb,
        mailbox_manager_t *mailbox_manager,
        std::shared_ptr<semilattice_readwrite_view_t<
            auth_semilattice_metadata_t> > auth_semilattice_view,
        rdb_context_t *rdb_context,
        table_meta_client_t *table_meta_client) :
    m_fdb(fdb),
    m_mailbox_manager(mailbox_manager),
    m_auth_semilattice_view(auth_semilattice_view),
    m_table_meta_client(table_meta_client),
    m_rdb_context(rdb_context)
#if RDB_CF
    , m_changefeed_client(m_mailbox_manager)
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
        user_context.require_config_permission(m_rdb_context, db->id);

        make_single_selection(
            user_context,
            name_string_t::guarantee_valid("db_config"),
            db->id.value,
            backtrace_id,
            env,
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
        const name_string_t &name,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize this or the single selection function.
    try {
        namespace_id_t table_id;
        m_table_meta_client->find(db->id, name, &table_id);

        user_context.require_config_permission(m_rdb_context, db->id, table_id);

        make_single_selection(
            user_context,
            name_string_t::guarantee_valid("table_config"),
            table_id.value,
            bt,
            env,
            selection_out);
        return true;
    } catch (const admin_op_exc_t &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    } CATCH_NAME_ERRORS(db->name, name, error_out)
}

bool real_reql_cluster_interface_t::table_status(
        counted_t<const ql::db_t> db,
        const name_string_t &name,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize this or the single selection function.
    try {
        namespace_id_t table_id;
        m_table_meta_client->find(db->id, name, &table_id);
        make_single_selection(
            env->get_user_context(),
            name_string_t::guarantee_valid("table_status"),
            table_id.value,
            bt,
            env,
            selection_out);
        return true;
    } catch (const admin_op_exc_t &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    } CATCH_NAME_ERRORS(db->name, name, error_out)
}

/* Checks that divisor is indeed a divisor of multiple. */
template <class T>
bool is_joined(const T &multiple, const T &divisor) {
    T cpy = multiple;

    semilattice_join(&cpy, divisor);
    return cpy == multiple;
}

template <class T>
void copy_value(const T *in, T *out) {
    *out = *in;
}

// TODO: fdb-ize functions (for writing) below.
// TODO: fdb-ize this, somehow.  (Caller passes in txn and uses it to mutate, probably.)
void real_reql_cluster_interface_t::make_single_selection(
        auth::user_context_t const &user_context,
        const name_string_t &table_name,
        const uuid_u &primary_key,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, admin_op_exc_t) {
    auto table_backend =
        artificial_reql_cluster_interface->get_table_backend(
            table_name,
            admin_identifier_format_t::name);
    counted_t<const ql::db_t> db = make_counted<ql::db_t>(artificial_reql_cluster_interface_t::database_id, artificial_reql_cluster_interface_t::database_name, config_version_checker::empty());
    counted_t<ql::table_t> table;

    if (table_backend.first != nullptr) {
        admin_err_t error;

        ql::datum_t row;
        if (!table_backend.first->read_row(
                user_context,
                convert_uuid_to_datum(primary_key),
                env->interruptor,
                &row,
                &error)) {
            throw admin_op_exc_t(error);
        } else if (!row.has()) {
            /* This is unlikely, but it can happen if the object is deleted between when we
            look up its name and when we call `read_row()` */
            throw no_such_table_exc_t();
        }

        table = make_counted<ql::table_t>(
            make_counted<artificial_table_t>(table_backend.first),
            db,
            table_name,
            read_mode_t::SINGLE,
            bt);

        *selection_out = make_scoped<ql::val_t>(
            ql::single_selection_t::from_row(bt, table, row),
            bt);
    } else if (table_backend.second != nullptr) {
        // TODO: Do we really need to read the row up-front?

        ql::datum_t row;
        fdb_error_t loop_err = txn_retry_loop_coro(env->get_rdb_ctx()->fdb, env->interruptor,
                [&](FDBTransaction *txn) {
            admin_err_t error;
            ql::datum_t tmp_row;
            if (!table_backend.second->read_row(
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
                throw no_such_table_exc_t();
            }
            row = std::move(tmp_row);
        });
        guarantee_fdb_TODO(loop_err, "real_reql_cluster_interface_t::make_single_selection retry loop");

        table = make_counted<ql::table_t>(
            make_counted<artificial_table_fdb_t>(table_backend.second),
            db,
            table_name,
            read_mode_t::SINGLE,
            bt);

        *selection_out = make_scoped<ql::val_t>(
            ql::single_selection_t::from_row(bt, table, row),
            bt);
    } else {
        guarantee(table_backend.first != nullptr || table_backend.second != nullptr,
            "real_reql_cluster_interface_t::make_single_selection used invalid table???");
    }
}
