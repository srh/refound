// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/real_reql_cluster_interface.hpp"

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/grant.hpp"
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/servers/config_client.hpp"
#include "clustering/administration/tables/calculate_status.hpp"
#include "clustering/administration/tables/generate_config.hpp"
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

#define NAMESPACE_INTERFACE_EXPIRATION_MS (60 * 1000)

real_reql_cluster_interface_t::real_reql_cluster_interface_t(
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
        lifetime_t<name_resolver_t const &> name_resolver) :
    m_fdb(fdb),
    m_mailbox_manager(mailbox_manager),
    m_auth_semilattice_view(auth_semilattice_view),
    m_cluster_semilattice_view(cluster_semilattice_view),
    m_table_meta_client(table_meta_client),
    m_cross_thread_database_watchables(get_num_threads()),
    m_rdb_context(rdb_context),
    m_namespace_repo(
        m_mailbox_manager,
        table_query_directory,
        multi_table_manager,
        m_rdb_context,
        m_table_meta_client),
    m_changefeed_client(
        m_mailbox_manager,
        [this](const namespace_id_t &id, signal_t *interruptor) {
            return this->m_namespace_repo.get_namespace_interface(id, interruptor);
        },
        name_resolver),
    m_server_config_client(server_config_client)
{
    guarantee(m_auth_semilattice_view->home_thread() == home_thread());
    guarantee(m_cluster_semilattice_view->home_thread() == home_thread());
    guarantee(m_table_meta_client->home_thread() == home_thread());
    guarantee(m_server_config_client->home_thread() == home_thread());
    for (int thr = 0; thr < get_num_threads(); ++thr) {
        m_cross_thread_database_watchables[thr].init(
            new cross_thread_watchable_variable_t<databases_semilattice_metadata_t>(
                clone_ptr_t<semilattice_watchable_t<databases_semilattice_metadata_t> >
                    (new semilattice_watchable_t<databases_semilattice_metadata_t>(
                        metadata_field(&cluster_semilattice_metadata_t::databases, m_cluster_semilattice_view))), threadnum_t(thr)));
    }
}

bool real_reql_cluster_interface_t::db_drop_uuid(
            auth::user_context_t const &user_context,
            database_id_t database_id,
            const name_string_t &name,
            signal_t *interruptor_on_home,
            ql::datum_t *result_out,
            admin_err_t *error_out) {
    assert_thread();
    guarantee(name != name_string_t::guarantee_valid("rethinkdb"),
        "real_reql_cluster_interface_t should never get queries for system tables");

    // TODO: We'll need to get the table names from fdb.
    std::map<namespace_id_t, table_basic_config_t> table_names;
    m_table_meta_client->list_names(&table_names);
    std::set<namespace_id_t> table_ids;
    for (auto const &table_name : table_names) {
        if (table_name.second.database == database_id) {
            table_ids.insert(table_name.first);
        }
    }

    user_context.require_config_permission(m_rdb_context, database_id, table_ids);

    // Here we actually delete the tables
    size_t tables_dropped = 0;
    for (const auto &table_id : table_ids) {
        try {
            m_table_meta_client->drop(table_id, interruptor_on_home);
            ++tables_dropped;
        } catch (const no_such_table_exc_t &) {
             /* The table was dropped by something else between the time when we called
             `list_names()` and when we went to actually delete it. This is OK. */
        } CATCH_OP_ERRORS(name, table_names.at(table_id).name, error_out,
            "The database was not dropped, but some of the tables in it may or may not "
                "have been dropped.",
            "The database was not dropped, but some of the tables in it may or may not "
                "have been dropped.")
    }

    // TODO: fdb-ize this function
    cluster_semilattice_metadata_t metadata = m_cluster_semilattice_view->get();
    auto iter = metadata.databases.databases.find(database_id);
    if (iter != metadata.databases.databases.end() && !iter->second.is_deleted()) {
        iter->second.mark_deleted();
        m_cluster_semilattice_view->join(metadata);
        metadata = m_cluster_semilattice_view->get();
    } else {
        *error_out = admin_err_t{
            "The database was already deleted.", query_state_t::FAILED};
        return false;
    }
    wait_for_cluster_metadata_to_propagate(metadata, interruptor_on_home);

    if (result_out != nullptr) {
        ql::datum_t old_config =
            convert_db_or_table_config_and_name_to_datum(name, database_id.value);

        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("dbs_dropped", ql::datum_t(1.0));
        result_builder.overwrite(
            "tables_dropped", ql::datum_t(static_cast<double>(tables_dropped)));
        result_builder.overwrite(
            "config_changes",
            make_replacement_pair(std::move(old_config), ql::datum_t::null()));
        *result_out = std::move(result_builder).to_datum();
    }

    return true;
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
// TODO: What does m_table_meta_client do?  Is it bloat?  (In context of table_create, calling ->create() on it.)


bool real_reql_cluster_interface_t::table_list(
        counted_t<const ql::db_t> db,
        UNUSED signal_t *interruptor_on_caller,
        std::set<name_string_t> *names_out,
        UNUSED admin_err_t *error_out) {
    // TODO: fdb-ize this function.
    guarantee(db->name != name_string_t::guarantee_valid("rethinkdb"),
        "real_reql_cluster_interface_t should never get queries for system tables");
    std::map<namespace_id_t, table_basic_config_t> tables;
    m_table_meta_client->list_names(&tables);
    for (const auto &pair : tables) {
        if (pair.second.database == db->id) {
            names_out->insert(pair.second.name);
        }
    }
    return true;
}

bool real_reql_cluster_interface_t::table_find(
        const name_string_t &name,
        counted_t<const ql::db_t> db,
        UNUSED optional<admin_identifier_format_t> identifier_format,
        signal_t *interruptor_on_caller,
        counted_t<base_table_t> *table_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize this function.
    guarantee(db->name != name_string_t::guarantee_valid("rethinkdb"),
        "real_reql_cluster_interface_t should never get queries for system tables");
    namespace_id_t table_id;
    std::string primary_key;
    try {
        m_table_meta_client->find(db->id, name, &table_id, &primary_key);

        /* Note that we completely ignore `identifier_format`. `identifier_format` is
        meaningless for real tables, so it might seem like we should produce an error.
        The reason we don't is that the user might write a query that access both a
        system table and a real table, and they might specify `identifier_format` as a
        global optarg. So then they would get a spurious error for the real table. This
        behavior is also consistent with that of system tables that aren't affected by
        `identifier_format`. */
        table_out->reset(new real_table_t(
            table_id,
            m_namespace_repo.get_namespace_interface(table_id, interruptor_on_caller),
            primary_key,
            &m_changefeed_client,
            m_table_meta_client));

        return true;
    } CATCH_NAME_ERRORS(db->name, name, error_out)
}

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

void real_reql_cluster_interface_t::wait_for_cluster_metadata_to_propagate(
        const cluster_semilattice_metadata_t &metadata,
        signal_t *interruptor_on_caller) {
    int threadnum = get_thread_id().threadnum;

    // TODO: Remove this.

    guarantee(m_cross_thread_database_watchables[threadnum].has());
    m_cross_thread_database_watchables[threadnum]->get_watchable()->run_until_satisfied(
        [&](const databases_semilattice_metadata_t &md) -> bool {
            return is_joined(md, metadata.databases);
        },
        interruptor_on_caller);
}

template <class T>
void copy_value(const T *in, T *out) {
    *out = *in;
}

void real_reql_cluster_interface_t::get_databases_metadata(
        databases_semilattice_metadata_t *out) {
    // TODO: fdb-ize
    int threadnum = get_thread_id().threadnum;
    r_sanity_check(m_cross_thread_database_watchables[threadnum].has());
    m_cross_thread_database_watchables[threadnum]->apply_read(
            std::bind(&copy_value<databases_semilattice_metadata_t>,
                      ph::_1, out));
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
    artificial_table_backend_t *table_backend =
        artificial_reql_cluster_interface->get_table_backend(
            table_name,
            admin_identifier_format_t::name);
    guarantee(table_backend != nullptr);

    admin_err_t error;

    ql::datum_t row;
    if (!table_backend->read_row(
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

    // TODO: This doesn't initialize config_version.  (It shouldn't have to!)
    counted_t<ql::db_t const> db = make_counted<const ql::db_t>(artificial_reql_cluster_interface_t::database_id, artificial_reql_cluster_interface_t::database_name);

    counted_t<ql::table_t> table = make_counted<ql::table_t>(
        make_counted<artificial_table_t>(m_rdb_context, db->id, table_backend),
        db,
        table_name,
        read_mode_t::SINGLE,
        bt);

    *selection_out = make_scoped<ql::val_t>(
        ql::single_selection_t::from_row(bt, table, row),
        bt);
}
