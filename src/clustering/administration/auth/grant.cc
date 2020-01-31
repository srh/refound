// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/table_common.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/auth/user.hpp"
#include "clustering/administration/metadata.hpp"
// TODO: Ugly circular module ref here.
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "rpc/semilattice/view.hpp"

namespace auth {

// TODO: Can permission_selector_function be a raw function pointer?
bool grant(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        auth::username_t username,
        ql::datum_t permissions,
        signal_t *interruptor,
        std::function<auth::permissions_t *(auth::user_t *)> &&permission_selector_function,
        ql::datum_t *result_out,
        admin_err_t *error_out)
        THROWS_ONLY(interrupted_exc_t, permissions_error_t) {
    if (username.is_admin()) {
        *error_out = admin_err_t{
            "The permissions of the user `" + username.to_string() +
                "` can't be modified.",
            query_state_t::FAILED};
        return false;
    }


    fdb_user_fut<write_permission> auth_fut
        = user_context.transaction_require_write_permission(
            txn,
            artificial_reql_cluster_interface_t::database_id,
            artificial_table_backend_t::compute_artificial_table_id(
                name_string_t::guarantee_valid("permissions")));
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    fdb_value_fut<user_t> user_fut = transaction_get_user(txn, username);

    // TODO: A question to answer is, why don't we perform this using an
    // artificial_table_t operation?
    auth_fut.block_and_check(interruptor);

    user_t user;
    if (!user_fut.block_and_deserialize(interruptor, &user)) {
        *error_out = admin_err_t{
            "User `" + username.to_string() + "` not found.", query_state_t::FAILED};
        return false;
    }

    ql::datum_t old_permissions;
    ql::datum_t new_permissions;
    try {
        auth::permissions_t *permissions_ref = permission_selector_function(&user);
        old_permissions = permissions_ref->to_datum();
        permissions_ref->merge(permissions);
        new_permissions = permissions_ref->to_datum();
    } catch (admin_op_exc_t const &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    }

    // We have a new user value.  Write it!
    transaction_set_user(txn, username, user);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);

    ql::datum_object_builder_t result_builder;
    result_builder.overwrite("granted", ql::datum_t(1.0));
    result_builder.overwrite(
        "permissions_changes",
        make_replacement_pair(old_permissions, new_permissions));
    *result_out = std::move(result_builder).to_datum();
    return true;
}

// OOO: Remove this (and decl)
bool grant(
        FDBTransaction *txn,
        std::shared_ptr<semilattice_readwrite_view_t<auth_semilattice_metadata_t>>
            auth_semilattice_view,
        rdb_context_t *rdb_context,
        auth::user_context_t const &user_context,
        auth::username_t username,
        ql::datum_t permissions,
        signal_t *interruptor,
        std::function<auth::permissions_t &(auth::user_t &)> permission_selector_function,
        ql::datum_t *result_out,
        admin_err_t *error_out) {
    // TODO: fdb-ize
    if (username.is_admin()) {
        *error_out = admin_err_t{
            "The permissions of the user `" + username.to_string() +
                "` can't be modified.",
            query_state_t::FAILED};
        return false;
    }

    counted_t<const ql::db_t> db;
    if (!rdb_context->cluster_interface->db_find(
            name_string_t::guarantee_valid("rethinkdb"),
            interruptor,
            &db,
            error_out)) {
        return false;
    }

    counted_t<base_table_t> table;
    if (!rdb_context->cluster_interface->table_find(
            name_string_t::guarantee_valid("permissions"),
            db,
            make_optional(admin_identifier_format_t::uuid),
            interruptor,
            &table,
            error_out)) {
        return false;
    }

    user_context.require_read_permission(rdb_context, db->id, table->get_id());
    user_context.require_write_permission(rdb_context, db->id, table->get_id());

    auth_semilattice_metadata_t auth_metadata = auth_semilattice_view->get();
    auto grantee = auth_metadata.m_users.find(username);
    if (grantee == auth_metadata.m_users.end() ||
            !static_cast<bool>(grantee->second.get_ref())) {
        *error_out = admin_err_t{
            "User `" + username.to_string() + "` not found.", query_state_t::FAILED};
        return false;
    }

    ql::datum_t old_permissions;
    ql::datum_t new_permissions;
    try {
        grantee->second.apply_write([&](optional<auth::user_t> *user) {
            auth::permissions_t &permissions_ref = permission_selector_function(user->get());
            old_permissions = permissions_ref.to_datum();
            permissions_ref.merge(permissions);
            new_permissions = permissions_ref.to_datum();
        });
    } catch (admin_op_exc_t const &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    }

    auth_semilattice_view->join(auth_metadata);

    // Wait for the metadata to propegate
    rdb_context->get_auth_watchable()->run_until_satisfied(
        [&](auth_semilattice_metadata_t const &metadata) -> bool {
            auth_semilattice_metadata_t copy = metadata;
            semilattice_join(&copy, auth_metadata);
            return copy == metadata;
        },
        interruptor);
    (void) rdb_context;
    (void) interruptor;

    ql::datum_object_builder_t result_builder;
    result_builder.overwrite("granted", ql::datum_t(1.0));
    result_builder.overwrite(
        "permissions_changes",
        make_replacement_pair(old_permissions, new_permissions));
    *result_out = std::move(result_builder).to_datum();
    return true;
}

}  // namespace auth

