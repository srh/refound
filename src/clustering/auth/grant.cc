// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/table_common.hpp"

#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/auth/user.hpp"
#include "clustering/auth/user_fut.hpp"
// TODO: Ugly circular module ref here.
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

namespace auth {

bool grant(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        auth::username_t username,
        ql::datum_t permissions,
        const signal_t *interruptor,
        const std::function<auth::permissions_t *(auth::user_t *, FDBTransaction *, const signal_t *)> &permission_selector_function,
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
            artificial_table_fdb_backend_t::compute_artificial_table_id(
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
    user_t old_user = user;

    ql::datum_t old_permissions;
    ql::datum_t new_permissions;
    try {
        auth::permissions_t *permissions_ref = permission_selector_function(&user, txn, interruptor);
        old_permissions = permissions_ref->to_datum();
        permissions_ref->merge(permissions);
        new_permissions = permissions_ref->to_datum();
    } catch (admin_op_exc_t const &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    }

    // We have a new user value.  Write it!
    transaction_modify_user(txn, username, old_user, user);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    cv.value++;
    transaction_set_config_version(txn, cv);

    ql::datum_object_builder_t result_builder;
    result_builder.overwrite("granted", ql::datum_t(1.0));
    result_builder.overwrite(
        "permissions_changes",
        make_replacement_pair(old_permissions, new_permissions));
    *result_out = std::move(result_builder).to_datum();
    return true;
}

}  // namespace auth

