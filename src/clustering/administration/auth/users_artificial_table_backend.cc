// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/auth/users_artificial_table_backend.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/auth/user.hpp"
#include "clustering/administration/metadata.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

namespace auth {

users_artificial_table_fdb_backend_t::users_artificial_table_fdb_backend_t()
    : artificial_table_fdb_backend_t(name_string_t::guarantee_valid("users")) {
}

ql::datum_t user_to_datum(username_t const &username, user_t const &user) {
    ql::datum_object_builder_t builder;
    builder.overwrite("id", ql::datum_t(datum_string_t(username.to_string())));
    builder.overwrite("password", ql::datum_t::boolean(!user.get_password().is_empty()));
    return std::move(builder).to_datum();
}

std::string users_artificial_table_fdb_backend_t::get_primary_key_name() const {
    return "id";
}


bool users_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        UNUSED user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {
    // QQQ: Do we need to filter out the admin user (which is put into fdb)?
    std::string prefix = REQLFDB_USERS_BY_USERNAME;
    std::string end = prefix_end(prefix);

    std::vector<std::pair<username_t, user_t>> rows;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&](FDBTransaction *txn) {
        std::vector<std::pair<username_t, user_t>> builder;
        transaction_read_whole_range_coro(txn, prefix, end, interruptor, [&](const FDBKeyValue &kv) {
            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view just_the_key = whole_key.guarantee_without_prefix(prefix);
            username_t uname = username_parse_pkey(just_the_key);
            builder.emplace_back();
            builder.back().first = std::move(uname);
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &builder.back().second);
            return true;
        });
        rows = std::move(builder);
    });
    guarantee_fdb_TODO(loop_err, "users_artificial_table_fdb_backend_t::read_all_rows_as_vector"
        " retry loop");

    std::vector<ql::datum_t> datums;
    datums.reserve(rows.size());
    for (auto &row : rows) {
        datums.push_back(user_to_datum(row.first, row.second));
    }

    *rows_out = std::move(datums);
    return true;
}

// QQQ: Double-check all fdb backend read_row functions output ql::datum_t::null().

bool users_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    // QQQ: Do we need to filter out the admin user (which is put into fdb)?
    *row_out = ql::datum_t::null();

    if (primary_key.get_type() == ql::datum_t::R_STR) {
        username_t username(primary_key.as_str().to_std());

        fdb_value_fut<user_t> user_fut = transaction_get_user(txn, username);
        user_t user;
        if (user_fut.block_and_deserialize(interruptor, &user)) {
            *row_out = user_to_datum(username, user);
        }
    }

    return true;
}

// QQQ: Check write_row callers' treatment of new_value_inout obeys fdb transaction rules and doesn't leak side effects outside the txn retry loop.

bool users_artificial_table_fdb_backend_t::write_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) {
    if (primary_key.get_type() != ql::datum_t::R_STR || pkey_was_autogenerated) {
        *error_out = admin_err_t{
            "Expected a username as the primary key, got " + primary_key.print() + ".",
            query_state_t::FAILED};
        return false;
    }

    username_t username(primary_key.as_str().to_std());

    // NNN: Update config version in all cases.
    if (new_value_inout->has()) {
        try {
            // TODO: See if .merge() is what can throw the admin_op_exc_t.
            fdb_value_fut<user_t> user_fut = transaction_get_user(txn, username);
            fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
            // TODO: new_value_inout doesn't really get written back out.  It didn't
            // pre-fdb, either.  Should it be?
            user_t user;
            if (user_fut.block_and_deserialize(interruptor, &user)) {
                user.merge(*new_value_inout);
            } else {
                user = user_t(*new_value_inout);
            }

            transaction_set_user(txn, username, user);
            reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
            cv.value++;
            transaction_set_config_version(txn, cv);
            return true;
        } catch(admin_op_exc_t const &admin_op_exc) {
            *error_out = admin_op_exc.to_admin_err();
            return false;
        }
    } else {
        if (username.is_admin()) {
            *error_out = admin_err_t{
                "The user `" + username.to_string() + "` can't be deleted.",
                query_state_t::FAILED};
            return false;
        }

        fdb_value_fut<user_t> user_fut = transaction_get_user(txn, username);
        fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

        user_t user;
        if (!user_fut.block_and_deserialize(interruptor, &user)) {
            *error_out = admin_err_t{
                "User `" + username.to_string() + "` not found.",
                query_state_t::FAILED};
            return false;
        }

        // User exists.  Delete it.
        transaction_erase_user(txn, username);
        reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
        cv.value++;
        transaction_set_config_version(txn, cv);

        // TODO (long run): Any metadata referencing the user (log entries) might
        // identify it with a uuid.  Then if we drop/recreate a user, we don't use the
        // uuid.
    }

    return true;
}

}  // namespace auth
