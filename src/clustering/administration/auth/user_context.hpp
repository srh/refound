// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_USER_CONTEXT_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_USER_CONTEXT_HPP

#include <set>
#include <string>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "clustering/administration/auth/permission_error.hpp"
#include "clustering/administration/auth/permissions.hpp"
#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/auth/user.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"
#include "fdb/fdb.hpp"
#include "fdb/typed.hpp"
#include "rpc/serialize_macros.hpp"
// TODO: Gross include cycles.
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

namespace auth {

template <class T>
class fdb_user_fut {
public:
    void block_and_check(const signal_t *interruptor)
            THROWS_ONLY(permission_error_t, interrupted_exc_t) {
        if (user_fut.empty()) {
            return;
        }
        user_t user;
        bool present = user_fut.block_and_deserialize(interruptor, &user);
        if (!present) {
            throw permission_error_t(username, checker.permission_name());
        }
        if (!checker.check(user)) {
            throw permission_error_t(username, checker.permission_name());
        }
    }
    static fdb_user_fut<T> success() {
        fdb_user_fut<T> ret;
        return ret;
    }
    fdb_user_fut() : username(), user_fut() {}
    fdb_user_fut(FDBTransaction *txn, const username_t &uname, T &&_checker)
        : username(uname), user_fut(transaction_get_user(txn, uname)),
          checker(std::move(_checker)) {}

// TODO: Private.
    username_t username;
    fdb_value_fut<user_t> user_fut;
    T checker;
};

class config_permission {
public:
    bool check(const user_t &user) const { return user.has_config_permission(); }
    const char *permission_name() const { return "config"; }
};

class user_context_t
{
public:
    user_context_t();
    explicit user_context_t(permissions_t permissions);
    explicit user_context_t(username_t username, bool read_only = false);

    bool is_admin_user() const;

    void require_admin_user() const THROWS_ONLY(permission_error_t);

    void require_read_permission(
            rdb_context_t *rdb_context,
            database_id_t const &database_id,
            namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t);

    void require_write_permission(
            rdb_context_t *rdb_context,
            database_id_t const &database_id,
            namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t);

    // transaction_require_config_permission functions work using solely the txn.
    // They may quick-throw if we hold a permissions_t.
    fdb_user_fut<config_permission> transaction_require_config_permission(
            FDBTransaction *txn) const THROWS_ONLY(permission_error_t);

    void require_config_permission(
            rdb_context_t *rdb_context) const THROWS_ONLY(permission_error_t);
    void require_config_permission(
            rdb_context_t *rdb_context,
            database_id_t const &database) const THROWS_ONLY(permission_error_t);
    void require_config_permission(
            rdb_context_t *rdb_context,
            database_id_t const &database_id,
            namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t);
    void require_config_permission(
            rdb_context_t *rdb_context,
            database_id_t const &database_id,
            std::set<namespace_id_t> const &table_ids) const THROWS_ONLY(
                permission_error_t);

    void require_connect_permission(
            rdb_context_t *rdb_context) const THROWS_ONLY(permission_error_t);

    std::string to_string() const;

    bool operator<(user_context_t const &rhs) const;
    bool operator==(user_context_t const &rhs) const;
    bool operator!=(user_context_t const &rhs) const;

    RDB_DECLARE_ME_SERIALIZABLE(user_context_t);

private:
    boost::variant<permissions_t, username_t> m_context;
    bool m_read_only;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_USER_CONTEXT_HPP
