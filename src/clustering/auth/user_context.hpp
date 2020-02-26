// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_USER_CONTEXT_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_USER_CONTEXT_HPP

#include <set>
#include <string>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "clustering/auth/permission_error.hpp"
#include "clustering/auth/permissions.hpp"
#include "clustering/auth/username.hpp"
#include "clustering/auth/user.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"
#include "rpc/serialize_macros.hpp"

namespace auth {

template <class T>
class fdb_user_fut;

class read_permission {
public:
    database_id_t db_id;
    namespace_id_t table_id;
    bool check(const user_t &user) const {
        return user.has_read_permission(db_id, table_id);
    }
    const char *permission_name() const { return "read"; }
};

class write_permission {
public:
    database_id_t db_id;
    namespace_id_t table_id;
    bool check(const user_t &user) const {
        return user.has_read_permission(db_id, table_id) &&
            user.has_write_permission(db_id, table_id);
    }
    const char *permission_name() const { return "write"; }
};

class config_permission {
public:
    bool check(const user_t &user) const { return user.has_config_permission(); }
    const char *permission_name() const { return "config"; }
};

class db_config_permission {
public:
    database_id_t db_id;
    bool check(const user_t &user) const { return user.has_config_permission(db_id); }
    const char *permission_name() const { return "config"; }
};

class db_table_config_permission {
public:
    database_id_t db_id;
    namespace_id_t table_id;
    bool check(const user_t &user) const {
        return user.has_config_permission(db_id, table_id);
    }
    const char *permission_name() const { return "config"; }
};

class db_multi_table_config_permission {
public:
    database_id_t db_id;
    std::vector<namespace_id_t> table_ids;
    bool check(const user_t &user) const {
        if (!user.has_config_permission(db_id)) {
            return false;
        }
        for (const auto &table_id : table_ids) {
            // TODO: Redundant checks on whether we have config permission for db id.
            if (!user.has_config_permission(db_id, table_id)) {
                return false;
            }
        }
        return true;
    }
    const char *permission_name() const { return "config"; }
};

class connect_permission {
public:
    bool check(const user_t &user) const {
        return user.has_config_permission();
    }
    const char *permission_name() const { return "connect"; }
};

// QQQ: These fdb functions should be config-cached in one way or another...

class user_context_t
{
public:
    user_context_t();
    explicit user_context_t(permissions_t permissions);
    explicit user_context_t(username_t username, bool read_only = false);

    bool is_admin_user() const;

    void require_admin_user() const THROWS_ONLY(permission_error_t);

    fdb_user_fut<read_permission> transaction_require_read_permission(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t);

    fdb_user_fut<write_permission> transaction_require_write_permission(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t);

    // transaction_require_config_permission functions work using solely the txn.
    // They may quick-throw if we hold a permissions_t.
    fdb_user_fut<config_permission> transaction_require_config_permission(
            FDBTransaction *txn) const THROWS_ONLY(permission_error_t);

    fdb_user_fut<db_config_permission> transaction_require_db_config_permission(
            FDBTransaction *txn,
            const database_id_t &db_id) const THROWS_ONLY(permission_error_t);

    fdb_user_fut<db_table_config_permission>
    transaction_require_db_and_table_config_permission(
            FDBTransaction *txn,
            const database_id_t &db_id,
            const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t);

    fdb_user_fut<db_multi_table_config_permission>
    transaction_require_db_multi_table_config_permission(
            FDBTransaction *txn,
            const database_id_t &db_id,
            std::vector<namespace_id_t> table_ids) const
            THROWS_ONLY(permission_error_t);

    fdb_user_fut<connect_permission> transaction_require_connect_permission(
            FDBTransaction *txn) const THROWS_ONLY(permission_error_t);

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
