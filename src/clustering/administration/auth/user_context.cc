// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/auth/user_context.hpp"

#include "clustering/administration/auth/user_fut.hpp"
#include "containers/archive/boost_types.hpp"

namespace auth {

user_context_t::user_context_t() { }

user_context_t::user_context_t(permissions_t permissions)
    : m_context(std::move(permissions)),
      m_read_only(false) {
}

user_context_t::user_context_t(username_t username, bool read_only)
    : m_context(std::move(username)),
      m_read_only(read_only) {
}

bool user_context_t::is_admin_user() const {
    if (auto const *username = boost::get<username_t>(&m_context)) {
        return username->is_admin();
    } else {
        return false;
    }
}

void user_context_t::require_admin_user() const THROWS_ONLY(permission_error_t) {
    if (!is_admin_user()) {
        throw permission_error_t("admin");
    }
}

template <class F, class T>
fdb_user_fut<T> require_permission_internal(
        FDBTransaction *txn,
        boost::variant<permissions_t, username_t> const &context,
        bool read_only,
        F &&permissions_selector_function,
        T &&checker)
    THROWS_ONLY(permission_error_t) {
    if (const permissions_t *permissions = boost::get<permissions_t>(&context)) {
        if (!permissions_selector_function(*permissions)) {
            throw permission_error_t(checker.permission_name());
        }
        return fdb_user_fut<T>::success();
    } else if (const username_t *username = boost::get<username_t>(&context)) {
        if (read_only) {
            throw permission_error_t(*username, checker.permission_name());
        }
        if (username->is_admin()) {
            return fdb_user_fut<T>::success();
        }

        fdb_user_fut<T> ret(txn, *username, std::forward<T>(checker));
        return ret;
    } else {
        unreachable();
    }
}

fdb_user_fut<read_permission> user_context_t::transaction_require_read_permission(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context,
        // Ignore the read-only flag for reads
        false,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_read() == tribool::True;
        },
        read_permission{db_id, table_id});
}

fdb_user_fut<write_permission> user_context_t::transaction_require_write_permission(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_read() == tribool::True &&
                permissions.get_write() == tribool::True;
        },
        write_permission{db_id, table_id});
}



fdb_user_fut<config_permission> user_context_t::transaction_require_config_permission(
        FDBTransaction *txn) const THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        config_permission{});
}

fdb_user_fut<db_config_permission>
user_context_t::transaction_require_db_config_permission(
        FDBTransaction *txn,
        const database_id_t &db_id) const THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        db_config_permission{db_id});
}

fdb_user_fut<db_table_config_permission>
user_context_t::transaction_require_db_and_table_config_permission(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const namespace_id_t &table_id) const THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        db_table_config_permission{db_id, table_id});
}

fdb_user_fut<db_multi_table_config_permission>
user_context_t::transaction_require_db_multi_table_config_permission(
        FDBTransaction *txn,
        database_id_t const &db_id,
        std::vector<namespace_id_t> table_ids) const
        THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        db_multi_table_config_permission{
            db_id,
            std::move(table_ids)});
}

fdb_user_fut<connect_permission>
user_context_t::transaction_require_connect_permission(
        FDBTransaction *txn) const
        THROWS_ONLY(permission_error_t) {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_connect() == tribool::True;
        },
        connect_permission{});
}

std::string user_context_t::to_string() const {
    if (auto const *username = boost::get<username_t>(&m_context)) {
        return username->to_string();
    } else {
        return "internal";
    }
}

bool user_context_t::operator<(user_context_t const &rhs) const {
    return std::tie(m_context, m_read_only) < std::tie(rhs.m_context, rhs.m_read_only);
}

bool user_context_t::operator==(user_context_t const &rhs) const {
    return std::tie(m_context, m_read_only) == std::tie(rhs.m_context, rhs.m_read_only);
}

bool user_context_t::operator!=(user_context_t const &rhs) const {
    return !(*this == rhs);
}

RDB_IMPL_SERIALIZABLE_2(
    user_context_t,
    m_context,
    m_read_only);
INSTANTIATE_SERIALIZABLE_FOR_CLUSTER(user_context_t);

}  // namespace auth
