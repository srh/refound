// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/auth/user_context.hpp"

#include "clustering/administration/metadata.hpp"
#include "containers/archive/boost_types.hpp"
#include "rdb_protocol/context.hpp"

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

fdb_user_fut<config_permission> user_context_t::transaction_require_config_permission(
        FDBTransaction *txn) const {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        config_permission{});
}

fdb_user_fut<db_config_permission>
user_context_t::transaction_require_db_config_permission(
        FDBTransaction *txn,
        const database_id_t &db_id) const {
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
        const namespace_id_t &table_id) const {
    return require_permission_internal(txn, m_context, m_read_only,
        [](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        db_table_config_permission{db_id, table_id});
}


template <typename F, typename G>
void require_permission_internal(
        boost::variant<permissions_t, username_t> const &context,
        bool read_only,
        rdb_context_t *rdb_context,
        F permissions_selector_function,
        G username_selector_function,
        std::string const &permission_name) {
    if (rdb_context == nullptr) {
        // This can only happen if the environment was constructed via the constructor
        // that exists for certain sindex and unit-testing purposes
        return;
    }

    // Note I'd preferred to have used a `boost::static_visitor`, but that would require
    // storing the functions in an `std::function`, causing an allocation
    if (auto const *permissions = boost::get<permissions_t>(&context)) {
        if (!permissions_selector_function(*permissions)) {
            throw permission_error_t(permission_name);
        }
    } else if (auto const *username = boost::get<username_t>(&context)) {
        if (read_only) {
            throw auth::permission_error_t(*username, permission_name);
        }
        // The admin user always has the permission
        if (!username->is_admin()) {
            rdb_context->get_auth_watchable()->apply_read(
                [&](auth_semilattice_metadata_t const *auth_metadata) {
                    auto user = auth_metadata->m_users.find(*username);
                    if (user == auth_metadata->m_users.end() ||
                            !static_cast<bool>(user->second.get_ref()) ||
                            !username_selector_function(user->second.get_ref().get())) {
                        throw auth::permission_error_t(*username, permission_name);
                    }
               });
        }
    } else {
        unreachable();
    }
}

void user_context_t::require_read_permission(
        rdb_context_t *rdb_context,
        database_id_t const &database_id,
        namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        // Ignore the read-only flag for reads
        false,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_read() == tribool::True;
        },
        [&](user_t const &user) -> bool {
            return user.has_read_permission(database_id, table_id);
        },
        "read");
}

void user_context_t::require_write_permission(
        rdb_context_t *rdb_context,
        database_id_t const &database_id,
        namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        m_read_only,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_read() == tribool::True
                && permissions.get_write() == tribool::True;
        },
        [&](user_t const &user) -> bool {
            return user.has_read_permission(database_id, table_id) &&
                user.has_write_permission(database_id, table_id);
        },
        "write");
}

void user_context_t::require_config_permission(
        rdb_context_t *rdb_context) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        m_read_only,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        [&](auth::user_t const &user) -> bool {
            return user.has_config_permission();
        },
        "config");
};

void user_context_t::require_config_permission(
        rdb_context_t *rdb_context,
        database_id_t const &database_id) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        m_read_only,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        [&](auth::user_t const &user) -> bool {
            return user.has_config_permission(database_id);
        },
        "config");
}

void user_context_t::require_config_permission(
        rdb_context_t *rdb_context,
        database_id_t const &database_id,
        namespace_id_t const &table_id) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        m_read_only,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        [&](auth::user_t const &user) -> bool {
            return user.has_config_permission(database_id, table_id);
        },
        "config");
}

void user_context_t::require_config_permission(
        rdb_context_t *rdb_context,
        database_id_t const &database_id,
        std::set<namespace_id_t> const &table_ids) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        m_read_only,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_config() == tribool::True;
        },
        [&](auth::user_t const &user) -> bool {
            // First check the permissions on the database
            if (!user.has_config_permission(database_id)) {
                return false;
            }

            // Next, for every table, check if the user has permissions on that table
            for (auto const &table_id : table_ids) {
                if (!user.has_config_permission(database_id, table_id)) {
                    return false;
                }
            }

            return true;
        },
        "config");
}

void user_context_t::require_connect_permission(
        rdb_context_t *rdb_context) const THROWS_ONLY(permission_error_t) {
    require_permission_internal(
        m_context,
        // Ignore the read-only flag for connect, they have no write or config impact
        false,
        rdb_context,
        [&](permissions_t const &permissions) -> bool {
            return permissions.get_connect() == tribool::True;
        },
        [&](user_t const &user) -> bool {
            return user.has_connect_permission();
        },
        "connect");
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
