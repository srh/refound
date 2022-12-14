// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/auth/user.hpp"

#include <set>
#include <string>

#include "arch/runtime/runtime_utils.hpp"
#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "clustering/id_types.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/versioned.hpp"

namespace auth {

// Note that `m_global_permissions` has a different constructor from the other
// permissions since it has the `"connect"` permission, be sure to construct it

user_t::user_t()
    : m_password(),
      m_global_permissions(
        tribool::Indeterminate,
        tribool::Indeterminate,
        tribool::Indeterminate,
        tribool::Indeterminate) {
}

user_t::user_t(password_t password, permissions_t global_permissions)
    : m_password(std::move(password)),
      m_global_permissions(std::move(global_permissions)) {
}

user_t::user_t(ql::datum_t const &datum)
    : m_password(password_t("")),
      m_global_permissions(
        tribool::Indeterminate,
        tribool::Indeterminate,
        tribool::Indeterminate,
        tribool::Indeterminate) {
    merge(datum);
}

void user_t::merge(ql::datum_t const &datum) {
    if (!datum.has() || datum.get_type() != ql::datum_t::R_OBJECT) {
        throw admin_op_exc_t(
            "Expected an object, got " + datum.print() + ".", query_state_t::FAILED);
    }

    ql::datum_t id = datum.get_field("id", ql::NOTHROW);
    if (!id.has()) {
        throw admin_op_exc_t("Expected a field named `id`.", query_state_t::FAILED);
    }

    ql::datum_t password = datum.get_field("password", ql::NOTHROW);
    if (!password.has()) {
        throw admin_op_exc_t("Expected a field named `password`.", query_state_t::FAILED);
    }

    if (datum.obj_size() != 2) {
        std::set<std::string> keys;
        for (size_t i = 0; i < datum.obj_size(); ++i) {
            keys.insert(datum.get_pair(i).first.to_std());
        }
        keys.erase("id");
        keys.erase("password");

        throw admin_op_exc_t(
            "Unexpected key(s) `" + string_join(keys, "`, `") + "`.",
            query_state_t::FAILED);
    }

    if (password.get_type() == ql::datum_t::R_OBJECT) {
        set_password(password_t(password));
    } else if (password.get_type() == ql::datum_t::R_STR) {
        set_password(password_t(password.as_str().to_std()));
    } else if (password.get_type() == ql::datum_t::R_BOOL) {
        if (password.as_bool()) {
            if (m_password.is_empty()) {
                throw admin_op_exc_t(
                    "Expected an object or string to set the password, or `false` to "
                    "keep it unset, got " + password.print() + ".",
                    query_state_t::FAILED);
            }
        } else {
            set_password(password_t(""));
        }
    } else {
        throw admin_op_exc_t(
            "Expected an object, string or boolean for `password`, got " +
                password.print() + ".",
            query_state_t::FAILED);
    }
}

password_t const &user_t::get_password() const {
    return m_password;
}

void user_t::set_password(password_t password) {
    m_password = std::move(password);
}

permissions_t const &user_t::get_global_permissions() const {
    return m_global_permissions;
}

permissions_t &user_t::get_global_permissions() {
    return m_global_permissions;
}

void user_t::set_global_permissions(permissions_t permissions) {
    m_global_permissions = std::move(permissions);
}

std::map<database_id_t, permissions_t> const &user_t::get_database_permissions() const {
    return m_database_permissions;
}

permissions_t user_t::get_database_permissions(database_id_t const &database_id) const {
    auto iter = m_database_permissions.find(database_id);
    if (iter != m_database_permissions.end()) {
        return iter->second;
    } else {
        return permissions_t(
            tribool::Indeterminate, tribool::Indeterminate, tribool::Indeterminate);
    }
}

permissions_t &user_t::get_database_permissions(database_id_t const &database_id) {
    return m_database_permissions.insert(
        std::make_pair(
            database_id,
            permissions_t(
                tribool::Indeterminate,
                tribool::Indeterminate,
                tribool::Indeterminate))).first->second;
}

void user_t::set_database_permissions(
        database_id_t const &database_id,
        permissions_t permissions) {
    if (permissions.is_indeterminate()) {
        m_database_permissions.erase(database_id);
    } else {
        m_database_permissions[database_id] = std::move(permissions);
    }
}

std::map<namespace_id_t, permissions_t> const &user_t::get_table_permissions() const {
    return m_table_permissions;
}

permissions_t user_t::get_table_permissions(namespace_id_t const &table_id) const {
    auto iter = m_table_permissions.find(table_id);
    if (iter != m_table_permissions.end()) {
        return iter->second;
    } else {
        return permissions_t(
            tribool::Indeterminate, tribool::Indeterminate, tribool::Indeterminate);
    }
}

permissions_t &user_t::get_table_permissions(namespace_id_t const &table_id) {
    return m_table_permissions.insert(
        std::make_pair(
            table_id,
            permissions_t(
                tribool::Indeterminate,
                tribool::Indeterminate,
                tribool::Indeterminate))).first->second;
}

void user_t::set_table_permissions(
        namespace_id_t const &table_id,
        permissions_t permissions) {
    if (permissions.is_indeterminate()) {
        m_table_permissions.erase(table_id);
    } else {
        m_table_permissions[table_id] = std::move(permissions);
    }
}

void user_t::set_db_or_table_permissions_indeterminate(
        const uuid_u &db_or_table_id) {
    // Only one of these can succeed, of course.
    m_table_permissions.erase(namespace_id_t{db_or_table_id});
    m_database_permissions.erase(database_id_t{db_or_table_id});
}

bool user_t::has_read_permission(
        database_id_t const &database_id,
        namespace_id_t const &table_id) const {
    auto table = m_table_permissions.find(table_id);
    if (table != m_table_permissions.end()) {
        tribool table_permission = table->second.get_read();
        if (table_permission != tribool::Indeterminate) {
            return table_permission == tribool::True;
        }
    }

    auto database = m_database_permissions.find(database_id);
    if (database != m_database_permissions.end()) {
        tribool database_permission = database->second.get_read();
        if (database_permission != tribool::Indeterminate) {
            return database_permission == tribool::True;
        }
    }

    if (database_id != artificial_reql_cluster_interface_t::database_id) {
        return m_global_permissions.get_read() == tribool::True;
    } else {
        // The artificial table does not inherit permissions from the global scope.
        return false;
    }
}

bool user_t::has_write_permission(
        database_id_t const &database_id,
        namespace_id_t const &table_id) const {
    auto table = m_table_permissions.find(table_id);
    if (table != m_table_permissions.end()) {
        tribool table_permission = table->second.get_write();
        if (table_permission != tribool::Indeterminate) {
            return table_permission == tribool::True;
        }
    }

    auto database = m_database_permissions.find(database_id);
    if (database != m_database_permissions.end()) {
        tribool database_permission = database->second.get_write();
        if (database_permission != tribool::Indeterminate) {
            return database_permission == tribool::True;
        }
    }

    if (database_id != artificial_reql_cluster_interface_t::database_id) {
        return m_global_permissions.get_write() == tribool::True;
    } else {
        // The artificial table does not inherit permissions from the global scope.
        return false;
    }
}

bool user_t::has_config_permission() const {
    return m_global_permissions.get_config() == tribool::True;
}

bool user_t::has_config_permission(
        database_id_t const &database_id) const {
    auto database = m_database_permissions.find(database_id);
    if (database != m_database_permissions.end()) {
        tribool database_permission = database->second.get_config();
        if (database_permission != tribool::Indeterminate) {
            return database_permission == tribool::True;
        }
    }

    if (database_id != artificial_reql_cluster_interface_t::database_id) {
        return has_config_permission();
    } else {
        // The artificial table does not inherit permissions from the global scope.
        return false;
    }
}

bool user_t::has_config_permission(
        database_id_t const &database_id,
        namespace_id_t const &table_id) const {
    auto table = m_table_permissions.find(table_id);
    if (table != m_table_permissions.end()) {
        tribool table_permission = table->second.get_config();
        if (table_permission != tribool::Indeterminate) {
            return table_permission == tribool::True;
        }
    }

    return has_config_permission(database_id);
}

bool user_t::has_connect_permission() const {
    return m_global_permissions.get_connect() == tribool::True;
}

bool user_t::operator==(user_t const &rhs) const {
    return
        m_password == rhs.m_password &&
        m_global_permissions == rhs.m_global_permissions &&
        m_database_permissions == rhs.m_database_permissions &&
        m_table_permissions == rhs.m_table_permissions;
}

std::unordered_set<uuid_u> get_index_uuids(const user_t &user) {
    std::unordered_set<uuid_u> ret;
    for (const auto &pair : user.get_database_permissions()) {
        ret.insert(pair.first.value);
    }
    for (const auto &pair : user.get_table_permissions()) {
        ret.insert(pair.first.value);
    }
    return ret;
}

RDB_IMPL_SERIALIZABLE_4(
    user_t,
    m_password,
    m_global_permissions,
    m_database_permissions,
    m_table_permissions);
INSTANTIATE_SERIALIZABLE_SINCE_v2_3(user_t);

}  // namespace auth
