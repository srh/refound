// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/auth/username.hpp"

#include "clustering/auth/authentication_error.hpp"
#include "containers/archive/stl_types.hpp"
#include "crypto/saslprep.hpp"

namespace auth {

username_t::username_t() { }

username_t::username_t(std::string const &username) {
    m_username = crypto::saslprep(username);
    if (m_username.empty()) {
        throw authentication_error_t(18, "Empty username");
    }
}

bool username_t::is_admin() const {
    return m_username == "admin";
}

bool username_t::operator<(username_t const &rhs) const {
    return m_username < rhs.m_username;
}

bool username_t::operator==(username_t const &rhs) const {
    return m_username == rhs.m_username;
}

bool username_t::operator!=(username_t const &rhs) const {
    return !(*this == rhs);
}

RDB_IMPL_SERIALIZABLE_1(username_t, m_username);
INSTANTIATE_SERIALIZABLE_SINCE_v2_3(username_t);

}  // namespace auth
