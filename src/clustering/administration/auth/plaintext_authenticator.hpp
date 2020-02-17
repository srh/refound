// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_PLAINTEXT_AUTHENTICATOR_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_PLAINTEXT_AUTHENTICATOR_HPP

#include <string>

#include "clustering/administration/auth/base_authenticator.hpp"
#include "clustering/administration/auth/username.hpp"

namespace auth {

class plaintext_authenticator_t : public base_authenticator_t {
public:
    explicit plaintext_authenticator_t(
        username_t const &username = username_t("admin"));

    std::string next_message(
        FDBDatabase *fdb, const signal_t *interruptor, std::string const &)
            override
            THROWS_ONLY(authentication_error_t, interrupted_exc_t);
    username_t get_authenticated_username() const override
            THROWS_ONLY(authentication_error_t);

private:
    username_t m_username;
    bool m_is_authenticated;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_PLAINTEXT_AUTHENTICATOR_HPP
