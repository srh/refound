// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_BASE_AUTHENTICATOR_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_BASE_AUTHENTICATOR_HPP

#include <string>

#include "concurrency/interruptor.hpp"
#include "clustering/auth/authentication_error.hpp"
#include "clustering/auth/username.hpp"
#include "fdb/fdb.hpp"

namespace auth {

class base_authenticator_t {
public:
    virtual ~base_authenticator_t() { }

    virtual std::string next_message(FDBDatabase *fdb, const signal_t *interruptor, std::string const &)
            THROWS_ONLY(authentication_error_t, interrupted_exc_t) = 0;

    // TODO: It would be nice if the session got broken when the user password changed -- which is something we could easily check whenever we use the user context to read FDB.
    virtual username_t get_authenticated_username() const
            THROWS_ONLY(authentication_error_t) = 0;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_BASE_AUTHENTICATOR_HPP
