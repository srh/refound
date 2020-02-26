// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/auth/plaintext_authenticator.hpp"

#include "clustering/auth/authentication_error.hpp"
#include "clustering/auth/user.hpp"
#include "crypto/compare_equal.hpp"
#include "crypto/pbkcs5_pbkdf2_hmac.hpp"
#include "crypto/saslprep.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

namespace auth {

plaintext_authenticator_t::plaintext_authenticator_t(
        username_t const &username)
    : m_username(username),
      m_is_authenticated(false) {
}

std::string plaintext_authenticator_t::next_message(
        FDBDatabase *fdb, const signal_t *interruptor,
        std::string const &password) THROWS_ONLY(authentication_error_t, interrupted_exc_t) {
    optional<user_t> user;

    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [&](FDBTransaction *txn) {
        fdb_value_fut<auth::user_t> user_fut = transaction_get_user(txn, m_username);
        user_t loop_user;
        if (!user_fut.block_and_deserialize(interruptor, &loop_user)) {
            return;
        }
        user.set(std::move(loop_user));
    });
    guarantee_fdb_TODO(loop_err, "next_message loading user");


    if (!user.has_value()) {
        // The user doesn't exist
        throw authentication_error_t(17, "Unknown user");
    }

    std::array<unsigned char, SHA256_DIGEST_LENGTH> hash =
        crypto::pbkcs5_pbkdf2_hmac_sha256(
            crypto::saslprep(password),
            user->get_password().get_salt(),
            user->get_password().get_iteration_count());

    if (!crypto::compare_equal(user->get_password().get_hash(), hash)) {
        throw authentication_error_t(12, "Wrong password");
    }

    m_is_authenticated = true;

    return "";
}

username_t plaintext_authenticator_t::get_authenticated_username() const
        THROWS_ONLY(authentication_error_t) {
    if (m_is_authenticated) {
        return m_username;
    } else {
        throw authentication_error_t(20, "No authenticated user");
    }
}

}  // namespace auth
