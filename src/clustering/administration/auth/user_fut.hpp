#ifndef CLUSTERING_ADMINISTRATION_AUTH_USER_FUT_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_USER_FUT_HPP

#include "clustering/administration/auth/permission_error.hpp"
#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/auth/user.hpp"
#include "fdb/fdb.hpp"
#include "fdb/typed.hpp"
// TODO: Gross include dependency (for transaction_get_user)
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

class signal_t;

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

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_USER_FUT_HPP
