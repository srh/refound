// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_GRANT_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_GRANT_HPP

#include "rdb_protocol/table_common.hpp"

#include "fdb/fdb.hpp"

template <class> class semilattice_readwrite_view_t;
struct admin_err_t;

namespace auth {

class user_t;

MUST_USE bool grant(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        auth::username_t username,
        ql::datum_t permissions,
        const signal_t *interruptor,
        const std::function<auth::permissions_t *(auth::user_t *, FDBTransaction *, const signal_t *)> &permission_selector_function,
        ql::datum_t *result_out,
        admin_err_t *error_out)
    THROWS_ONLY(interrupted_exc_t, permissions_error_t);

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_GRANT_HPP
