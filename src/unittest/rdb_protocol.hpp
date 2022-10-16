// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef UNITTEST_RDB_PROTOCOL_HPP_
#define UNITTEST_RDB_PROTOCOL_HPP_

#include <functional>
#include <set>
#include <string>
#include <vector>

#include "containers/scoped.hpp"

class namespace_interface_t;
class order_source_t;
struct sindex_name_t;
class store_t;

namespace unittest {

/* A few functions that might be of use for other unit tests as well. */

void run_with_namespace_interface(
        std::function<void(
            namespace_interface_t *,
            order_source_t *,
            store_t *)> fun,
        bool oversharding = false,
        int num_restarts = 1);

void wait_for_sindex(
    store_t *store,
    const std::string &id);

// Defined in rdb_btree.cc.
sindex_name_t create_sindex(store_t *store);

} /* namespace unittest */

#endif  // UNITTEST_RDB_PROTOCOL_HPP_
