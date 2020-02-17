// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_CHANGEFEED_HPP_
#define RDB_PROTOCOL_CHANGEFEED_HPP_

#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <string>
#include <vector>
#include <utility>

#include "errors.hpp"

#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/datumspec.hpp"
#include "rdb_protocol/shards.hpp"

// The string is the btree index key
typedef std::pair<ql::datum_t, std::string> index_pair_t;
typedef std::map<std::string, std::vector<index_pair_t> > index_vals_t;

namespace ql {


namespace changefeed {

optional<datum_t> apply_ops(
    const datum_t &val,
    const std::vector<scoped_ptr_t<op_t> > &ops,
    env_t *env,
    const datum_t &key) THROWS_NOTHING;

} // namespace changefeed
} // namespace ql

#endif // RDB_PROTOCOL_CHANGEFEED_HPP_

