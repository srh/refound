// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/changefeed.hpp"

namespace ql {

namespace changefeed {

optional<datum_t> apply_ops(
    const datum_t &val,
    const std::vector<scoped_ptr_t<op_t> > &ops,
    env_t *env,
    const datum_t &key) THROWS_NOTHING {
    try {
        groups_t groups;
        groups[datum_t()] = std::vector<datum_t>{val};
        for (const auto &op : ops) {
            (*op)(env, &groups, [&]() { return key; });
        }
        // TODO: when we support `.group.changes` this will need to change.
        guarantee(groups.size() <= 1);
        std::vector<datum_t> *vec = &groups[datum_t()];
        guarantee(groups.size() == 1);
        // TODO: when we support `.concatmap.changes` this will need to change.
        guarantee(vec->size() <= 1);
        if (vec->size() == 1) {
            return make_optional((*vec)[0]);
        } else {
            return r_nullopt;
        }
    } catch (const base_exc_t &) {
        // Do nothing.  This is similar to index behavior where we drop a row if
        // we fail to execute the code required to produce the index.  (In this
        // case, if you change the value of a row so that one of the
        // transformations errors on it, we report the row as being deleted from
        // the selection you asked for changes on.)
        return r_nullopt;
    }
}

} // namespace changefeed
} // namespace ql
