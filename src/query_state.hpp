#ifndef RETHINKDB_QUERY_STATE_HPP_
#define RETHINKDB_QUERY_STATE_HPP_

#include "rpc/serialize_macros.hpp"

enum class query_state_t { FAILED, INDETERMINATE };

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        query_state_t, int8_t, query_state_t::FAILED, query_state_t::INDETERMINATE);

#endif  // RETHINKDB_QUERY_STATE_HPP_
