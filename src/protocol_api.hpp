// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef PROTOCOL_API_HPP_
#define PROTOCOL_API_HPP_

#include <algorithm>
#include <memory> // for std::shared_ptr, remove.
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "btree/keys.hpp"
#include "buffer_cache/types.hpp"
#include "concurrency/interruptor.hpp"
#include "concurrency/signal.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/object_buffer.hpp"
#include "containers/scoped.hpp"
#include "query_state.hpp"
#include "rpc/serialize_macros.hpp"
#include "version.hpp"

namespace auth {
class permission_error_t;
class user_context_t;
class username_t;
}  // namespace auth

struct read_t;
struct read_response_t;
struct write_t;
struct write_response_t;

class cannot_perform_query_exc_t : public std::exception {
public:
    // SHOULD ONLY BE USED FOR SERIALIZATION
    cannot_perform_query_exc_t()
        : message("UNINITIALIZED"), query_state(query_state_t::FAILED) { }
    cannot_perform_query_exc_t(const std::string &s, query_state_t _query_state)
        : message(s), query_state(_query_state) { }
    ~cannot_perform_query_exc_t() throw () { }
    const char *what() const throw () {
        return message.c_str();
    }
    query_state_t get_query_state() const throw () { return query_state; }
private:
    RDB_DECLARE_ME_SERIALIZABLE(cannot_perform_query_exc_t);
    std::string message;
    query_state_t query_state;
};
RDB_DECLARE_SERIALIZABLE_FOR_CLUSTER(cannot_perform_query_exc_t);

enum class table_readiness_t {
    unavailable,
    outdated_reads,
    reads,
    writes,
    finished
};

// Specifies the desired behavior for insert operations, upon discovering a
// conflict.
//  - conflict_behavior_t::ERROR: Signal an error upon conflicts.
//  - conflict_behavior_t::REPLACE: Replace the old row with the new row if a
//    conflict occurs.
//  - conflict_behavior_t::UPDATE: Merge the old and new rows if a conflict
//    occurs.
enum class conflict_behavior_t { ERROR, REPLACE, UPDATE, FUNCTION };

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(conflict_behavior_t,
                                      int8_t,
                                      conflict_behavior_t::ERROR,
                                      conflict_behavior_t::FUNCTION);

// Specifies whether or not to ignore a write hook on a table while doing an
// insert or a replace.
//  - ignore_write_hook_t::YES: Ignores the write hook, requires config permissions.
//  - ignore_write_hook_t::NO: Applies the write hook as normal.
enum class ignore_write_hook_t {
    NO = 0,
    YES = 1
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(ignore_write_hook_t,
                                      int8_t,
                                      ignore_write_hook_t::NO,
                                      ignore_write_hook_t::YES);

// Specifies the durability requirements of a write operation.
//  - DURABILITY_REQUIREMENT_DEFAULT: Use the table's durability settings.
//  - DURABILITY_REQUIREMENT_HARD: Override the table's durability settings with
//    hard durability.
//  - DURABILITY_REQUIREMENT_SOFT: Override the table's durability settings with
//    soft durability.
enum durability_requirement_t { DURABILITY_REQUIREMENT_DEFAULT,
                                DURABILITY_REQUIREMENT_HARD,
                                DURABILITY_REQUIREMENT_SOFT };

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(durability_requirement_t,
                                      int8_t,
                                      DURABILITY_REQUIREMENT_DEFAULT,
                                      DURABILITY_REQUIREMENT_SOFT);

enum class read_mode_t { MAJORITY, SINGLE, OUTDATED, DEBUG_DIRECT };

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(read_mode_t,
                                      int8_t,
                                      read_mode_t::MAJORITY,
                                      read_mode_t::DEBUG_DIRECT);

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        importable_reql_version_t, int8_t,
        importable_reql_version_t::EARLIEST, importable_reql_version_t::LATEST);

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        reql_version_t, int8_t,
        reql_version_t::EARLIEST, reql_version_t::LATEST);

enum class emergency_repair_mode_t { DEBUG_RECOMMIT,
                                     UNSAFE_ROLLBACK,
                                     UNSAFE_ROLLBACK_OR_ERASE };

#endif /* PROTOCOL_API_HPP_ */
