#ifndef RETHINKDB_RDB_PROTOCOL_DB_HPP_
#define RETHINKDB_RDB_PROTOCOL_DB_HPP_

#include "containers/counted.hpp"
#include "containers/name_string.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"
#include "rdb_protocol/error.hpp"

// Represents a db_name -> db_id lookup as of config version cv.  If cv is ::empty(),
// the db was the artificial db and the id lookup doesn't need to be verified (because
// it's constant).

// NNN: Maybe this should have a backtrace_id_t in it.
class provisional_db_id {
public:
    name_string_t db_name;
    ql::backtrace_id_t bt;
};

namespace ql {

class db_t : public single_threaded_countable_t<db_t> {
public:
    db_t(database_id_t _id, const name_string_t &_name, config_version_checker _cv)
        : id(_id), name(_name), cv(_cv) { }
    const database_id_t id;
    const name_string_t name;
    // The config version behind the db id lookup, if there is one.
    // QQQ: Force everybody accessing database_id_t to use this value.
    const config_version_checker cv;
};
} // namespace ql

#endif  // RETHINKDB_RDB_PROTOCOL_DB_HPP_
