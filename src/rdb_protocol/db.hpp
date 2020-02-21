#ifndef RETHINKDB_RDB_PROTOCOL_DB_HPP_
#define RETHINKDB_RDB_PROTOCOL_DB_HPP_

#include "containers/counted.hpp"
#include "containers/name_string.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"

namespace ql {
class db_t : public single_threaded_countable_t<db_t> {
public:
    db_t(database_id_t _id, const name_string_t &_name, config_version_checker _cv)
        : id(_id), name(_name), cv(_cv) { }
    const database_id_t id;
    const name_string_t name;
    // The config version behind the db id lookup, if there is one.
    // QQQ: Force everybody accessing database_id_t to use this value.
    config_version_checker cv;
};
} // namespace ql

#endif  // RETHINKDB_RDB_PROTOCOL_DB_HPP_
