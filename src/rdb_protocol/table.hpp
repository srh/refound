#ifndef RETHINKDB_RDB_PROTOCOL_TABLE_HPP_
#define RETHINKDB_RDB_PROTOCOL_TABLE_HPP_

#include "containers/optional.hpp"
#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/db.hpp"

namespace ql {
class env_t;
class table_t;
};

class provisional_table_id {
public:
    provisional_db_id prov_db;
    name_string_t table_name;
    // TODO: Make this non-optional -- just put in the universal default value (which is?).  So far I've seen "name" used as the default.
    optional<admin_identifier_format_t> identifier_format;
    ql::backtrace_id_t bt;

    // Callers of this function generally must _not_ assume the table exists, e.g. for use
    // in error messages -- every caller right now has verified the table exists when
    // using this function for error message creation.  The trailing underscore is left in
    // the name as a marker of weirdness.
    std::string display_name_() const {
        return prov_db.db_name.str() + "." + table_name.str();
    }
};

namespace ql {

std::pair<datum_t, scoped<table_t>> prov_read_row(
    env_t *env,
    const provisional_table_id &id,
    const datum_t &pval);

}


#endif  // RETHINKDB_RDB_PROTOCOL_TABLE_HPP_
