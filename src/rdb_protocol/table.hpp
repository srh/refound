#ifndef RETHINKDB_RDB_PROTOCOL_TABLE_HPP_
#define RETHINKDB_RDB_PROTOCOL_TABLE_HPP_

class provisional_table_id {
public:
    provisional_db_id prov_db;
    name_string_t table_name;
    // TODO: Make this non-optional -- just put in the universal default value (which is?).  So far I've seen "name" used as the default.
    optional<admin_identifier_format_t> identifier_format;
    ql::backtrace_id_t bt;

    // NNN: Remove -- or look at every usage to see if error message does not falsely assume table exists.
    std::string display_name() const {
        return prov_db.db_name.str() + "." + table_name.str();
    }
};


#endif  // RETHINKDB_RDB_PROTOCOL_TABLE_HPP_
