// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_
#define CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_

#include "containers/name_string.hpp"
#include "query_state.hpp"
#include "utils.hpp"

// TODO: Remove no_such_table_exc_t, if it's used sparingly.

class no_such_table_exc_t : public std::runtime_error {
public:
    no_such_table_exc_t() :
        std::runtime_error("There is no table with the given name / UUID.") { }
};

struct admin_err_t {
    std::string msg;
    query_state_t query_state;
};

// These have to be macros because `rfail` and `rfail_datum` are macros.
#define REQL_RETHROW(X) do {                                            \
        rfail((X).query_state == query_state_t::FAILED                  \
              ? ql::base_exc_t::OP_FAILED                               \
              : ql::base_exc_t::OP_INDETERMINATE,                       \
              "%s", (X).msg.c_str());                                   \
    } while (0)
#define REQL_RETHROW_DATUM(X) do {                                      \
        rfail_datum((X).query_state == query_state_t::FAILED            \
                    ? ql::base_exc_t::OP_FAILED                         \
                    : ql::base_exc_t::OP_INDETERMINATE,                 \
                    "%s", (X).msg.c_str());                             \
    } while (0)


/* This is a generic class for errors that occur during administrative operations. It has
a string error message which is suitable for presenting to the user. */
class admin_op_exc_t : public std::runtime_error {
public:
    explicit admin_op_exc_t(admin_err_t err)
        : std::runtime_error(std::move(err.msg)),
          m_query_state(err.query_state) { }

    admin_op_exc_t(std::string message, query_state_t query_state)
        : std::runtime_error(std::move(message)),
          m_query_state(query_state) { }

    admin_err_t to_admin_err() const {
        return admin_err_t{what(), m_query_state};
    }

private:
    query_state_t m_query_state;
};

inline admin_err_t table_dne_error(
        const name_string_t &db_name, const name_string_t &table_name) {
    return admin_err_t{
        strprintf("Table `%s.%s` does not exist.", db_name.c_str(), table_name.c_str()),
        query_state_t::FAILED
    };
}

#define rfail_table_dne(db_name, table_name) \
    rfail(ql::base_exc_t::OP_FAILED, "Table `%s.%s` does not exist.", \
          (db_name).c_str(), (table_name).c_str())

/* `CATCH_NAME_ERRORS` is a helper macro for catching the
exceptions thrown by the `table_meta_client_t` and producing consistent error messages.
They're designed to be used as follows:

    try {
        something_that_might_throw()
    } CATCH_NAME_ERRORS(db, name, error_out);

TODO: Probably these should re-throw `admin_op_exc_t` instead of setting `*error_out` and
returning `false`. */

// NNN: Remove these.

#define CATCH_NAME_ERRORS(db, name, error_out)                                        \
    catch (const no_such_table_exc_t &) {                                             \
        *(error_out) = table_dne_error((db), (name));                                 \
        return false;                                                                 \
    }

#endif /* CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_ */

