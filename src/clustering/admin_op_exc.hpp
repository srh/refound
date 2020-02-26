// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_
#define CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_

#include "containers/name_string.hpp"
#include "query_state.hpp"
#include "utils.hpp"

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
#define REQL_RETHROW_SRC(bt, X) \
        rfail_src((bt), (X).query_state == query_state_t::FAILED \
                    ? ql::base_exc_t::OP_FAILED \
                    : ql::base_exc_t::OP_INDETERMINATE, \
                    "%s", (X).msg.c_str())



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

#define rfail_table_dne(db_name, table_name) \
    rfail(ql::base_exc_t::OP_FAILED, "Table `%s.%s` does not exist.", \
          (db_name).c_str(), (table_name).c_str())
#define rfail_table_dne_src(bt, db_name, table_name) \
    rfail_src((bt), ql::base_exc_t::OP_FAILED, "Table `%s.%s` does not exist.", \
          (db_name).c_str(), (table_name).c_str())
// TODO: Don't triple-eval the argument.  Make this an inline function?  (Does rfail_src pick up a line number?)
#define rfail_prov_table_dne(prov_table) \
    rfail_src((prov_table).bt, ql::base_exc_t::OP_FAILED, "Table `%s.%s` does not exist.", \
          (prov_table).prov_db.db_name.c_str(), (prov_table).table_name.c_str())

#endif /* CLUSTERING_ADMINISTRATION_ADMIN_OP_EXC_HPP_ */

