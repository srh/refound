// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_METADATA_HPP_
#define CLUSTERING_ADMINISTRATION_METADATA_HPP_

#include "utils.hpp"

// TODO: Remove spurious includers.

struct admin_err_t;
class name_string_t;

admin_err_t db_not_found_error(const name_string_t &name);

#define rfail_db_not_found(bt, name) \
    rfail_src((bt), ::ql::base_exc_t::OP_FAILED, \
        "Database `%s` does not exist.", (name).c_str())

#endif  // CLUSTERING_ADMINISTRATION_METADATA_HPP_
