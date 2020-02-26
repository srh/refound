// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/metadata.hpp"

#include "clustering/admin_op_exc.hpp"
#include "containers/name_string.hpp"

admin_err_t db_not_found_error(const name_string_t &name) {
    return admin_err_t{
            strprintf("Database `%s` does not exist.", name.c_str()),
            query_state_t::FAILED};
}


