// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/metadata.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "containers/archive/archive.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/versioned.hpp"
#include "rdb_protocol/protocol.hpp"
#include "stl_utils.hpp"

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_3(auth_semilattice_metadata_t, m_users);
RDB_IMPL_SEMILATTICE_JOINABLE_1(auth_semilattice_metadata_t, m_users);
RDB_IMPL_EQUALITY_COMPARABLE_1(auth_semilattice_metadata_t, m_users);

admin_err_t db_not_found_error(const name_string_t &name) {
    return admin_err_t{
            strprintf("Database `%s` does not exist.", name.c_str()),
            query_state_t::FAILED};
}


