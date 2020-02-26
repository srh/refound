// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/tables/table_metadata.hpp"

#include "clustering/id_types.hpp"
#include "containers/archive/archive.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl/unordered_map.hpp"
#include "containers/archive/versioned.hpp"
#include "containers/archive/optional.hpp"
#include "rdb_protocol/protocol.hpp"

// We start with an empty object, not null -- because a good user would set fields of
// that object.
user_data_t default_user_data() {
    return user_data_t{ql::datum_t::empty_object()};
}

RDB_MAKE_SERIALIZABLE_1(user_data_t, datum);

RDB_IMPL_EQUALITY_COMPARABLE_1(user_data_t, datum);

RDB_DECLARE_SERIALIZABLE(table_config_t);


RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(table_basic_config_t,
    name, database, primary_key);
RDB_IMPL_EQUALITY_COMPARABLE_3(table_basic_config_t,
    name, database, primary_key);

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_5(sindex_metaconfig_t,
    config, sindex_id, creation_task_or_nil);
RDB_IMPL_EQUALITY_COMPARABLE_3(sindex_metaconfig_t,
    config, sindex_id, creation_task_or_nil);

RDB_IMPL_SERIALIZABLE_4_SINCE_v2_5(table_config_t,
    basic, sindexes, write_hook, user_data);

RDB_IMPL_EQUALITY_COMPARABLE_4(table_config_t,
    basic, sindexes, write_hook, user_data);
