// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/secondary_operations.hpp"

#include "containers/archive/stl_types.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/archive/vector_stream.hpp"
#include "containers/archive/versioned.hpp"
#include "debug.hpp"
#include "protocol_api.hpp"
#include "utils.hpp"

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_4(
        sindex_reql_version_info_t, original_reql_version, latest_compatible_reql_version, latest_checked_reql_version
);

RDB_IMPL_SERIALIZABLE_4_SINCE_v2_4(
        sindex_disk_info_t, mapping, mapping_version_info, multi, geo);


