// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/servers/server_metadata.hpp"

#include "containers/archive/stl_types.hpp"
#include "logger.hpp"

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(server_config_t, name, tags, cache_size_bytes);
RDB_IMPL_EQUALITY_COMPARABLE_3(server_config_t, name, tags, cache_size_bytes);

RDB_IMPL_SERIALIZABLE_2_SINCE_v2_1(server_config_versioned_t, config, version);
RDB_IMPL_EQUALITY_COMPARABLE_2(server_config_versioned_t, config, version);

RDB_IMPL_SERIALIZABLE_1(server_config_business_card_t, set_config_addr);
INSTANTIATE_SERIALIZABLE_FOR_CLUSTER(server_config_business_card_t);

