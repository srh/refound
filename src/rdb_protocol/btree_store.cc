// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/store.hpp"  // NOLINT(build/include_order)

#include <functional>  // NOLINT(build/include_order)

#include "arch/runtime/coroutines.hpp"
#include "btree/concurrent_traversal.hpp"
#include "btree/operations.hpp"
#include "btree/reql_specific.hpp"
#include "buffer_cache/page_cache.hpp"
#include "rdb_protocol/secondary_operations.hpp"
#include "buffer_cache/alt.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/vector_stream.hpp"
#include "containers/archive/versioned.hpp"
#include "containers/disk_backed_queue.hpp"
#include "containers/scoped.hpp"
#include "logger.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rdb_protocol/store_metainfo.hpp"
#include "stl_utils.hpp"

sindex_name_t compute_sindex_deletion_name(uuid_u sindex_uuid) {
    sindex_name_t result("_DEL_" + uuid_to_str(sindex_uuid));
    result.being_deleted = true;
    return result;
}
