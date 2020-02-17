// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_STORE_HPP_
#define RDB_PROTOCOL_STORE_HPP_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "rdb_protocol/secondary_operations.hpp"
#include "buffer_cache/cache_account.hpp"
#include "buffer_cache/types.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/queue/disk_backed_queue_wrapper.hpp"
#include "concurrency/new_mutex.hpp"
#include "concurrency/new_semaphore.hpp"
#include "concurrency/rwlock.hpp"
#include "containers/map_sentries.hpp"
#include "containers/optional.hpp"
#include "containers/scoped.hpp"
#include "perfmon/perfmon.hpp"
#include "paths.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rockstore/store.hpp"
#include "store_view.hpp"
#include "utils.hpp"

class real_superblock_lock;
class real_superblock_lock;
class sindex_config_t;
class sindex_status_t;
class store_t;
class store_metainfo_manager_t;
class btree_slice_t;
class cache_conn_t;
class cache_t;
class internal_disk_backed_queue_t;
class io_backender_t;
class real_superblock_lock;
class txn_t;
struct rdb_modification_report_t;
namespace rockstore { class store; }

#endif  // RDB_PROTOCOL_STORE_HPP_
