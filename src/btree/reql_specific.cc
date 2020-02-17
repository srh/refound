// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "btree/reql_specific.hpp"

#include "rocksdb/write_batch.h"

#include "rdb_protocol/secondary_operations.hpp"
#include "clustering/immediate_consistency/version.hpp"


btree_slice_t::btree_slice_t(cache_t *c, perfmon_collection_t *parent,
                             const std::string &identifier,
                             index_type_t index_type)
    : stats(parent,
            (index_type == index_type_t::SECONDARY ? "index-" : "") + identifier),
      cache_(c) { }

btree_slice_t::~btree_slice_t() { }
