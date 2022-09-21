// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_BTREE_HPP_
#define RDB_PROTOCOL_BTREE_HPP_

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "btree/types.hpp"
#include "concurrency/auto_drainer.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/protocol.hpp"

struct sindex_disk_info_t;

struct rdb_modification_info_t;

struct btree_batched_replacer_t {
    virtual ~btree_batched_replacer_t() { }
    virtual ql::datum_t replace(
        const ql::datum_t &d, size_t index) const = 0;
    virtual return_changes_t should_return_changes() const = 0;

    static ql::datum_t apply_write_hook(
        const datum_string_t &pkey,
        const ql::datum_t &d,
        const ql::datum_t &res_,
        const ql::datum_t &write_timestamp,
        const counted_t<const ql::func_t> &write_hook);
};

/* Secondary Indexes */

struct rdb_modification_info_t {
    ql::datum_t deleted;
    ql::datum_t added;

    bool has_any() const { return deleted.has() || added.has(); }
};

// Exposed now for fdb.
void compute_keys(const store_key_t &primary_key,
                  ql::datum_t doc,
                  const sindex_disk_info_t &index_info,
                  std::vector<store_key_t> *keys_out,
                  std::vector<index_pair_t> *cfeed_keys_out);

ql::serialization_result_t datum_serialize_to_string(const ql::datum_t &datum, std::string *out);

#endif /* RDB_PROTOCOL_BTREE_HPP_ */
