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
#include "rdb_protocol/store.hpp"

namespace rocksdb { class Snapshot; }

class btree_slice_t;
enum class delete_mode_t;
template <class> class promise_t;
struct sindex_disk_info_t;

struct rdb_modification_info_t;
struct rdb_modification_report_t;
class rdb_modification_report_cb_t;

#if RDB_CF
void rdb_get(
    rockshard rocksh,
    const store_key_t &store_key,
    real_superblock_lock *superblock,
    point_read_response_t *response);
#endif  // RDB_CF

struct btree_batched_replacer_t {
    virtual ~btree_batched_replacer_t() { }
    virtual ql::datum_t replace(
        const ql::datum_t &d, size_t index) const = 0;
    virtual return_changes_t should_return_changes() const = 0;

    ql::datum_t apply_write_hook(
        const datum_string_t &pkey,
        const ql::datum_t &d,
        const ql::datum_t &res_,
        const ql::datum_t &write_timestamp,
        const counted_t<const ql::func_t> &write_hook) const;
};

#if RDB_CF
// TODO: So much duplication, remove this?
void rdb_rget_slice(
    rockshard rocksh,
    btree_slice_t *slice,
    const key_range_t &range,
    const optional<std::map<store_key_t, uint64_t> > &primary_keys,
    real_superblock_lock *superblock,
    ql::env_t *ql_env,
    const ql::batchspec_t &batchspec,
    const std::vector<ql::transform_variant_t> &transforms,
    const optional<ql::terminal_variant_t> &terminal,
    sorting_t sorting,
    rget_read_response_t *response,
    release_superblock_t release_superblock);

// TODO: So much duplication, remove this?
void rdb_rget_secondary_slice(
    rockshard rocksh,
    uuid_u sindex_uuid,
    btree_slice_t *slice,
    const ql::datumspec_t &datumspec,
    const key_range_t &sindex_range,
    real_superblock_lock *superblock,
    ql::env_t *ql_env,
    const ql::batchspec_t &batchspec,
    const std::vector<ql::transform_variant_t> &transforms,
    const optional<ql::terminal_variant_t> &terminal,
    const key_range_t &pk_range,
    sorting_t sorting,
    require_sindexes_t require_sindex_val,
    const sindex_disk_info_t &sindex_info,
    rget_read_response_t *response,
    release_superblock_t release_superblock);
#endif// RDB_CF

/* Secondary Indexes */

struct rdb_modification_info_t {
    // TODO: Remove(?) data_pair_t wrapper (after figuring out
    // serialization/non-serialization needs).
    struct data_pair_t { ql::datum_t first; };
    data_pair_t deleted;
    data_pair_t added;
};

RDB_DECLARE_SERIALIZABLE(rdb_modification_info_t);

struct rdb_modification_report_t {
    rdb_modification_report_t() { }
    explicit rdb_modification_report_t(const store_key_t &_primary_key)
        : primary_key(_primary_key) { }

    store_key_t primary_key;
    rdb_modification_info_t info;
};

RDB_DECLARE_SERIALIZABLE(rdb_modification_report_t);

void post_construct_secondary_index_range(
        store_t *store,
        const std::set<uuid_u> &sindexes_to_post_construct,
        key_range_t *construction_range_inout,
        const std::function<bool(int64_t)> &check_should_abort,
        const signal_t *interruptor)
    THROWS_ONLY(interrupted_exc_t);

// Exposed now for fdb.
void compute_keys(const store_key_t &primary_key,
                  ql::datum_t doc,
                  const sindex_disk_info_t &index_info,
                  std::vector<std::pair<store_key_t, ql::datum_t> > *keys_out,
                  std::vector<index_pair_t> *cfeed_keys_out);

ql::serialization_result_t datum_serialize_to_string(const ql::datum_t &datum, std::string *out);

#endif /* RDB_PROTOCOL_BTREE_HPP_ */
