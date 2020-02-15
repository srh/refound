// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/store_metainfo.hpp"

#include "btree/keys.hpp"
#include "btree/reql_specific.hpp"
#include "clustering/immediate_consistency/version.hpp"
#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/vector_stream.hpp"
#include "rockstore/rockshard.hpp"

store_metainfo_manager_t::store_metainfo_manager_t(rockshard rocksh, real_superblock_lock *superblock) {
    std::vector<std::pair<std::vector<char>, std::vector<char> > > kv_pairs;
    // TODO: this is inefficient, cut out the middleman (vector)
    get_superblock_metainfo(rocksh, superblock, &kv_pairs);
    std::vector<key_range_t> regions;
    std::vector<version_t> values;
    for (auto &pair : kv_pairs) {
        key_range_t region;
        {
            buffer_read_stream_t key(pair.first.data(), pair.first.size());
            archive_result_t res = deserialize_for_metainfo(&key, &region);
            guarantee_deserialization(res, "region");
        }
        regions.push_back(region);
        guarantee(pair.second.size() == sizeof(version_t));
        version_t value = *reinterpret_cast<const version_t *>(pair.second.data());
        values.push_back(value);
    }
    guarantee(regions.size() == 1 && regions[0] == key_range_t::universe());
    cache = values[0];
}

version_t store_metainfo_manager_t::get(
        real_superblock_lock *superblock) const {
    guarantee(superblock != nullptr);
    superblock->read_acq_signal()->wait_lazily_ordered();
    return cache;
}

void store_metainfo_manager_t::update(
        real_superblock_lock *superblock,
        rockshard rocksh,
        const version_t &new_value) {
    // TODO: Strip out non-rocks writing (but keep superblock write acquisition waiting).
    guarantee(superblock != nullptr);
    superblock->write_acq_signal()->wait_lazily_ordered();

    cache = new_value;

    std::vector<std::vector<char> > keys;
    std::vector<version_t> values;
    {
        const version_t value = new_value;
        vector_stream_t key;
        write_message_t wm;
        serialize_for_metainfo(&wm, key_range_t::universe());
        key.reserve(wm.size());
        DEBUG_VAR int res = send_write_message(&key, &wm);
        rassert(!res);

        keys.push_back(std::move(key.vector()));
        values.push_back(value);
    }

    set_superblock_metainfo(superblock, rocksh, keys, values);
}
