// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_STORE_METAINFO_HPP_
#define RDB_PROTOCOL_STORE_METAINFO_HPP_

#include <functional>

#include "clustering/immediate_consistency/version.hpp"
#include "containers/uuid.hpp"

class real_superblock_lock;
class rockshard;

class store_metainfo_manager_t {
public:
    explicit store_metainfo_manager_t(rockshard rocksh, real_superblock_lock *superblock);

    version_t get(
        real_superblock_lock *superblock) const;

    void update(
        real_superblock_lock *superblock,
        rockshard rocksh,
        const version_t &new_metainfo);

private:
    version_t cache;
};

#endif /* RDB_PROTOCOL_STORE_METAINFO_HPP_ */

