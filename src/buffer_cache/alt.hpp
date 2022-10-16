// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BUFFER_CACHE_ALT_HPP_
#define BUFFER_CACHE_ALT_HPP_

#include <map>
#include <vector>
#include <utility>
#include <unordered_map>

#include "rocksdb/utilities/write_batch_with_index.h"

#include "buffer_cache/page_cache.hpp"
#include "buffer_cache/types.hpp"
#include "concurrency/rwlock.hpp"
#include "concurrency/new_semaphore.hpp"
#include "containers/scoped.hpp"
#include "containers/two_level_array.hpp"
#include "containers/uuid.hpp"
#include "repli_timestamp.hpp"

class perfmon_collection_t;
namespace rockstore { class store; }
class real_superblock_lock;

// TODO: Throttling has to be done globally.  But beware of deadlock.
class alt_txn_throttler_t {
public:
    explicit alt_txn_throttler_t(int64_t minimum_unwritten_changes_limit);
    ~alt_txn_throttler_t();

    alt::throttler_acq_t begin_txn_or_throttle(int64_t expected_change_count);

    void inform_memory_limit_change(uint64_t memory_limit);

private:
    const int64_t minimum_unwritten_changes_limit_;

    new_semaphore_t unwritten_block_changes_semaphore_;
    new_semaphore_t unwritten_index_changes_semaphore_;

    DISABLE_COPYING(alt_txn_throttler_t);
};

class lock_state {
public:
    rwlock_t real_superblock_lock;
};

class cache_t : public home_thread_mixin_t {
public:
    explicit cache_t(perfmon_collection_t *perfmon_collection);
    ~cache_t();

    // These todos come from the mirrored cache.  The real problem is that whole
    // cache account / priority thing is just one ghetto hack amidst a dozen other
    // throttling systems.  TODO: Come up with a consistent priority scheme,
    // i.e. define a "default" priority etc.  TODO: As soon as we can support it, we
    // might consider supporting a mem_cap paremeter.
    cache_account_t create_cache_account(int priority);

private:
    friend class txn_t;
    friend class real_superblock_lock;

    // throttler_ can cause the txn_t constructor to block
    alt_txn_throttler_t throttler_;

    lock_state locks_;

    DISABLE_COPYING(cache_t);
};

class txn_t {
public:
    // Constructor for read-only transactions.
    txn_t(cache_conn_t *cache_conn, read_access_t read_access);

    txn_t(cache_conn_t *cache_conn,
          write_durability_t durability,
          int64_t expected_change_count);

    ~txn_t();

    // Every write transaction must be committed before it's
    // destructed.
    // There is no roll-back / abort! Destructing an uncommitted
    // write-transaction will terminate the server.
    void commit(rockstore::store *rocks, scoped_ptr_t<real_superblock_lock> superblock);

    cache_t *cache() { return cache_; }
    access_t access() const { return access_; }

    void set_account(cache_account_t *cache_account);
    cache_account_t *account() { return cache_account_; }

    // TODO: Add status checks to every usage.
    // TODO: It would be nice to just avoid duplicate secondary index writing, so
    // we can use a plain WriteBatch.
    rocksdb::WriteBatchWithIndex batch;

private:
    void help_construct(int64_t expected_change_count, cache_conn_t *cache_conn);

    cache_t *const cache_;

    // Initialized to cache()->page_cache_.default_cache_account(), and modified by
    // set_account().
    cache_account_t *cache_account_;

    // TODO: Make this not be scoped_ptr_t?
    scoped_ptr_t<alt::throttler_acq_t> throttler_acq_;

    const access_t access_;

    // Only applicable if access_ == write.
    const write_durability_t durability_;

    bool is_committed_;

    DISABLE_COPYING(txn_t);
};

// TODO: Public/private properly.
// TODO: Move impls to .cc file.

// TODO: Remove.
inline void wait_for_rwlock(rwlock_in_line_t *acq, access_t access) {
    if (access == access_t::read) {
        acq->read_signal()->wait_lazily_ordered();
    } else {
        acq->write_signal()->wait_lazily_ordered();
    }
}

class real_superblock_lock {
public:
    real_superblock_lock() = delete;
    DISABLE_COPYING(real_superblock_lock);
    explicit real_superblock_lock(
            txn_t *txn, access_t access, new_semaphore_in_line_t &&write_semaphore_acq)
        : txn_(txn),
          access_(access),
          write_semaphore_acq_(std::move(write_semaphore_acq)),
          acq_(&txn->cache()->locks_.real_superblock_lock, access) {}

    txn_t *txn() { return txn_; }
    access_t access() const { return access_; }

    const signal_t *read_acq_signal() { return acq_.read_signal(); }
    const signal_t *write_acq_signal() { return acq_.write_signal(); }
    // Calls added where we used to construct an sindex_block, in order to
    // cautiously preserve locking behavior.
    const signal_t *sindex_block_read_signal() { return acq_.read_signal(); }
    const signal_t *sindex_block_write_signal() { return acq_.write_signal(); }

    // Calls added where we used to construct an sindex_superblock_lock, in order to
    // describe previous locking behavior
    const signal_t *sindex_superblock_read_signal() { return acq_.read_signal(); }
    const signal_t *sindex_superblock_write_signal() { return acq_.write_signal(); }

    void reset_superblock() {
        txn_ = nullptr;
        acq_.reset();
        write_semaphore_acq_.reset();
    }

    txn_t *txn_;
    access_t access_;
    // The write_semaphore_acq_ is empty for reads.  For writes it locks the
    // write superblock acquisition semaphore until acq_ is released.  Note that
    // this is used to throttle writes compared to reads, but not required for
    // correctness.
    // TODO: We should (with rocks) always let reads jump the line ahead of writes?
    new_semaphore_in_line_t write_semaphore_acq_;
    rwlock_in_line_t acq_;
};


#endif  // BUFFER_CACHE_ALT_HPP_
