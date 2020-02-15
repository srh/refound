// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_IMMEDIATE_CONSISTENCY_HISTORY_HPP_
#define CLUSTERING_IMMEDIATE_CONSISTENCY_HISTORY_HPP_

#include <map>
#include <set>
#include <stdexcept>

#include "clustering/immediate_consistency/version.hpp"
#include "concurrency/signal.hpp"
#include "containers/uuid.hpp"
#include "region/region_map.hpp"
#include "rpc/semilattice/joins/macros.hpp"
#include "rpc/serialize_macros.hpp"
#include "timestamps.hpp"

/* The state of the database at the time that the `broadcaster_t` was created and the
sequence of writes that pass through a `broadcaster_t` are collectively referred to as a
"branch". Except for the "fake branch" identified by a nil UUID, there is a 1:1
relationship between branches and `broadcaster_t`s.

Thus, the branches form a directed acyclic graph. The branch created by the first primary
"descends" from `version_t::zero()`. When a primary is created as a replacement for an
unavailable primary, its branch descends from the branch of the unavailable primary. When
shards are split or merged, the newly-created branches may descend from part of a single
existing branch or from part of several existing branches.

When a primary is created, it publishes a `branch_birth_certificate_t` that describes its
newly-created branch. */

class branch_birth_certificate_t {
public:
    /* The region that the branch applies to. This is the same as the region of the
    `broadcaster_t` that corresponds to the branch. Every write to the branch must affect
    only some (non-proper) subset of this region. */
    region_t get_region() const {
        return region_t::universe();
    }

    /* The timestamp of the first state on the branch. `version_t(branch_id,
    initial_timestamp)` describes the same B-tree state as `origin`. `initial_timestamp`
    is always equal to the maximum of the timestamps in `origin`. */
    state_timestamp_t initial_timestamp;

    /* The state of the meta-info of the B-tree when the `broadcaster_t` was
    constructed. */
    version_t origin;
};

RDB_DECLARE_SERIALIZABLE_FOR_CLUSTER(branch_birth_certificate_t);
RDB_DECLARE_EQUALITY_COMPARABLE(branch_birth_certificate_t);

/* `missing_branch_exc_t` is thrown if we try to fetch a birth certificate for a branch
that doesn't exist. This can happen if the branch history GC cleans up the branch while a
B-tree still exists that points at it, which is uncommon but not impossible. */

class missing_branch_exc_t : public std::runtime_error {
public:
    missing_branch_exc_t() :
        std::runtime_error("tried to look up a branch that is not in the history") { }
};

/* A `branch_history_t` is a map from `branch_id_t` to `branch_birth_certificate_t`.
`branch_history_reader_t` is an interface that allows reading from a `branch_history_t`
or something equivalent; this is because otherwise we might need to copy
`branch_history_t`, which may be expensive. Note that `branch_history_t` subclasses
from `branch_history_reader_t`. This is so we can pass `branch_history_t` to the many
functions that take a `branch_history_reader_t *`. */

class branch_history_t;

class branch_history_reader_t {
public:
    /* Returns information about one specific branch. Throws `missing_branch_exc_t` if we
    don't have a record for this branch. */
    virtual branch_birth_certificate_t get_branch(const branch_id_t &branch)
        const THROWS_ONLY(missing_branch_exc_t) = 0;

    /* Checks whether a given branch id is known. */
    virtual bool is_branch_known(const branch_id_t &branch) const THROWS_NOTHING = 0;

    /* Copies records related to the given branch and all its known ancestors into `out`.
    `out`. The reason this mutates `out` instead of returning a `branch_history_t` is so
    that you can call it several times with different branches that share history; they
    will re-use records that they share. */
    void export_branch_history(
        const branch_id_t &branch, branch_history_t *out) const THROWS_NOTHING;

    /* Convenience function that finds all records related to the given version
    map and copies them into `out` */
    void export_branch_history(
        const version_t &version, branch_history_t *out)
        const THROWS_NOTHING;

protected:
    virtual ~branch_history_reader_t() { }
};

class branch_history_t : public branch_history_reader_t {
public:
    branch_birth_certificate_t get_branch(const branch_id_t &branch)
        const THROWS_ONLY(missing_branch_exc_t);
    bool is_branch_known(const branch_id_t &branch) const THROWS_NOTHING;

    std::map<branch_id_t, branch_birth_certificate_t> branches;
};
RDB_DECLARE_SERIALIZABLE_FOR_CLUSTER(branch_history_t);
RDB_DECLARE_EQUALITY_COMPARABLE(branch_history_t);

/* These are the key functions that we use to do lookups in the branch history. */

/* `version_is_ancestor()` returns `true` if every key in `relevant_region` of the table
passed through `ancestor` version on the way to `descendent` version. Also returns true
if `ancestor` and `descendent` are the same version. Throws `missing_branch_exc_t` if the
question can't be answered because some history is missing. */
bool version_is_ancestor(
    const branch_history_reader_t *bh,
    const version_t &ancestor,
    const version_t &descendent,
    const region_t &relevant_region)
    THROWS_ONLY(missing_branch_exc_t);

/* `version_find_branch_common()` is like `version_find_common()` but in place of one of
the versions, it uses the latest version on the given branch. */
region_map_t<version_t> version_find_branch_common(
    const branch_history_reader_t *bh,
    const version_t &version,
    const branch_id_t &branch,
    const region_t &relevant_region)
    THROWS_ONLY(missing_branch_exc_t);

/* `branch_history_combiner_t` is a `branch_history_reader_t` that reads from two or more
other `branch_history_reader_t`s. */
class branch_history_combiner_t : public branch_history_reader_t {
public:
    branch_history_combiner_t(
        const branch_history_reader_t *_r1,
        const branch_history_reader_t *_r2)
        : r1(_r1), r2(_r2) { }
    branch_birth_certificate_t get_branch(const branch_id_t& branch)
        const THROWS_ONLY(missing_branch_exc_t);
    bool is_branch_known(const branch_id_t &branch) const THROWS_NOTHING;
private:
    const branch_history_reader_t *r1, *r2;
};

/* `branch_history_manager_t` is a `branch_history_reader_t` with the addition of methods
to add branches to the branch history. This is used for a branch history which is backed
to disk. */
class branch_history_manager_t :
    public branch_history_reader_t,
    public home_thread_mixin_t
{
public:
    /* Adds a new branch to the database. Blocks until it is safely on disk. Blocks to
    avoid a race condition where we write the branch ID to a B-tree's metainfo, crash
    before flushing the `branch_birth_certificate_t` to disk, and then cannot find the
    `branch_birth_certificate_t` upon restarting. If it already exists, this is a no-op.
    */
    virtual void create_branch(
        branch_id_t branch_id,
        const branch_birth_certificate_t &bc)
        THROWS_NOTHING = 0;

    /* Like `create_branch` but for all the records in a `branch_history_t`, atomically.
    */
    virtual void import_branch_history(
        const branch_history_t &new_records)
        THROWS_NOTHING = 0;

    /* `prepare_gc()` fills `branches` with the IDs of all known branches. `perform_gc()`
    deletes any branches whose IDs are in `remove_branches`. The standard procedure is to
    call `prepare_gc()`, delete the live branches from the set, then call `perform_gc()`.
    */
    virtual void prepare_gc(
        std::set<branch_id_t> *branches_out)
        THROWS_NOTHING = 0;
    virtual void perform_gc(
        const std::set<branch_id_t> &remove_branches)
        THROWS_NOTHING = 0;

protected:
    virtual ~branch_history_manager_t() { }
};

#endif /* CLUSTERING_IMMEDIATE_CONSISTENCY_HISTORY_HPP_ */
