// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/store.hpp"

#include "btree/reql_specific.hpp"
#include "btree/operations.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/vector_stream.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/table_common.hpp"

#if RDB_CF
void store_t::note_reshard() {
    // This is no longer for resharding, it just shuts off changefeeds for
    // local_replicator_t.  Maybe this is unnecessary; maybe we could let the destructor
    // do its work.

    // We acquire `changefeed_servers_lock` and move the matching pointer out of
    // `changefeed_servers`. We then destruct the server in a separate step,
    // after releasing the lock.
    // The reason we do this is to avoid deadlocks that could happen if someone
    // was holding a lock on the drainer in one of the changefeed servers,
    // and was at the same time trying to acquire the `changefeed_servers_lock`.
    scoped_ptr_t<ql::changefeed::server_t> to_destruct;
    {
        rwlock_acq_t acq(&the_changefeed_server_lock, access_t::write);
        ASSERT_NO_CORO_WAITING;
        if (the_changefeed_server.has()) {
            to_destruct = std::move(the_changefeed_server);
            the_changefeed_server.reset();
        }
    }
    // The changefeed server is actually getting destructed here. This might
    // block.
}
#endif


void store_t::help_construct_bring_sindexes_up_to_date() {
    // Make sure to continue bringing sindexes up-to-date if it was interrupted earlier

    // This uses a dummy interruptor because this is the only thing using the store at
    //  the moment (since we are still in the constructor), so things should complete
    //  rather quickly.
    cond_t dummy_interruptor;
    write_token_t token;
    new_write_token(&token);

    scoped_ptr_t<txn_t> txn;
    scoped_ptr_t<real_superblock_lock> superblock;
    acquire_superblock_for_write(1,
                                 write_durability_t::SOFT,
                                 &token,
                                 &txn,
                                 &superblock,
                                 &dummy_interruptor);

    superblock->sindex_block_write_signal()->wait();

    auto clear_sindex = [this](uuid_u sindex_id,
                               auto_drainer_t::lock_t store_keepalive) {
        try {
            /* Clear the sindex. */
            clear_sindex_data(
                sindex_id,
                key_range_t::universe(),
                store_keepalive.get_drain_signal());

            /* Drop the sindex, now that it's empty. */
            drop_sindex(sindex_id);
        } catch (const interrupted_exc_t &e) {
            /* Ignore */
        }
    };

    // Get the map of indexes and check if any were postconstructing or being deleted.
    // Kick off coroutines to finish the respective operations
    {
        std::map<sindex_name_t, secondary_index_t> sindexes;
        get_secondary_indexes(rocksh(), superblock.get(), &sindexes);
        for (auto it = sindexes.begin(); it != sindexes.end(); ++it) {
            if (it->second.being_deleted) {
                coro_t::spawn_sometime(std::bind(clear_sindex,
                                                 it->second.id, drainer.lock()));
            } else if (!it->second.post_construction_complete()) {
                // TODO: rocks read operations in resume_construct_sindex.
                coro_t::spawn_sometime(std::bind(&rdb_protocol::resume_construct_sindex,
                                                 it->second.id,
                                                 it->second.needs_post_construction_range,
                                                 this,
                                                 drainer.lock()));
            }
        }
    }

    txn->commit(rocks, std::move(superblock));
}

#if RDB_CF
void acquire_sindex_for_read(
    store_t *store,
    real_superblock_lock *superblock,
    const std::string &table_name,
    const std::string &sindex_id,
    sindex_disk_info_t *sindex_info_out,
    uuid_u *sindex_uuid_out) {
    rassert(sindex_info_out != NULL);
    rassert(sindex_uuid_out != NULL);

    std::vector<char> sindex_mapping_data;

    sindex_disk_info_t sindex_info;
    uuid_u sindex_uuid;
    try {
        bool found = store->acquire_sindex_superblock_for_read(
            sindex_name_t(sindex_id),
            table_name,
            superblock,
            &sindex_info,
            &sindex_uuid);
        // TODO: consider adding some logic on the machine handling the
        // query to attach a real backtrace here.
        rcheck_toplevel(found, ql::base_exc_t::OP_FAILED,
                strprintf("Index `%s` was not found on table `%s`.",
                          sindex_id.c_str(), table_name.c_str()));
    } catch (const sindex_not_ready_exc_t &e) {
        throw ql::exc_t(
            ql::base_exc_t::OP_FAILED, e.what(), ql::backtrace_id_t::empty());
    }

    *sindex_info_out = sindex_info;
    *sindex_uuid_out = sindex_uuid;
}

// TODO: Remove this?
void do_read_for_changefeed(rockshard rocksh,
             ql::env_t *env,
             store_t *store,
             btree_slice_t *btree,
             real_superblock_lock *superblock,
             const rget_read_t &rget,
             rget_read_response_t *res,
             optional<uuid_u> *sindex_id_out) {
    if (!rget.sindex.has_value()) {
        // rget using a primary index
        *sindex_id_out = r_nullopt;
        rdb_rget_slice(
            rocksh,
            btree,
            rget.region,
            rget.primary_keys,
            superblock,
            env,
            rget.batchspec,
            rget.transforms,
            rget.terminal,
            rget.sorting,
            res,
            release_superblock_t::KEEP);
    } else {
        // rget using a secondary index
        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        key_range_t sindex_range;
        try {
            acquire_sindex_for_read(
                    store,
                    superblock,
                    rget.table_name,
                    rget.sindex->id,
                    &sindex_info,
                    &sindex_uuid);
            *sindex_id_out = make_optional(sindex_uuid);
            if (rget.sindex->region.has_value()) {
                sindex_range = *rget.sindex->region;
            } else {
                sindex_range =
                    rget.sindex->datumspec.covering_range().to_sindex_keyrange();
            }
            if (sindex_info.geo == sindex_geo_bool_t::GEO) {
                res->result = ql::exc_t(
                    ql::base_exc_t::LOGIC,
                    strprintf(
                        "Index `%s` is a geospatial index.  Only get_nearest and "
                        "get_intersecting can use a geospatial index.",
                        rget.sindex->id.c_str()),
                    ql::backtrace_id_t::empty());
                return;
            }

            rdb_rget_secondary_slice(
                rocksh,
                sindex_uuid,
                store->get_sindex_slice(sindex_uuid),
                rget.sindex->datumspec,
                sindex_range,
                superblock,
                env,
                rget.batchspec,
                rget.transforms,
                rget.terminal,
                rget.region,
                rget.sorting,
                rget.sindex->require_sindex_val,
                sindex_info,
                res,
                release_superblock_t::KEEP);
        } catch (const ql::exc_t &e) {
            res->result = e;
            return;
        } catch (const ql::datum_exc_t &e) {
            // TODO: consider adding some logic on the machine handling the
            // query to attach a real backtrace here.
            res->result = ql::exc_t(e, ql::backtrace_id_t::empty());
            return;
        }
    }
}
#endif  // RDB_CF

void store_t::delayed_clear_and_drop_sindex(
        secondary_index_t sindex,
        auto_drainer_t::lock_t store_keepalive)
        THROWS_NOTHING {
    try {
        /* Clear the sindex */
        clear_sindex_data(sindex.id,
                          key_range_t::universe(),
                          store_keepalive.get_drain_signal());

        /* Drop the sindex, now that it's empty. */
        drop_sindex(sindex.id);
    } catch (const interrupted_exc_t &e) {
        /* Ignore. The sindex deletion will continue when the store
        is next started up. */
    }
}

namespace_id_t const &store_t::get_table_id() const {
    return table_id;
}

store_t::sindex_context_map_t *store_t::get_sindex_context_map() {
    return &sindex_context;
}

// TODO: Cleanup this fluff.
#if RDB_CF
std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const rwlock_acq_t *acq) {
    acq->guarantee_is_holding(&the_changefeed_server_lock);
    if (the_changefeed_server.has()) {
        return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<const scoped_ptr_t<ql::changefeed::server_t> *,
          scoped_ptr_t<rwlock_acq_t> > store_t::access_the_changefeed_server() {
    return std::make_pair(&the_changefeed_server,
                          make_scoped<rwlock_acq_t>(&the_changefeed_server_lock,
                                                    access_t::read));
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const region_t &) {
    // TODO: What is region?  Region is unused.
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::read);
    return changefeed_server(&acq);
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const store_key_t &) {
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::read);
    if (the_changefeed_server.has()) {
        return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>
        store_t::get_or_make_changefeed_server() {
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::write);
    guarantee(ctx != nullptr);
    guarantee(ctx->manager != nullptr);
    auto existing = changefeed_server(&acq);
    if (existing.first != nullptr) {
        return existing;
    }
    the_changefeed_server =
            make_scoped<ql::changefeed::server_t>(ctx->manager, this);
    return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
}
#endif  // RDB_CF
