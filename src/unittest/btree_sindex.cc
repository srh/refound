// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "unittest/gtest.hpp"

#include "arch/io/disk.hpp"
#include "btree/operations.hpp"
#include "btree/reql_specific.hpp"
#include "buffer_cache/alt.hpp"
#include "buffer_cache/cache_balancer.hpp"
#include "containers/uuid.hpp"
#include "unittest/unittest_utils.hpp"
#include "random.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/store.hpp"
#include "rdb_protocol/protocol.hpp"
#include "serializer/log/log_serializer.hpp"

namespace unittest {

TPTEST(BTreeSindex, LowLevelOps) {
    temp_file_t temp_file;
    temp_rockstore rocks;

    io_backender_t io_backender(rocks.rocks(), file_direct_io_mode_t::buffered_desired);
    dummy_cache_balancer_t balancer(GIGABYTE);

    filepath_file_opener_t file_opener(temp_file.name(), &io_backender);
    log_serializer_t::create(
        &file_opener,
        log_serializer_t::static_config_t());

    log_serializer_t serializer(
        log_serializer_t::dynamic_config_t(),
        &file_opener,
        &get_global_perfmon_collection());

    cache_t cache(&serializer, &balancer, &get_global_perfmon_collection());
    cache_conn_t cache_conn(&cache);
    namespace_id_t table_id = str_to_uuid("12345678-abcd-abcd-abcd-12345678abcd");

    rockshard rocksh(io_backender.rocks(), table_id, 0);

    {
        txn_t txn(&cache_conn, write_durability_t::HARD, 1);
        {
            buf_lock_t sb_lock(&txn, SUPERBLOCK_ID, alt_create_t::create);
            real_superblock_t superblock(std::move(sb_lock));
            btree_slice_t::init_real_superblock(
                &superblock, rockshard(io_backender.rocks(), table_id, 0), std::vector<char>(), binary_blob_t());
        }
        txn.commit();
    }

    order_source_t order_source;

    std::map<sindex_name_t, secondary_index_t> mirror;

    {
        scoped_ptr_t<txn_t> txn;
        {
            scoped_ptr_t<real_superblock_t> superblock;
            get_btree_superblock_and_txn_for_writing(&cache_conn, nullptr,
                write_access_t::write, 1,
                write_durability_t::SOFT,
                &superblock, &txn);

            buf_lock_t sindex_block(superblock->expose_buf(),
                superblock->get_sindex_block_id(rocksh),
                access_t::write);

            initialize_secondary_indexes(rockshard(io_backender.rocks(), table_id, 0), &sindex_block);
        }
        txn->commit();
    }

    for (int i = 0; i < 100; ++i) {
        sindex_name_t name(uuid_to_str(generate_uuid()));

        secondary_index_t s;
        s.superblock = randint(1000);

        std::string opaque_blob = rand_string(1000);
        s.opaque_definition.assign(opaque_blob.begin(), opaque_blob.end());

        mirror[name] = s;

        scoped_ptr_t<txn_t> txn;
        {
            scoped_ptr_t<real_superblock_t> superblock;
            get_btree_superblock_and_txn_for_writing(
                &cache_conn,
                nullptr,
                write_access_t::write,
                1,
                write_durability_t::SOFT,
                &superblock,
                &txn);
            buf_lock_t sindex_block(
                superblock->expose_buf(),
                superblock->get_sindex_block_id(rocksh),
                access_t::write);

            set_secondary_index(rocksh, &sindex_block, name, s);
        }
        txn->commit();
    }

    {
        scoped_ptr_t<txn_t> txn;
        {
            scoped_ptr_t<real_superblock_t> superblock;
            get_btree_superblock_and_txn_for_writing(
                &cache_conn,
                nullptr,
                write_access_t::write,
                1,
                write_durability_t::SOFT,
                &superblock,
                &txn);
            buf_lock_t sindex_block(
                superblock->expose_buf(),
                superblock->get_sindex_block_id(rocksh),
                access_t::write);

            std::map<sindex_name_t, secondary_index_t> sindexes;
            get_secondary_indexes(rocksh, &sindex_block, &sindexes);

            auto it = sindexes.begin();
            auto jt = mirror.begin();

            for (;;) {
                if (it == sindexes.end()) {
                    ASSERT_TRUE(jt == mirror.end());
                    break;
                }
                ASSERT_TRUE(jt != mirror.end());

                ASSERT_TRUE(it->first == jt->first);
                ASSERT_TRUE(it->second.superblock == jt->second.superblock &&
                    it->second.opaque_definition == jt->second.opaque_definition);
                ++it;
                ++jt;
            }
        }
        txn->commit();
    }
}

// TODO: Remove this whole test, it's now stupid.
TPTEST(BTreeSindex, BtreeStoreAPI) {
    temp_file_t temp_file;
    temp_rockstore temp_rocks;
    io_backender_t io_backender(temp_rocks.rocks(), file_direct_io_mode_t::buffered_desired);
    dummy_cache_balancer_t balancer(GIGABYTE);

    filepath_file_opener_t file_opener(temp_file.name(), &io_backender);
    log_serializer_t::create(
        &file_opener,
        log_serializer_t::static_config_t());

    log_serializer_t serializer(
        log_serializer_t::dynamic_config_t(),
        &file_opener,
        &get_global_perfmon_collection());
    namespace_id_t table_id = str_to_uuid("12345678-abcd-abcd-abcd-121234345656");
    store_t store(
            region_t::universe(),
            0,  // Just one shard.
            io_backender.rocks(),
            &serializer,
            &balancer,
            "unit_test_store",
            true,
            &get_global_perfmon_collection(),
            nullptr,
            &io_backender,
            base_path_t("."),
            table_id,
            update_sindexes_t::UPDATE);

    cond_t dummy_interruptor;

    std::set<sindex_name_t> created_sindexs;

    for (int i = 0; i < 50; ++i) {
        sindex_name_t name = sindex_name_t(uuid_to_str(generate_uuid()));
        created_sindexs.insert(name);
        optional<uuid_u> index_id;
        {
            write_token_t token;
            store.new_write_token(&token);

            scoped_ptr_t<txn_t> txn;
            {
                scoped_ptr_t<real_superblock_t> super_block;

                store.acquire_superblock_for_write(
                    1, write_durability_t::SOFT, &token,
                    &txn, &super_block, &dummy_interruptor);

                buf_lock_t sindex_block(
                    super_block->expose_buf(),
                    super_block->get_sindex_block_id(store.rocksh()),
                    access_t::write);

                index_id = store.add_sindex_internal(
                    name, std::vector<char>(), &sindex_block);
                guarantee(index_id);
            }
            txn->commit();
        }

        {
            write_token_t token;
            store.new_write_token(&token);

            scoped_ptr_t<txn_t> txn;
            {
                scoped_ptr_t<real_superblock_t> super_block;

                store.acquire_superblock_for_write(
                    1, write_durability_t::SOFT, &token,
                    &txn, &super_block, &dummy_interruptor);

                buf_lock_t sindex_block(
                    super_block->expose_buf(),
                    super_block->get_sindex_block_id(store.rocksh()),
                    access_t::write);

                store.mark_index_up_to_date(*index_id, &sindex_block, key_range_t::empty());
            }
            txn->commit();
        }

        store_key_t key(ql::datum_t::compose_secondary(
                "sec",
                store_key_t("pri"),
                r_nullopt));

        {
            //Insert a piece of data in to the btree.
            write_token_t token;
            store.new_write_token(&token);

            scoped_ptr_t<txn_t> txn;
            {
                scoped_ptr_t<real_superblock_t> super_block;

                store.acquire_superblock_for_write(
                    1, write_durability_t::SOFT,
                    &token, &txn, &super_block,
                    &dummy_interruptor);

                scoped_ptr_t<sindex_superblock_t> sindex_super_block;
                uuid_u sindex_uuid;

                bool sindex_exists = store.acquire_sindex_superblock_for_write(
                    name,
                    "",
                    super_block.get(),
                    &sindex_super_block,
                    &sindex_uuid);
                ASSERT_TRUE(sindex_exists);

                ql::datum_t data = ql::datum_t(1.0);

                point_write_response_t response;

                rdb_set_sindex_for_unittest(
                    rockshard(io_backender.rocks(), table_id, 0), sindex_uuid, key, data, true,
                    store.get_sindex_slice(sindex_uuid),
                    repli_timestamp_t::distant_past,
                    sindex_super_block.get(), &response,
                    static_cast<profile::trace_t *>(nullptr));
            }
            txn->commit();
        }

        {
            //Read that data
            read_token_t token;
            store.new_read_token(&token);

            scoped_ptr_t<txn_t> txn;
            scoped_ptr_t<real_superblock_t> main_sb;
            scoped_ptr_t<sindex_superblock_t> sindex_super_block;
            uuid_u sindex_uuid;

            store.acquire_superblock_for_read(
                    &token, &txn, &main_sb,
                    &dummy_interruptor, true);

            {
                std::vector<char> opaque_definition;
                bool sindex_exists = store.acquire_sindex_superblock_for_read(
                        name,
                        "",
                        main_sb.get(),
                        release_superblock_t::RELEASE,
                        &sindex_super_block,
                        &opaque_definition,
                        &sindex_uuid);
                ASSERT_TRUE(sindex_exists);
            }

            ql::datum_t response;

            rdb_get_secondary_for_unittest(
                store.rocksh(), sindex_uuid, key,
                sindex_super_block.get(), &response);

            ASSERT_EQ(ql::datum_t(1.0), response);
        }
    }

    for (auto it  = created_sindexs.begin(); it != created_sindexs.end(); ++it) {
        /* Drop the sindex */
        cond_t non_interruptor;
        store.sindex_drop(it->name, &non_interruptor);
    }
}

} // namespace unittest
