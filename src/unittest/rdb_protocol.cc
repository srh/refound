// Copyright 2010-2015 RethinkDB, all rights reserved.
#include <functional>
#include <vector>

#include "arch/io/disk.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "extproc/extproc_pool.hpp"
#include "extproc/extproc_spawner.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/datum_json.hpp"
#include "rdb_protocol/datum_stream/vector.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/protocol.hpp"
#include "stl_utils.hpp"
#include "unittest/dummy_namespace_interface.hpp"
#include "unittest/gtest.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

#if RDB_FDB_UNITTEST

void run_with_namespace_interface(
        std::function<void(
            namespace_interface_t *,
            store_t *
            )> fun,
        bool oversharding,
        int num_restarts) {
    /* Pick shards */
    std::vector<region_t> store_shards;
    if (oversharding) {
        store_shards.push_back(region_t::universe());
    } else {
        store_shards.push_back(region_t(key_range_t(key_range_t::none, store_key_t(), key_range_t::open, store_key_t("n"))));
        store_shards.push_back(region_t(key_range_t(key_range_t::closed, store_key_t("n"), key_range_t::none, store_key_t() )));
    }

    temp_file_t temp_file;

    temp_rockstore temp_rocks;
    io_backender_t io_backender(temp_rocks.rocks(), file_direct_io_mode_t::buffered_desired);

    extproc_pool_t extproc_pool(2);
    dummy_semilattice_controller_t<auth_semilattice_metadata_t> auth_manager;
    rdb_context_t ctx(TODO_fdb(), &extproc_pool, nullptr, auth_manager.get_view());
    namespace_id_t table_id = namespace_id_t{
        str_to_uuid("12345678-aaaa-bbbb-cccc-12345678abcd")
    };

    for (int rep = 0; rep < num_restarts; ++rep) {
        const bool do_create = rep == 0;
        scoped_ptr_t<store_t> underlying_store =
                make_scoped<store_t>(io_backender.rocks(),
                    temp_file.name().permanent_path().c_str(), do_create,
                    &get_global_perfmon_collection(), &ctx, &io_backender,
                    base_path_t("."), table_id, update_sindexes_t::UPDATE);

        // These vectors are for shards; there is just one shard now.
        std::vector<store_view_t *> store_ptrs;

        /* Set up namespace interface */
        dummy_namespace_interface_t nsi(underlying_store.get(),
                                        do_create);

        fun(&nsi, underlying_store.get());
    }
}

void run_in_thread_pool_with_namespace_interface(
        std::function<void(
            namespace_interface_t *,
            store_t *)> fun,
        bool oversharded,
        int num_restarts = 1) {
    extproc_spawner_t extproc_spawner;
    unittest::run_in_thread_pool(std::bind(&run_with_namespace_interface,
                                           fun,
                                           oversharded,
                                           num_restarts));
}

/* `SetupTeardown` makes sure that it can start and stop without anything going
horribly wrong */
void run_setup_teardown_test(
        namespace_interface_t *,
        const store_t *) {
    /* Do nothing */
}
TEST(RDBProtocol, SetupTeardown) {
    run_in_thread_pool_with_namespace_interface(&run_setup_teardown_test, false);
}

TEST(RDBProtocol, OvershardedSetupTeardown) {
    run_in_thread_pool_with_namespace_interface(&run_setup_teardown_test, true);
}

/* `GetSet` tests basic get and set operations */
void run_get_set_test(
        namespace_interface_t *nsi,
        store_t *) {
    {
        write_t write(
                point_write_t(store_key_t("a"), ql::datum_t::null()),
                durability_requirement_t::DEFAULT,
                profile_bool_t::PROFILE,
                ql::configured_limits_t());
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        if (point_write_response_t *maybe_point_write_response_t = boost::get<point_write_response_t>(&response.response)) {
            ASSERT_EQ(maybe_point_write_response_t->result, point_write_result_t::STORED);
        } else {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    {
        read_t read(point_read_t(store_key_t("a")),
                    profile_bool_t::PROFILE, read_mode_t::SINGLE);
        read_response_t response;

        cond_t interruptor;
        nsi->read(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
            read,
            &response,
            &interruptor);

        if (point_read_response_t *maybe_point_read_response = boost::get<point_read_response_t>(&response.response)) {
            ASSERT_TRUE(maybe_point_read_response->data.has());
            ASSERT_EQ(ql::datum_t(ql::datum_t::construct_null_t()),
                      maybe_point_read_response->data);
        } else {
            ADD_FAILURE() << "got wrong result back";
        }
    }
}

TEST(RDBProtocol, GetSet) {
    run_in_thread_pool_with_namespace_interface(&run_get_set_test, false);
}

TEST(RDBProtocol, OvershardedGetSet) {
    run_in_thread_pool_with_namespace_interface(&run_get_set_test, true);
}

void wait_for_sindex(
        store_t *store,
        const std::string &id) {
    cond_t non_interruptor;
    for (int attempts = 0; attempts < 50; ++attempts) {
        bool all_ok = true;
        std::map<std::string, std::pair<sindex_config_t, sindex_status_t> > res =
            store->sindex_list(&non_interruptor);
        auto it = res.find(id);
        if (it == res.end() || !it->second.second.ready) {
            all_ok = false;
            continue;
        }
        if (all_ok) {
            return;
        }
        nap((attempts + 1) * 50);
    }
    ADD_FAILURE() << "Waiting for sindex " << id << " timed out.";
}

void drop_sindex(store_t *store,
                 const std::string &id) {
    cond_t non_interruptor;
    store->sindex_drop(id, &non_interruptor);
}

void run_create_drop_sindex_test(
        namespace_interface_t *nsi,
        store_t *store) {
    /* Create a secondary index. */
    std::string id = create_sindex(store).name;
    wait_for_sindex(store, id);

    rapidjson::Document data;
    data.Parse("{\"id\" : 0, \"sid\" : 1}");
    ASSERT_FALSE(data.HasParseError());
    ql::configured_limits_t limits;
    ql::datum_t d
        = ql::to_datum(data.FindMember("id")->value, limits, reql_version_t::LATEST);
    store_key_t pk = store_key_t(d.print_primary());
    ql::datum_t sindex_key_literal = ql::datum_t(1.0);

    {
        /* Insert a piece of data (it will be indexed using the secondary
         * index). */
        write_t write(
            point_write_t(pk, ql::to_datum(data, limits, reql_version_t::LATEST)),
            durability_requirement_t::DEFAULT,
            profile_bool_t::PROFILE,
            ql::configured_limits_t());
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        if (point_write_response_t *maybe_point_write_response
            = boost::get<point_write_response_t>(&response.response)) {
            ASSERT_EQ(maybe_point_write_response->result, point_write_result_t::STORED);
        } else {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    {
        /* Access the data using the secondary index. */
        read_t read = make_sindex_read(sindex_key_literal, id);
        read_response_t response;

        cond_t interruptor;
        nsi->read(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
            read,
            &response,
            &interruptor);

        if (rget_read_response_t *rget_resp = boost::get<rget_read_response_t>(&response.response)) {
            auto streams = boost::get<ql::grouped_t<ql::stream_t> >(
                &rget_resp->result);
            ASSERT_TRUE(streams != nullptr);
            ASSERT_EQ(1, streams->size());
            // Order doesn't matter because streams->size() is 1.
            auto stream = &streams->begin()->second;
            ASSERT_TRUE(stream != nullptr);
            ASSERT_EQ(1u, stream->substreams.size());
            ASSERT_EQ(ql::to_datum(data, limits, reql_version_t::LATEST),
                      stream->substreams.begin()->second.stream.at(0).data);
        } else {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    {
        /* Delete the data. */
        point_delete_t del(pk);
        write_t write(del, durability_requirement_t::DEFAULT, profile_bool_t::PROFILE,
                      ql::configured_limits_t());
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        if (point_delete_response_t *maybe_point_delete_response = boost::get<point_delete_response_t>(&response.response)) {
            ASSERT_EQ(maybe_point_delete_response->result, point_delete_result_t::DELETED);
        } else {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    {
        /* Access the data using the secondary index. */
        read_t read = make_sindex_read(sindex_key_literal, id);
        read_response_t response;

        cond_t interruptor;
        nsi->read(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
            read,
            &response,
            &interruptor);

        if (rget_read_response_t *rget_resp = boost::get<rget_read_response_t>(&response.response)) {
            auto streams = boost::get<ql::grouped_t<ql::stream_t> >(
                &rget_resp->result);
            ASSERT_TRUE(streams != nullptr);
            ASSERT_EQ(0, streams->size());
        } else {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    drop_sindex(store, id);
}

void populate_sindex(namespace_interface_t *nsi,
                     int num_docs) {
    for (int i = 0; i < num_docs; ++i) {
        std::string json_doc = strprintf("{\"id\" : %d, \"sid\" : %d}", i, i % 4);
        rapidjson::Document data;
        data.Parse(json_doc.c_str());
        ASSERT_FALSE(data.HasParseError());
        ql::configured_limits_t limits;
        ql::datum_t d
            = ql::to_datum(data.FindMember("id")->value, limits,
                           reql_version_t::LATEST);
        store_key_t pk = store_key_t(d.print_primary());

        /* Insert a piece of data (it will be indexed using the secondary
         * index). */
        write_t write(point_write_t(pk, ql::to_datum(data, limits,
                                                     reql_version_t::LATEST)),
                      durability_requirement_t::SOFT, profile_bool_t::PROFILE, limits);
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        /* The result can be either STORED or DUPLICATE (in case this
         * test has been run before on the same store). Either is fine.*/
        if (!boost::get<point_write_response_t>(&response.response)) {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }
}

/* Randomly inserts and drops documents */
void fuzz_sindex(namespace_interface_t *nsi,
                 int goal_size,
                 auto_drainer_t::lock_t drainer_lock) {
    // We assume that about half of the ids up to goal_size * 2 will be deleted at any
    // time.
    std::vector<bool> doc_exists(goal_size * 2, false);
    while (!drainer_lock.get_drain_signal()->is_pulsed()) {
        int id = randint(goal_size * 2);
        write_t write;
        ql::configured_limits_t limits;
        if (randint(2) == 0) {
            // Insert
            if (doc_exists[id]) {
                continue;
            }

            std::string json_doc = strprintf("{\"id\" : %d, \"sid\" : %d}",
                                             id, randint(goal_size));
            rapidjson::Document data;
            data.Parse(json_doc.c_str());
            ASSERT_FALSE(data.HasParseError());
            ql::datum_t d(static_cast<double>(id));
            store_key_t pk = store_key_t(d.print_primary());

            write = write_t(point_write_t(pk, ql::to_datum(data, limits,
                                                         reql_version_t::LATEST)),
                            durability_requirement_t::SOFT, profile_bool_t::PROFILE, limits);
            doc_exists[id] = true;
        } else {
            // Delete
            if (!doc_exists[id]) {
                continue;
            }

            ql::datum_t d(static_cast<double>(id));
            store_key_t pk = store_key_t(d.print_primary());

            write = write_t(point_delete_t(pk),
                            durability_requirement_t::SOFT, profile_bool_t::PROFILE, limits);
            doc_exists[id] = false;
        }
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        /* The result can be either STORED or DUPLICATE (in case this
         * test has been run before on the same store). Either is fine.*/
        if (!boost::get<point_write_response_t>(&response.response)
            && !boost::get<point_delete_response_t>(&response.response)) {
            ADD_FAILURE() << "got wrong type of result back";
        }

        nap(2);
    }
}

void run_fuzz_create_drop_sindex(
        namespace_interface_t *nsi,
        store_t *store) {
    const int num_docs = 500;

    /* Start fuzzing the table. */

    auto_drainer_t fuzzing_drainer;
    /* spawn_dangerously_now so that we can capture the fuzzing_drainer inside the
    lambda.*/
    coro_t::spawn_now_dangerously([&]() {
        fuzz_sindex(nsi, num_docs, fuzzing_drainer.lock());
    });

    /* Let some time pass to allow `fuzz_sindex` to populate the table. */
    nap(2 * num_docs);

    /* Create a secondary index (this will post construct the index while under load). */
    std::string id = create_sindex(store).name;
    wait_for_sindex(store, id);

    /* Drop the index in the middle of fuzzing to test proper interruption under load. */
    drop_sindex(store, id);

    /* Fuzzing is stopped here by draining `fuzzing_drainer`. */
}

TEST(RDBProtocol, SindexFuzzCreateDrop) {
    // Run the test 3 times (on the same data file)
    run_in_thread_pool_with_namespace_interface(&run_fuzz_create_drop_sindex, false, 3);
}

void run_create_drop_sindex_with_data_test(
        namespace_interface_t *nsi,
        store_t *store,
        int num_docs) {
    /* Create a secondary index. */
    std::string id = create_sindex(store).name;
    wait_for_sindex(store, id);
    populate_sindex(nsi, num_docs);
    drop_sindex(store, id);
}

void run_repeated_sindex_test(
        namespace_interface_t *nsi,
        store_t *store,
        void (*fn)(
            namespace_interface_t *,
            store_t *,
            int)
        ) {
    // Run the test with just a few documents in it
    fn(nsi, store, 128);
    // ... and just for fun sometimes do it a second time immediately after.
    if (randint(4) == 0) {
        fn(nsi, store, 128);
    }

    // Nap for a random time before we shut down the namespace interface
    // (in 3 out of 4 cases).
    if (randint(4) != 0) {
        nap(randint(200));
    }
}

TEST(RDBProtocol, SindexCreateDrop) {
    run_in_thread_pool_with_namespace_interface(&run_create_drop_sindex_test, false);
}

TEST(RDBProtocol, SindexRepeatedCreateDrop) {
    // Repeat the test 10 times on the same data files.
    run_in_thread_pool_with_namespace_interface(
        std::bind(&run_repeated_sindex_test,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3,
                  &run_create_drop_sindex_with_data_test),
        false,
        10);
}

TEST(RDBProtocol, OvershardedSindexCreateDrop) {
    run_in_thread_pool_with_namespace_interface(&run_create_drop_sindex_test, true);
}

void rename_sindex(store_t *store,
                   std::string old_name,
                   std::string new_name) {
    std::map<std::string, std::string> renames{ {old_name, new_name} };
    cond_t non_interruptor;
    store->sindex_rename_multi(renames, &non_interruptor);
}

void read_sindex(namespace_interface_t *nsi,
                 double value,
                 std::string index,
                 size_t expected_size) {
    /* Access the data using the secondary index. */
    ql::datum_t key_literal = ql::datum_t(value);
    read_t read = make_sindex_read(key_literal, index);
    read_response_t response;

    cond_t interruptor;
    nsi->read(
        auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
        read,
        &response,
        &interruptor);

    if (rget_read_response_t *rget_resp = boost::get<rget_read_response_t>(&response.response)) {
        auto streams = boost::get<ql::grouped_t<ql::stream_t> >(
            &rget_resp->result);
        ASSERT_TRUE(streams != nullptr);
        ASSERT_EQ(1, streams->size());
        // Order doesn't matter because streams->size() is 1.
        ql::stream_t *stream = &streams->begin()->second;
        ASSERT_TRUE(stream != nullptr);
        ASSERT_EQ(1ul, stream->substreams.size());
        ASSERT_EQ(expected_size, stream->substreams.begin()->second.stream.size());
    } else {
        ADD_FAILURE() << "got wrong type of result back";
    }
}

void run_rename_sindex_test(namespace_interface_t *nsi,
                            store_t *store,
                            int num_rows) {
    bool sindex_before_data = randint(2) == 0;
    std::string id1;
    std::string id2;

    if (sindex_before_data) {
        id1 = create_sindex(store).name;
        id2 = create_sindex(store).name;
    }

    populate_sindex(nsi, num_rows);

    if (!sindex_before_data) {
        id1 = create_sindex(store).name;
        id2 = create_sindex(store).name;
    }

    wait_for_sindex(store, id1);
    wait_for_sindex(store, id2);

    rename_sindex(store, id1, id2);
    rename_sindex(store, id2, "fake");
    rename_sindex(store, "fake", "last");

    // At this point the only sindex should be 'last'
    // Perform a read to make sure it survived all the moves
    read_sindex(nsi, 3.0, "last", num_rows / 4);

    drop_sindex(store, "last");
}

TEST(RDBProtocol, SindexRename) {
    run_in_thread_pool_with_namespace_interface(
        std::bind(&run_rename_sindex_test,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3,
                  64),
        false);
}

TEST(RDBProtocol, OvershardedSindexRename) {
    run_in_thread_pool_with_namespace_interface(
        std::bind(&run_rename_sindex_test,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3,
                  64),
        true);
}

TEST(RDBProtocol, SindexRepeatedRename) {
    // Repeat the test 10 times on the same data files.
    run_in_thread_pool_with_namespace_interface(
        std::bind(&run_repeated_sindex_test,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3,
                  &run_rename_sindex_test),
        false,
        10);
}

void check_sindexes(
        store_t *store,
        const std::set<std::string> &expect) {
    cond_t non_interruptor;
    std::map<std::string, std::pair<sindex_config_t, sindex_status_t> > res =
        store->sindex_list(&non_interruptor);
    for (const std::string &name : expect) {
        EXPECT_EQ(1, res.count(name));
    }
    for (const auto &pair : res) {
        EXPECT_EQ(1, expect.count(pair.first));
    }
}

void run_sindex_list_test(
        UNUSED namespace_interface_t *nsi,
        store_t *store) {
    std::set<std::string> sindexes;

    // Do the whole test a couple times on the same namespace for kicks
    for (size_t i = 0; i < 2; ++i) {
        size_t reps = 10;

        // Make a bunch of sindexes
        for (size_t j = 0; j < reps; ++j) {
            check_sindexes(store, sindexes);
            sindexes.insert(create_sindex(store).name);
        }

        // Remove all the sindexes
        for (size_t j = 0; j < reps; ++j) {
            check_sindexes(store, sindexes);
            drop_sindex(store, *sindexes.begin());
            sindexes.erase(sindexes.begin());
        }
        ASSERT_TRUE(sindexes.empty());
        check_sindexes(store, std::set<std::string>());
    } // Do it again
}

TEST(RDBProtocol, SindexList) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_list_test, false);
}

TEST(RDBProtocol, OvershardedSindexList) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_list_test, true);
}

void run_sindex_oversized_keys_test(
        namespace_interface_t *nsi,
        store_t *store) {
    std::string sindex_id = create_sindex(store).name;
    wait_for_sindex(store, sindex_id);
    ql::configured_limits_t limits;

    for (size_t i = 0; i < 20; ++i) {
        for (size_t j = 100; j < 200; j += 5) {
            std::string id = std::to_string(j)
                + std::string(i + rdb_protocol::MAX_PRIMARY_KEY_SIZE - 10, ' ');
            std::string sid(j, 'a');
            auto sindex_key_literal = ql::datum_t(datum_string_t(sid));
            std::string json_doc = strprintf("{\"id\" : \"%s\", \"sid\" : \"%s\"}",
                                             id.c_str(),
                                             sid.c_str());
            rapidjson::Document data;
            data.Parse(json_doc.c_str());
            ASSERT_FALSE(data.HasParseError());
            store_key_t pk;
            try {
                pk = store_key_t(ql::to_datum(
                                     data.FindMember("id")->value,
                                     limits, reql_version_t::LATEST).print_primary());
            } catch (const ql::base_exc_t &ex) {
                ASSERT_TRUE(id.length() >= rdb_protocol::MAX_PRIMARY_KEY_SIZE);
                continue;
            }

            {
                /* Insert a piece of data (it will be indexed using the secondary
                 * index). */
                write_t write(point_write_t(pk, ql::to_datum(data, limits,
                                                             reql_version_t::LATEST)),
                              durability_requirement_t::DEFAULT,
                              profile_bool_t::PROFILE,
                              limits);
                write_response_t response;

                cond_t interruptor;
                nsi->write(
                    auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
                    write,
                    &response,
                    &interruptor);

                auto resp = boost::get<point_write_response_t>(
                        &response.response);
                if (!resp) {
                    ADD_FAILURE() << "got wrong type of result back";
                } else {
                    ASSERT_EQ(resp->result, point_write_result_t::STORED);
                }
            }

            {
                /* Access the data using the secondary index. */
                read_t read
                    = make_sindex_read(sindex_key_literal, sindex_id);
                read_response_t response;

                cond_t interruptor;
                nsi->read(
                    auth::user_context_t(auth::permissions_t(tribool::True, tribool::False, tribool::False, tribool::False)),
                    read,
                    &response,
                    &interruptor);

                if (rget_read_response_t *rget_resp
                    = boost::get<rget_read_response_t>(&response.response)) {
                    auto streams = boost::get<ql::grouped_t<ql::stream_t> >(
                        &rget_resp->result);
                    ASSERT_TRUE(streams != nullptr);
                    ASSERT_EQ(1, streams->size());
                    // Order doesn't matter because streams->size() is 1.
                    auto stream = &streams->begin()->second;
                    ASSERT_TRUE(stream != nullptr);
                    // There should be results equal to the number of iterations
                    // performed
                    ASSERT_EQ(1ul, stream->substreams.size());
                    ASSERT_EQ(i + 1, stream->substreams.begin()->second.stream.size());
                } else {
                    ADD_FAILURE() << "got wrong type of result back";
                }
            }
        }
    }

}

TEST(RDBProtocol, OverSizedKeys) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_oversized_keys_test, false);
}

TEST(RDBProtocol, OvershardedOverSizedKeys) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_oversized_keys_test, true);
}

void run_sindex_missing_attr_test(
        namespace_interface_t *nsi,
        store_t *store) {
    create_sindex(store);

    ql::configured_limits_t limits;
    rapidjson::Document data;
    data.Parse("{\"id\" : 0}");
    ASSERT_FALSE(data.HasParseError());
    store_key_t pk = store_key_t(ql::to_datum(
                                     data.FindMember("id")->value,
                                     limits, reql_version_t::LATEST).print_primary());
    {
        /* Insert a piece of data (it will be indexed using the secondary
         * index). */
        write_t write(point_write_t(pk, ql::to_datum(data, limits,
                                                     reql_version_t::LATEST)),
                      durability_requirement_t::DEFAULT,
                      profile_bool_t::PROFILE,
                      ql::configured_limits_t());
        write_response_t response;

        cond_t interruptor;
        nsi->write(
            auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::False, tribool::False)),
            write,
            &response,
            &interruptor);

        if (!boost::get<point_write_response_t>(&response.response)) {
            ADD_FAILURE() << "got wrong type of result back";
        }
    }

    //TODO we're not sure if data which is missing an attribute should be put
    //in the sindex or not right now. We should either be checking that the
    //value is in the sindex right now or be checking that it isn't.
}

TEST(RDBProtocol, MissingAttr) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_missing_attr_test, false);
}

TEST(RDBProtocol, OvershardedMissingAttr) {
    run_in_thread_pool_with_namespace_interface(&run_sindex_missing_attr_test, true);
}


#endif  // RDB_FDB_UNITTEST

}   /* namespace unittest */
