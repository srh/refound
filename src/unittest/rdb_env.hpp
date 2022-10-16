// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef UNITTEST_RDB_ENV_HPP_
#define UNITTEST_RDB_ENV_HPP_

#include <stdexcept>
#include <set>
#include <map>
#include <string>
#include <utility>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "clustering/administration/main/ports.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/auth/permission_error.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/watchable.hpp"
#include "extproc/extproc_pool.hpp"
#include "extproc/extproc_spawner.hpp"
#include "rdb_protocol/env.hpp"
#include "rpc/directory/read_manager.hpp"
#include "rpc/directory/write_manager.hpp"
#include "rpc/semilattice/view/field.hpp"
#include "rpc/semilattice/watchable.hpp"
#include "unittest/dummy_metadata_controller.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

#if RDB_FDB_UNITTEST
// These classes are used to provide a mock environment for running reql queries

// The mock namespace interface handles all read and write calls, using a simple in-
//  memory map of store_key_t to datum_t.  The get_data function allows a test to
//  read or modify the dataset to prepare for a query or to check that changes were made.
class mock_namespace_interface_t : public namespace_interface_t {
public:
    explicit mock_namespace_interface_t(datum_string_t _primary_key,
                                        std::map<store_key_t, ql::datum_t> &&_data,
                                        ql::env_t *_env);
    virtual ~mock_namespace_interface_t();

    void read(
        UNUSED auth::user_context_t const &user_context,
        const read_t &query,
        read_response_t *response,
        UNUSED order_token_t tok,
        const signal_t *interruptor)
        THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t, auth::permission_error_t);

    void write(
        UNUSED auth::user_context_t const &user_context,
        const write_t &query,
        write_response_t *response,
        UNUSED order_token_t tok,
        const signal_t *interruptor)
        THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t, auth::permission_error_t);

    bool check_readiness(table_readiness_t readiness, const signal_t *interruptor);

    std::map<store_key_t, ql::datum_t> *get_data();

    std::string get_primary_key() const;

private:
    cond_t ready_cond;
    datum_string_t primary_key;
    std::map<store_key_t, ql::datum_t> data;
    ql::env_t *env;

    // TODO: Move to .cc file, thx.
    struct read_visitor_t : public boost::static_visitor<void> {
        void operator()(const point_read_t &get);
        void operator()(const dummy_read_t &d);
#if RDB_CF
        void NORETURN operator()(const changefeed_subscribe_t &);
        void NORETURN operator()(const changefeed_limit_subscribe_t &);
        void NORETURN operator()(const changefeed_stamp_t &);
        void NORETURN operator()(const changefeed_point_stamp_t &);
#endif  // RDB_CF
        void NORETURN operator()(UNUSED const rget_read_t &rget);
        void NORETURN operator()(UNUSED const intersecting_geo_read_t &gr);
        void NORETURN operator()(UNUSED const nearest_geo_read_t &gr);

        read_visitor_t(mock_namespace_interface_t *parent, read_response_t *_response);

        mock_namespace_interface_t *parent;
        read_response_t *response;
    };

    // TODO: Move to .cc file, thx.
    struct write_visitor_t : public boost::static_visitor<void> {
        void operator()(const batched_replace_t &br);
        void operator()(const batched_insert_t &br);
        void operator()(const dummy_write_t &d);
        void NORETURN operator()(UNUSED const point_write_t &w);
        void NORETURN operator()(UNUSED const point_delete_t &d);
        void NORETURN operator()(UNUSED const sync_t &s);

        write_visitor_t(mock_namespace_interface_t *parent, write_response_t *_response);

        mock_namespace_interface_t *parent;
        write_response_t *response;
    };
};

#endif // RDB_FDB_UNITTEST

class invalid_name_exc_t : public std::exception {
public:
    explicit invalid_name_exc_t(const std::string& name) :
        error_string(strprintf("invalid name string: %s", name.c_str())) { }
    ~invalid_name_exc_t() throw () { }
    const char *what() const throw () {
        return error_string.c_str();
    }
private:
    const std::string error_string;
};

#if RDB_FDB_UNITTEST

/* Because of how internal objects are meant to be instantiated, the proper order of
instantiation is to create a test_rdb_env_t at the top-level of the test (before entering
the thread pool), then to call make_env() on the object once inside the thread pool. From
there, the instance can provide a pointer to the ql::env_t. At the moment, metaqueries
will not work, but everything else should be good. That is, you can specify databases and
tables, but you can't create or destroy them using reql in this environment. As such, you
should create any necessary databases and tables BEFORE creating the instance_t by using
the add_table and add_database functions. */
class test_rdb_env_t {
public:
    test_rdb_env_t();
    ~test_rdb_env_t();

    void add_database(const std::string &db_name);
    void add_table(const std::string &db_name,
                   const std::string &table_name,
                   const std::string &primary_key);

    // The initial_data parameter allows a test to provide a starting dataset.  At
    // the moment, it just takes a set of maps of strings to strings, which will be
    // converted into a set of JSON structures.  This means that the JSON values will
    // only be strings, but if a test needs different properties in their objects,
    // this call should be modified.
    void add_table(const std::string &db_name,
                   const std::string &table_name,
                   const std::string &primary_key,
                   const std::set<ql::datum_t, optional_datum_less_t> &initial_data);

    class instance_t : private reql_cluster_interface_t {
    public:
        explicit instance_t(test_rdb_env_t &&test_env);

        ql::env_t *get_env();
        rdb_context_t *get_rdb_context();
        void interrupt();

        std::map<store_key_t, ql::datum_t> *get_data(name_string_t db,
                                                     name_string_t table);


        bool db_config(
                auth::user_context_t const &user_context,
                const counted_t<const ql::db_t> &db,
                ql::backtrace_id_t bt,
                ql::env_t *env,
                scoped_ptr_t<ql::val_t> *selection_out,
                admin_err_t *error_out) override;

        bool table_config(
                auth::user_context_t const &user_context,
                counted_t<const ql::db_t> db,
                const name_string_t &name,
                ql::backtrace_id_t bt,
                ql::env_t *env,
                scoped_ptr_t<ql::val_t> *selection_out,
                admin_err_t *error_out) override;
        bool table_status(
                counted_t<const ql::db_t> db,
                const name_string_t &name,
                ql::backtrace_id_t bt,
                ql::env_t *env,
                scoped_ptr_t<ql::val_t> *selection_out,
                admin_err_t *error_out) override;

        // QQQ: This isn't going to work.
#if RDB_CF
        ql::changefeed::client_t *get_changefeed_client() override {
            crash("unimplemented");
        }
#endif  // RDB_CF

    private:
        extproc_pool_t extproc_pool;
        dummy_semilattice_controller_t<auth_semilattice_metadata_t> auth_manager;
        rdb_context_t rdb_ctx;
        std::map<name_string_t, database_id_t> databases;
        std::map<std::pair<database_id_t, name_string_t>,
                 scoped_ptr_t<mock_namespace_interface_t> > tables;
        scoped_ptr_t<ql::env_t> env;
        cond_t interruptor;
    };

    scoped_ptr_t<instance_t> make_env();

private:
    extproc_spawner_t extproc_spawner;

    struct table_data_t {
        datum_string_t primary_key;
        std::map<store_key_t, ql::datum_t> initial_data;
    };

    std::set<name_string_t> databases;

    // Initial data for tables are stored here until the instance_t is constructed, at
    //  which point, it is moved into a mock_namespace_interface_t, and this is cleared.
    std::map<std::pair<name_string_t, name_string_t>, table_data_t> tables;
};

#endif  // RDB_FDB_UNITTEST

}  // namespace unittest

#endif // UNITTEST_RDB_ENV_HPP_

