// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "unittest/rdb_env.hpp"

#include "rdb_protocol/func.hpp"
#include "rdb_protocol/pseudo_time.hpp"
#include "rdb_protocol/real_table.hpp"

namespace unittest {

#if RDB_FDB_UNITTEST

mock_namespace_interface_t::mock_namespace_interface_t(
            datum_string_t _primary_key,
            std::map<store_key_t, ql::datum_t> &&_data,
            ql::env_t *_env) :
        primary_key(_primary_key),
        data(std::move(_data)),
        env(_env) {
    ready_cond.pulse();
}

mock_namespace_interface_t::~mock_namespace_interface_t() { }

void mock_namespace_interface_t::read(
        UNUSED auth::user_context_t const &user_context,
        const read_t &query,
        read_response_t *response,
        const signal_t *interruptor)
        THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t, auth::permission_error_t) {
    if (interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }
    read_visitor_t v(this, response);
    boost::apply_visitor(v, query.read);
}

void mock_namespace_interface_t::write(
        UNUSED auth::user_context_t const &user_context,
        const write_t &query,
        write_response_t *response,
        const signal_t *interruptor)
        THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t, auth::permission_error_t) {
    if (interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }
    write_visitor_t v(this, response);
    boost::apply_visitor(v, query.write);
}

std::map<store_key_t, ql::datum_t> *mock_namespace_interface_t::get_data() {
    return &data;
}

std::string mock_namespace_interface_t::get_primary_key() const {
    return primary_key.to_std();
}

void mock_namespace_interface_t::read_visitor_t::operator()(const point_read_t &get) {
    ql::configured_limits_t limits;
    response->response = point_read_response_t();
    point_read_response_t &res = boost::get<point_read_response_t>(response->response);

    if (parent->data.find(get.key) != parent->data.end()) {
        res.data = parent->data.at(get.key);
    } else {
        res.data = ql::datum_t::null();
    }
}

void mock_namespace_interface_t::read_visitor_t::operator()(const dummy_read_t &) {
    response->response = dummy_read_response_t();
}

#if RDB_CF
void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        const changefeed_subscribe_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        const changefeed_limit_subscribe_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        const changefeed_stamp_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        const changefeed_point_stamp_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}
#endif  // RDB_CF

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        UNUSED const rget_read_t &rget) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        UNUSED const intersecting_geo_read_t &gr) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::read_visitor_t::operator()(
        UNUSED const nearest_geo_read_t &gr) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

mock_namespace_interface_t::read_visitor_t::read_visitor_t(
        mock_namespace_interface_t *_parent,
        read_response_t *_response) :
    parent(_parent), response(_response) {
    // Do nothing
}

void mock_namespace_interface_t::write_visitor_t::operator()(
    const batched_replace_t &r) {
    ql::configured_limits_t limits;
    ql::datum_t stats = ql::datum_t::empty_object();
    std::set<std::string> conditions;
    for (auto it = r.keys.begin(); it != r.keys.end(); ++it) {
        auto data_it = parent->data.find(*it);
        ql::datum_object_builder_t resp;
        ql::datum_t old_val = data_it == parent->data.end() ?
            data_it->second : ql::datum_t::null();

        ql::datum_t new_val
            = r.f->call(parent->env, old_val)->as_datum(parent->env);
        parent->data.erase(*it);

        bool err;
        if (new_val.get_type() == ql::datum_t::R_OBJECT) {
            if (old_val.get_type() == ql::datum_t::R_NULL) {
                parent->data.insert(std::make_pair(*it, new_val));
                err = resp.add("inserted", ql::datum_t(1.0));
            } else {
                if (old_val == new_val) {
                    err = resp.add("unchanged", ql::datum_t(1.0));
                } else {
                    parent->data.erase(*it);
                    parent->data.insert(std::make_pair(*it, new_val));
                    err = resp.add("replaced", ql::datum_t(1.0));
                }
            }
        } else if (new_val.get_type() == ql::datum_t::R_NULL) {
            if (old_val.get_type() == ql::datum_t::R_NULL) {
                err = resp.add("skipped", ql::datum_t(1.0));
            } else {
                parent->data.erase(*it);
                err = resp.add("deleted", ql::datum_t(1.0));
            }
        } else {
            throw cannot_perform_query_exc_t(
                "value being inserted is neither an object nor an empty value",
                query_state_t::FAILED);
        }
        guarantee(!err);
        stats = stats.merge(std::move(resp).to_datum(), ql::stats_merge,
                            limits, &conditions);
    }
    ql::datum_object_builder_t result(std::move(stats));
    result.add_warnings(conditions, limits);
    response->response = std::move(result).to_datum();
}

void mock_namespace_interface_t::write_visitor_t::operator()(
    const batched_insert_t &bi) {
    ql::configured_limits_t limits;
    ql::datum_t stats = ql::datum_t::empty_object();
    std::set<std::string> conditions;
    for (auto it = bi.inserts.begin(); it != bi.inserts.end(); ++it) {
        store_key_t key((*it).get_field(datum_string_t(bi.pkey)).print_primary());
        auto data_it = parent->data.find(key);
        ql::datum_object_builder_t resp;
        ql::datum_t old_val = data_it != parent->data.end() ?
            data_it->second : ql::datum_t::null();

        ql::datum_t new_val = *it;
        parent->data.erase(key);

        bool err;
        if (new_val.get_type() == ql::datum_t::R_OBJECT) {
            if (old_val.get_type() == ql::datum_t::R_NULL) {
                parent->data.insert(std::make_pair(key, new_val));
                err = resp.add("inserted", ql::datum_t(1.0));
            } else {
                if (old_val == new_val) {
                    err = resp.add("unchanged", ql::datum_t(1.0));
                } else {
                    parent->data.erase(key);
                    parent->data.insert(std::make_pair(key, new_val));
                    err = resp.add("replaced", ql::datum_t(1.0));
                }
            }
        } else if (new_val.get_type() == ql::datum_t::R_NULL) {
            if (old_val.get_type() == ql::datum_t::R_NULL) {
                err = resp.add("skipped", ql::datum_t(1.0));
            } else {
                parent->data.erase(key);
                err = resp.add("deleted", ql::datum_t(1.0));
            }
        } else {
            throw cannot_perform_query_exc_t(
                "value being inserted is neither an object nor an empty value",
                query_state_t::FAILED);
        }
        guarantee(!err);
        stats = stats.merge(std::move(resp).to_datum(), ql::stats_merge, limits, &conditions);
    }
    ql::datum_object_builder_t result(stats);
    result.add_warnings(conditions, limits);
    response->response = std::move(result).to_datum();
}

void mock_namespace_interface_t::write_visitor_t::operator()(const dummy_write_t &) {
    response->response = dummy_write_response_t();
}

void NORETURN mock_namespace_interface_t::write_visitor_t::operator()(const point_write_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::write_visitor_t::operator()(const point_delete_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

void NORETURN mock_namespace_interface_t::write_visitor_t::operator()(const sync_t &) {
    throw cannot_perform_query_exc_t("unimplemented", query_state_t::FAILED);
}

mock_namespace_interface_t::write_visitor_t::write_visitor_t(
            mock_namespace_interface_t *_parent,
            write_response_t *_response) :
        parent(_parent), response(_response) {
    // Do nothing
}

test_rdb_env_t::test_rdb_env_t() { }

test_rdb_env_t::~test_rdb_env_t() { }

void test_rdb_env_t::add_table(const std::string &db_name,
                               const std::string &table_name,
                               const std::string &primary_key) {
    std::set<ql::datum_t, optional_datum_less_t> empty_data;
    add_table(db_name, table_name, primary_key, empty_data);
}

void test_rdb_env_t::add_table(
        const std::string &db_name,
        const std::string &table_name,
        const std::string &primary_key,
        const std::set<ql::datum_t, optional_datum_less_t> &initial_data) {
    std::pair<name_string_t, name_string_t> db_table(
        { name_string_t::guarantee_valid(db_name.c_str()),
          name_string_t::guarantee_valid(table_name.c_str()) });

    auto table_it = tables.insert(std::make_pair(db_table, table_data_t())).first;
    guarantee(table_it != tables.end());

    table_it->second.primary_key = datum_string_t(primary_key);

    for (auto const &row : initial_data) {
        store_key_t key(row.get_field(table_it->second.primary_key).print_primary());
        table_it->second.initial_data.insert(std::make_pair(key, row));
    }
}

void test_rdb_env_t::add_database(const std::string &db_name) {
    databases.insert(name_string_t::guarantee_valid(db_name.c_str()));
}

scoped_ptr_t<test_rdb_env_t::instance_t> test_rdb_env_t::make_env() {
    return make_scoped<instance_t>(std::move(*this));
}

test_rdb_env_t::instance_t::instance_t(test_rdb_env_t &&test_env) :
    extproc_pool(2),
    auth_manager(auth_semilattice_metadata_t("")),
    rdb_ctx(TODO_fdb(), &extproc_pool, this, auth_manager.get_view())
{
    env.init(
        new ql::env_t(
            &rdb_ctx,
            ql::return_empty_normal_batches_t::NO,
            &interruptor,
            serializable_env_t{
                ql::global_optargs_t(),
                auth::user_context_t(auth::permissions_t(tribool::True, tribool::True, tribool::True, tribool::True)),
                ql::datum_t()},
            nullptr /* no profile trace */));

    // Set up any databases, tables, and data
    for (auto const &db_name : test_env.databases) {
        databases[db_name] = database_id_t{generate_uuid()};
    }

    for (auto &&db_table_pair : test_env.tables) {
        auto db_it = databases.find(db_table_pair.first.first);
        guarantee(db_it != databases.end());

        scoped_ptr_t<mock_namespace_interface_t> storage(
            new mock_namespace_interface_t(
                db_table_pair.second.primary_key,
                std::move(db_table_pair.second.initial_data),
                env.get()));
        tables[std::make_pair(db_it->second, db_table_pair.first.second)] =
            std::move(storage);
    }

    test_env.databases.clear();
    test_env.tables.clear();
}

ql::env_t *test_rdb_env_t::instance_t::get_env() {
    return env.get();
}

rdb_context_t *test_rdb_env_t::instance_t::get_rdb_context() {
    return &rdb_ctx;
}

std::map<store_key_t, ql::datum_t> *test_rdb_env_t::instance_t::get_data(
        name_string_t db, name_string_t table) {
    auto db_it = databases.find(db);
    guarantee(db_it != databases.end());
    auto table_it = tables.find(std::make_pair(db_it->second, table));
    guarantee(table_it != tables.end());
    return table_it->second->get_data();
}

void test_rdb_env_t::instance_t::interrupt() {
    interruptor.pulse();
}

bool test_rdb_env_t::instance_t::db_config(
        UNUSED auth::user_context_t const &user_context,
        UNUSED const counted_t<const ql::db_t> &db,
        UNUSED ql::backtrace_id_t bt,
        UNUSED ql::env_t *local_env,
        UNUSED scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "test_db_env_t::instance_t doesn't support db_config()",
        query_state_t::FAILED};
    return false;
}

bool test_rdb_env_t::instance_t::table_config(
        UNUSED auth::user_context_t const &user_context,
        UNUSED counted_t<const ql::db_t> db,
        UNUSED const name_string_t &name,
        UNUSED ql::backtrace_id_t bt,
        UNUSED ql::env_t *local_env,
        UNUSED scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "test_rdb_env_t::instance_t doesn't support table_config()",
        query_state_t::FAILED};
    return false;
}

bool test_rdb_env_t::instance_t::table_status(
        UNUSED counted_t<const ql::db_t> db,
        UNUSED const name_string_t &name,
        UNUSED ql::backtrace_id_t bt,
        UNUSED ql::env_t *local_env,
        UNUSED scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "test_rdb_env_t::instance_t doesn't support table_status()",
        query_state_t::FAILED};
    return false;
}

#endif  // RDB_FDB_UNITTEST

}  // namespace unittest
