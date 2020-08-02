// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <map>
#include <string>

#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/admin_op_exc.hpp"
#include "clustering/auth/grant.hpp"
#include "clustering/auth/permissions.hpp"
#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/auth/username.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/real_reql_cluster_interface.hpp"
// For utility function; maybe move convert_db_or_table_config_and_name_to_datum out.
#include "clustering/tables/db_config.hpp"
// For utility function; maybe move convert_table_config_to_datum out.
#include "clustering/tables/table_config.hpp"
// TODO: Move db_not_found_error to a different location
#include "clustering/metadata.hpp"
#include "containers/name_string.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/prov_retry_loop.hpp"
#include "rdb_protocol/datum_stream.hpp"
#include "rdb_protocol/datum_string.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/pseudo_geometry.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/terms/writes.hpp"

namespace ql {

name_string_t get_name(backtrace_id_t src, const datum_string_t &raw_name,
                       const char *type_str) {
    name_string_t name;
    bool assignment_successful = name.assign_value(raw_name);
    rcheck_src(src,
                  assignment_successful,
                  base_exc_t::LOGIC,
                  strprintf("%s name `%s` invalid (%s).",
                            type_str,
                            raw_name.to_std().c_str(),
                            name_string_t::valid_char_msg));
    return name;
}

name_string_t get_name(env_t *env, const scoped_ptr_t<val_t> &name, const char *type_str) {
    r_sanity_check(name.has());
    return get_name(name->backtrace(), name->as_str(env), type_str);
}

class table_generate_config_params_t {
public:
    static table_generate_config_params_t make_default() {
        table_generate_config_params_t p;
        p.primary_replica_tag = name_string_t::guarantee_valid("default");
        p.num_replicas[p.primary_replica_tag] = 1;
        return p;
    }
    std::map<name_string_t, size_t> num_replicas;
    std::set<name_string_t> nonvoting_replica_tags;
    name_string_t primary_replica_tag;
};

void get_replicas_and_primary(env_t *env,
                              const scoped_ptr_t<val_t> &replicas,
                              const scoped_ptr_t<val_t> &nonvoting_replica_tags,
                              const scoped_ptr_t<val_t> &primary_replica_tag) {
    // This is vestigial option parsing, designed solely to error on certain options,
    // and force evaluation of the arguments, as they were pre-fdb.  If the code is more
    // complicated tha it needs to be, so be it.


    table_generate_config_params_t params_value = table_generate_config_params_t::make_default();
    auto *params = &params_value;

    if (replicas.has()) {
        params->num_replicas.clear();
        backtrace_id_t replicas_bt = replicas->backtrace();
        datum_t datum = replicas->as_datum(env);
        if (datum.get_type() == datum_t::R_OBJECT) {
            rcheck_src(replicas_bt, primary_replica_tag.has(), base_exc_t::LOGIC,
                "`primary_replica_tag` must be specified when `replicas` is an OBJECT.");
            for (size_t i = 0; i < datum.obj_size(); ++i) {
                std::pair<datum_string_t, datum_t> pair = datum.get_pair(i);
                name_string_t name = get_name(replicas_bt, pair.first, "Server tag");
                int64_t count = checked_convert_to_int(replicas_bt,
                                                       pair.second.as_num());
                rcheck_src(replicas_bt, count >= 0,
                    base_exc_t::LOGIC, "Can't have a negative number of replicas");
                size_t size_count = static_cast<size_t>(count);
                rcheck_src(replicas_bt, static_cast<int64_t>(size_count) == count,
                              base_exc_t::LOGIC,
                              strprintf("Integer too large: %" PRIi64, count));
                params->num_replicas.insert(std::make_pair(name, size_count));
            }
        } else if (datum.get_type() == datum_t::R_NUM) {
            rcheck_src(
                replicas_bt, !primary_replica_tag.has(), base_exc_t::LOGIC,
                "`replicas` must be an OBJECT if `primary_replica_tag` is specified.");
            rcheck_src(
                replicas_bt, !nonvoting_replica_tags.has(), base_exc_t::LOGIC,
                "`replicas` must be an OBJECT if `nonvoting_replica_tags` is "
                "specified.");
            size_t count = val_t::int64_as_int<size_t>(replicas_bt, datum.as_int());
            params->num_replicas.insert(
                std::make_pair(params->primary_replica_tag, count));
        } else {
            rfail_src(replicas_bt, base_exc_t::LOGIC,
                "Expected type OBJECT or NUMBER but found %s:\n%s",
                datum.get_type_name().c_str(), datum.print().c_str());
        }
    }

    if (nonvoting_replica_tags.has()) {
        params->nonvoting_replica_tags.clear();
        datum_t datum = nonvoting_replica_tags->as_datum(env);
        rcheck_target(nonvoting_replica_tags.get(), datum.get_type() == datum_t::R_ARRAY,
            base_exc_t::LOGIC, strprintf("Expected type ARRAY but found %s:\n%s",
            datum.get_type_name().c_str(), datum.print().c_str()));
        for (size_t i = 0; i < datum.arr_size(); ++i) {
            datum_t tag = datum.get(i);
            rcheck_target(
                nonvoting_replica_tags.get(), tag.get_type() == datum_t::R_STR,
                base_exc_t::LOGIC, strprintf("Expected type STRING but found %s:\n%s",
                tag.get_type_name().c_str(), tag.print().c_str()));
            params->nonvoting_replica_tags.insert(get_name(
                nonvoting_replica_tags->backtrace(), tag.as_str(), "Server tag"));
        }
    }

    if (primary_replica_tag.has()) {
        params->primary_replica_tag = get_name(
            primary_replica_tag->backtrace(), primary_replica_tag->as_str(env), "Server tag");
    }
}

// Meta operations (BUT NOT TABLE TERMS) should inherit from this.
class meta_op_term_t : public op_term_t {
public:
    meta_op_term_t(compile_env_t *env, const raw_term_t &term,
                   argspec_t &&argspec, const optargspec_t &optargspec = optargspec_t({}))
        : op_term_t(env, term, std::move(argspec), optargspec) { }

private:
    deterministic_t is_deterministic() const final { return deterministic_t::no(); }
};

counted_t<const db_t> make_artificial_db() {
    return make_counted<db_t>(
            artificial_reql_cluster_interface_t::database_id,
            artificial_reql_cluster_interface_t::database_name,
            config_version_checker::empty());
}

counted_t<const db_t> provisional_to_db(
        FDBDatabase *fdb,
        reqlfdb_config_cache *cc,
        const signal_t *interruptor,
        const provisional_db_id &prov_db) {
    if (prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
        return make_artificial_db();
    }

    optional<config_info<database_id_t>> cached
        = cc->try_lookup_cached_db(prov_db.db_name);
    if (cached.has_value()) {
        return make_counted<const db_t>(
            cached->ci_value, prov_db.db_name,
            config_version_checker{cached->ci_cv.value});
    }

    config_info<optional<database_id_t>> result;
    reqlfdb_config_version prior_cv = cc->config_version;
    // TODO: Read-only txn.
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&](FDBTransaction *txn) {
        result = config_cache_retrieve_db_by_name(
            prior_cv, txn, prov_db.db_name, interruptor);
    });
    guarantee_fdb_TODO(loop_err, "config_cache_retrieve_db_by_name loop");

    cc->note_version(result.ci_cv);

    if (result.ci_value.has_value()) {
        database_id_t db_id = *result.ci_value;
        cc->add_db(db_id, prov_db.db_name);

        return make_counted<const db_t>(
            *result.ci_value, prov_db.db_name,
            config_version_checker{result.ci_cv.value});
    } else {
        rfail_db_not_found(prov_db.bt, prov_db.db_name);
    }
}

// TODO: QQQ comments -- must be done later (much later, see how stuff plays out)
// TODO: OOO comments -- must be done later (soon)
// TODO: NNN comments -- must be done later (now-ish, immediate cleanup)

class db_term_t final : public meta_op_term_t {
public:
    db_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }
private:
    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const override {
        name_string_t db_name = get_name(env->env, args->arg(env, 0), "Database");

        return new_val(provisional_db_id{
            db_name,
            backtrace(),
        });
    }
    const char *name() const override { return "db"; }
};

class db_create_term_t : public meta_op_term_t {
public:
    db_create_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }
private:
    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        name_string_t db_name = get_name(env->env, args->arg(env, 0), "Database");

        if (db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error = db_already_exists_error(db_name);
            REQL_RETHROW(error);
        }

        database_id_t db_id{generate_uuid()};
        bool fdb_result;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Make sure config_cache_db_create checks auth permission.
                bool success = config_cache_db_create(
                    txn,
                    env->env->get_user_context(),
                    db_name,
                    db_id,
                    env->env->interruptor);
                if (success) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = success;
            });
            guarantee_fdb_TODO(loop_err, "db_create txn failed");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        // TODO: Wipe config cache after txn succeeds?  Why not.

        if (!fdb_result) {
            admin_err_t error = db_already_exists_error(db_name);
            REQL_RETHROW(error);
        }

        ql::datum_t new_config = convert_db_or_table_config_and_name_to_datum(db_name, db_id.value);
        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("dbs_created", ql::datum_t(1.0));
        result_builder.overwrite("config_changes",
            make_replacement_pair(ql::datum_t::null(), new_config));
        ql::datum_t result = std::move(result_builder).to_datum();
        return new_val(std::move(result));
    }
    const char *name() const override { return "db_create"; }
};

class table_create_term_t : public meta_op_term_t {
public:
    table_create_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 2),
            optargspec_t({"primary_key", "shards", "replicas",
                          "nonvoting_replica_tags", "primary_replica_tag",
                          "durability"})) { }
private:
    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        /* Parse arguments */

        // Parse the 'shards' optarg
        if (scoped_ptr_t<val_t> shards_optarg = args->optarg(env, "shards")) {
            rcheck_target(shards_optarg, shards_optarg->as_int(env) == 1, base_exc_t::LOGIC,
                          "Every table must have exactly one shard.  (The configuration is obsolete.)");
        }

        // Parse the 'replicas', 'nonvoting_replica_tags', and 'primary_replica_tag'
        // optargs.  (The results don't get output anywhere post-fdb, because we now
        // ignore these arguments.
        get_replicas_and_primary(env->env,
                                 args->optarg(env, "replicas"),
                                 args->optarg(env, "nonvoting_replica_tags"),
                                 args->optarg(env, "primary_replica_tag"));

        std::string primary_key = "id";
        if (scoped_ptr_t<val_t> v = args->optarg(env, "primary_key")) {
            primary_key = v->as_str(env).to_std();
        }

        UNUSED write_durability_t durability =
            parse_durability_optarg(env->env, args->optarg(env, "durability")) ==
                durability_requirement_t::SOFT ?
                    write_durability_t::SOFT : write_durability_t::HARD;
        // TODO: Can we emit a warning somehow if durability wasn't HARD?

        provisional_db_id prov_db;
        name_string_t tbl_name;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            prov_db = std::move(*dbv).as_prov_db(env->env);
            tbl_name = get_name(env->env, args->arg(env, 0), "Table");
        } else {
            prov_db = std::move(*args->arg(env, 0)).as_prov_db(env->env);
            tbl_name = get_name(env->env, args->arg(env, 1), "Table");
        }

        if (prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't create new tables "
                      "in it.", artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        // TODO: Fixup all '[&]' capture list usage.

        namespace_id_t new_table_id{generate_uuid()};

        bool fdb_result;
        table_config_t config_result;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Put this in config_cache_table_create?  Have it return a table_config_t?
                database_id_t db_id = expect_retrieve_db(txn, prov_db, env->env->interruptor);

                table_config_t config;
                config.basic.name = tbl_name;
                config.basic.database = db_id;
                config.basic.primary_key = primary_key;
                // TODO: Remove sharding UI.
                config.user_data = default_user_data();
                bool success = config_cache_table_create(
                    txn, env->env->get_user_context(), new_table_id, config,
                    env->env->interruptor);
                if (success) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = success;
                config_result = config;
            });
            guarantee_fdb_TODO(loop_err, "table_create txn failed");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!fdb_result) {
            admin_err_t error = table_already_exists_error(prov_db.db_name, tbl_name);
            REQL_RETHROW(error);
        }
        // TODO: Wipe the config cache after the txn succeeds?

        ql::datum_t new_config = convert_table_config_to_datum(new_table_id,
            convert_name_to_datum(prov_db.db_name), config_result);

        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("tables_created", ql::datum_t(1.0));
        result_builder.overwrite("config_changes",
            make_replacement_pair(ql::datum_t::null(), new_config));
        ql::datum_t result = std::move(result_builder).to_datum();

        return new_val(std::move(result));
    }
    const char *name() const override { return "table_create"; }
};

class db_drop_term_t : public meta_op_term_t {
public:
    db_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }
private:
    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        name_string_t db_name = get_name(env->env, args->arg(env, 0), "Database");

        if (db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't delete it.",
                          artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }


        optional<database_id_t> fdb_result;

        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: What if we can't iterate the table list (for checking
                // permissions) in a single fdb transaction?
                database_id_t db_id_local;
                optional<database_id_t> success
                    = config_cache_db_drop(txn,
                        env->env->get_user_context(),
                        db_name, env->env->interruptor);
                if (success.has_value()) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = success;
            });
            guarantee_fdb_TODO(loop_err, "db_drop txn failed");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!fdb_result.has_value()) {
            admin_err_t error = db_not_found_error(db_name);
            REQL_RETHROW(error);
        }

        // TODO: Wipe the config cache after the txn succeeds?  If the code doesn't bloat up too much.

        ql::datum_t old_config
            = convert_db_or_table_config_and_name_to_datum(db_name, fdb_result->value);
        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("dbs_dropped", ql::datum_t(1.0));
        result_builder.overwrite(
            "tables_dropped", ql::datum_t::null());  // TODO: Maybe a string, "unknown".
        // TODO: The db_drop term could wait for the job to complete.
        // OOO: Or, the db_drop term should output the drop job id.
        // ql::datum_t(static_cast<double>(tables_dropped)));
        result_builder.overwrite(
            "config_changes",
            make_replacement_pair(std::move(old_config), ql::datum_t::null()));
        ql::datum_t result = std::move(result_builder).to_datum();

        return new_val(result);
    }
    const char *name() const override { return "db_drop"; }
};

class table_drop_term_t : public meta_op_term_t {
public:
    table_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 2)) { }
private:
    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        provisional_db_id prov_db;
        name_string_t tbl_name;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            prov_db = std::move(*dbv).as_prov_db(env->env);
            tbl_name = get_name(env->env, args->arg(env, 0), "Table");
        } else {
            prov_db = std::move(*args->arg(env, 0)).as_prov_db(env->env);
            tbl_name = get_name(env->env, args->arg(env, 1), "Table");
        }

        if (prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't drop tables in it.",
                          artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        optional<std::pair<namespace_id_t, table_config_t>> fdb_result;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                optional<std::pair<namespace_id_t, table_config_t>> success
                    = config_cache_table_drop(txn,
                        env->env->get_user_context(),
                        prov_db, tbl_name,
                        env->env->interruptor);
                if (success.has_value()) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = std::move(success);
            });
            guarantee_fdb_TODO(loop_err, "table_drop txn failed");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!fdb_result.has_value()) {
            rfail_table_dne(prov_db.db_name, tbl_name);
        }

        // TODO: Wipe the config cache after the txn succeeds?
        ql::datum_t old_config = convert_table_config_to_datum(
            fdb_result->first, convert_name_to_datum(prov_db.db_name), fdb_result->second);

        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("tables_dropped", ql::datum_t(1.0));
        result_builder.overwrite("config_changes",
            make_replacement_pair(old_config, ql::datum_t::null()));
        ql::datum_t result = std::move(result_builder).to_datum();
        return new_val(std::move(result));
    }
    const char *name() const override { return "table_drop"; }
};

template <class T>
void sorted_vector_insert(std::vector<T> *vec, const T &value) {
    vec->insert(std::lower_bound(vec->begin(), vec->end(), value), value);
}

class db_list_term_t : public meta_op_term_t {
public:
    db_list_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(0)) { }
private:
    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *, eval_flags_t) const override {
        std::vector<name_string_t> db_list;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Use a snapshot read for this?  Config txn appropriately?
            db_list = config_cache_db_list_sorted(txn, env->env->interruptor);
        });
        guarantee_fdb_TODO(loop_err, "db_list txn failed");
        // TODO: Use the db_list (and get the config version) to write to the config cache?

        sorted_vector_insert(&db_list, artificial_reql_cluster_interface_t::database_name);

        std::vector<datum_t> arr;
        arr.reserve(db_list.size());
        for (const name_string_t &db : db_list) {
            arr.push_back(datum_t(datum_string_t(db.str())));
        }

        return new_val(datum_t(std::move(arr), env->env->limits()));
    }
    const char *name() const override { return "db_list"; }
};

class table_list_term_t : public meta_op_term_t {
public:
    table_list_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(0, 1)) { }
private:
    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const override {
        provisional_db_id db;
        if (args->num_args() == 0) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            db = std::move(*dbv).as_prov_db(env->env);
        } else {
            db = std::move(*args->arg(env, 0)).as_prov_db(env->env);
        }

        std::vector<name_string_t> table_list;
        if (db.db_name == artificial_reql_cluster_interface_t::database_name) {
            table_list = artificial_reql_cluster_interface_t::table_list_sorted();
        } else {
            fdb_error_t loop_err = txn_retry_loop_coro(env->fdb(), env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Use a snapshot read for this?  Config txn appropriately?
                table_list = config_cache_table_list_sorted(txn, db, env->env->interruptor);
            });
            guarantee_fdb_TODO(loop_err, "db_list txn failed");
        }

        // table_list is in sorted order.

        std::vector<datum_t> arr;
        arr.reserve(table_list.size());
        for (const auto &name : table_list) {
            arr.push_back(datum_t(datum_string_t(name.str())));
        }
        return new_val(datum_t(std::move(arr), env->env->limits()));
    }
    const char *name() const override { return "table_list"; }
};

class config_term_t : public meta_op_term_t {
public:
    config_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 1), optargspec_t({})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        // OOO: Fdb-ize this function.
        scoped_ptr_t<val_t> target = args->arg(env, 0);
        scoped_ptr_t<val_t> selection;
        try {
            /* Note that we always require an argument; we never take a default `db`
            argument. So `r.config()` is an error rather than the configuration for the
            current database. This is why we don't subclass from `table_or_db_meta_term_t`.
            */
            artificial_reql_cluster_interface_t *art_or_null
                = env->env->get_rdb_ctx()->artificial_interface_or_null;
            guarantee(art_or_null != nullptr);  // We're not a deterministic term.

            if (target->get_type().is_convertible(val_t::type_t::DB)) {
                provisional_db_id db = std::move(*target).as_prov_db(env->env);
                if (db.db_name == artificial_reql_cluster_interface_t::database_name) {
                    rfail(ql::base_exc_t::OP_FAILED,
                        "Database `%s` is special; you can't configure it.",
                            artificial_reql_cluster_interface_t::database_name.c_str());
                }

                selection = real_reql_cluster_interface::make_db_config_selection(
                        art_or_null,
                        env->env->get_user_context(),
                        db,
                        backtrace(),
                        env->env);
            } else {
                provisional_table_id table = std::move(*target).as_prov_table(env->env);

                if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
                    // NNN: We should fail with a table d.n.e. error if the table dne.
                    rfail(ql::base_exc_t::OP_FAILED,
                          "Database `%s` is special; you can't configure the tables in it.",
                          artificial_reql_cluster_interface_t::database_name.c_str());
                }

                selection = real_reql_cluster_interface::make_table_config_selection(
                        art_or_null,
                        env->env->get_user_context(),
                        table,
                        backtrace(),
                        env->env);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        return selection;
    }
    virtual const char *name() const { return "config"; }
};

class status_term_t : public meta_op_term_t {
public:
    status_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 1), optargspec_t({})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);

        // NNN: First check if the table exists?

        admin_err_t error{
            "The `status` term is not supported in reql-on-fdb.",  // TODO: Product name
                query_state_t::FAILED};
        REQL_RETHROW(error);
    }
    virtual const char *name() const { return "status"; }
};

/* Common superclass for terms that can operate on either a table or a database: `wait`,
`reconfigure`, and `rebalance`.  Provides the helper function, parse_args. */
class table_or_db_meta_term_t : public meta_op_term_t {
public:
    table_or_db_meta_term_t(compile_env_t *env, const raw_term_t &term,
                            optargspec_t &&_optargs)
        /* None of the subclasses take positional arguments except for the table/db. */
        : meta_op_term_t(env, term, argspec_t(0, 1), std::move(_optargs)) { }

protected:
    // .second is empty if called on a db.  .first == .second->prov_db otherwise.
    std::pair<provisional_db_id, optional<provisional_table_id>>
    parse_args(scope_env_t *env, args_t *args) const {
        if (args->num_args() == 0) {
            rfail(base_exc_t::LOGIC, "`%s` can only be called on a table or database.",
                name());
        }
        scoped_ptr_t<val_t> target = args->arg(env, 0);
        std::pair<provisional_db_id, optional<provisional_table_id>> ret;
        if (target->get_type().is_convertible(val_t::type_t::DB)) {
            ret.first = std::move(*target).as_prov_db(env->env);
        } else {
            ret.second.set(std::move(*target).as_prov_table(env->env));
            ret.first = ret.second->prov_db;
        }
        return ret;
    }
};

// TODO: Can we rename db's?  Do we check that we can't rename to the system db name?

class wait_term_t : public table_or_db_meta_term_t {
public:
    wait_term_t(compile_env_t *env, const raw_term_t &term)
        : table_or_db_meta_term_t(env, term,
                                  optargspec_t({"timeout", "wait_for"})) { }
private:
    static char const * const wait_outdated_str;
    static char const * const wait_reads_str;
    static char const * const wait_writes_str;
    static char const * const wait_all_str;

    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        std::pair<provisional_db_id, optional<provisional_table_id>> args_pair
            = parse_args(env, args);

        // TODO: Maybe this could do some fdb shard readiness query to check table
        // availability.

        // Check 'wait_for' optarg is valid -- but we ignore it, post-fdb.
        {
            table_readiness_t readiness = table_readiness_t::finished;
            if (scoped_ptr_t<val_t> wait_for = args->optarg(env, "wait_for")) {
                if (wait_for->as_str(env) == wait_outdated_str) {
                    readiness = table_readiness_t::outdated_reads;
                } else if (wait_for->as_str(env) == wait_reads_str) {
                    readiness = table_readiness_t::reads;
                } else if (wait_for->as_str(env) == wait_writes_str) {
                    readiness = table_readiness_t::writes;
                } else if (wait_for->as_str(env) == wait_all_str) {
                    readiness = table_readiness_t::finished;
                } else {
                    rfail_target(wait_for, base_exc_t::LOGIC,
                                 "Unknown table readiness state: '%s', must be one of "
                                 "'%s', '%s', '%s', or '%s'",
                                 wait_for->as_str(env).to_std().c_str(),
                                 wait_outdated_str, wait_reads_str,
                                 wait_writes_str, wait_all_str);
                }
            }
        }

        // Handle 'timeout' optarg
        // Since we don't actually wait right now, it's a bit silly, but we do ping fdb.
        signal_timer_t timeout_timer;
        wait_any_t combined_interruptor(env->env->interruptor);
        if (scoped_ptr_t<val_t> timeout = args->optarg(env, "timeout")) {
            timeout_timer.start(timeout->as_int<uint64_t>(env) * 1000);
            combined_interruptor.add(&timeout_timer);
        }

        // Handle system db cases.
        if (args_pair.first.db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; the system tables in it are "
                          "always available and don't need to be waited on.",
                artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        // OOO: There might be some kind of fdb status operation which lets us check all
        // shards in a range are accessible.  But assuming healthy fdb conditioning,
        // tables are _always_ ready.  So we might make this a no-op function which
        // merely checks table existence.

        // (Note that table readiness does not assume sindex construction is complete.)

        // Perform db or table wait
        double result;
        try {
            if (args_pair.second.has_value()) {
                config_info<std::pair<namespace_id_t, table_config_t>> config;
                fdb_error_t loop_err = txn_retry_loop_coro(env->fdb(), env->env->interruptor, [&](FDBTransaction *txn) {
                    // TODO: Use a snapshot read for this?  Config txn appropriately?
                    config = expect_retrieve_table(txn, *args_pair.second,
                        env->env->interruptor);
                });
                guarantee_fdb_TODO(loop_err, "wait term txn failed on table_list");

                result = 1.0;
            } else {
                std::vector<name_string_t> table_list;
                fdb_error_t loop_err = txn_retry_loop_coro(env->fdb(), env->env->interruptor, [&](FDBTransaction *txn) {
                    // TODO: Use a snapshot read for this?  Config txn appropriately?
                    table_list = config_cache_table_list_sorted(txn, args_pair.first,
                        env->env->interruptor);
                });
                guarantee_fdb_TODO(loop_err, "wait term txn failed on table_list");

                result = static_cast<double>(table_list.size());
            }
        } catch (const interrupted_exc_t &ex) {
            if (!timeout_timer.is_pulsed()) {
                throw;
            }
            rfail(base_exc_t::OP_FAILED, "Timed out while waiting for tables.");
        }

        ql::datum_object_builder_t builder;
        builder.overwrite("ready", ql::datum_t(result));
        return new_val(std::move(builder).to_datum());
    }
    const char *name() const final { return "wait"; }
};

char const * const wait_term_t::wait_outdated_str = "ready_for_outdated_reads";
char const * const wait_term_t::wait_reads_str = "ready_for_reads";
char const * const wait_term_t::wait_writes_str = "ready_for_writes";
char const * const wait_term_t::wait_all_str = "all_replicas_ready";

// This term exists simply to parse arguments and respond with an error.
class reconfigure_term_t : public table_or_db_meta_term_t {
public:
    reconfigure_term_t(compile_env_t *env, const raw_term_t &term)
        : table_or_db_meta_term_t(env, term,
            optargspec_t({"dry_run", "emergency_repair", "nonvoting_replica_tags",
                "primary_replica_tag", "replicas", "shards"})) { }
private:
    scoped_ptr_t<val_t> required_optarg(scope_env_t *env,
                                        args_t *args,
                                        const char *_name) const {
        scoped_ptr_t<val_t> result = args->optarg(env, _name);
        rcheck(result.has(), base_exc_t::LOGIC,
               strprintf("Missing required argument `%s`.", _name));
        return result;
    }

    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        std::pair<provisional_db_id, optional<provisional_table_id>> args_pair
            = parse_args(env, args);

        // Parse the 'dry_run' optarg
        bool dry_run = false;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "dry_run")) {
            dry_run = v->as_bool(env);
        }

        /* Figure out whether we're doing a regular reconfiguration or an emergency
        repair. */
        scoped_ptr_t<val_t> emergency_repair = args->optarg(env, "emergency_repair");
        if (!emergency_repair.has() ||
                emergency_repair->as_datum(env) == ql::datum_t::null()) {
            /* We're doing a regular reconfiguration. */

            // Parse the 'shards' optarg
            scoped_ptr_t<val_t> shards_optarg = required_optarg(env, args, "shards");
            rcheck_target(shards_optarg, shards_optarg->as_int(env) == 1,
                          base_exc_t::LOGIC,
                          "Every table must have exactly one shard.  (The configuration is obsolete.)");

            // Parse the 'replicas', 'nonvoting_replica_tags', and
            // 'primary_replica_tag' optargs (and then ignore the arguments).
            get_replicas_and_primary(env->env,
                                     required_optarg(env, args, "replicas"),
                                     args->optarg(env, "nonvoting_replica_tags"),
                                     args->optarg(env, "primary_replica_tag"));

            bool success;
            datum_t result;
            admin_err_t error;
            try {
                /* Perform the operation */
                rfail(base_exc_t::OP_FAILED, "Per-table reconfiguration is not supported on Reql-on-FDB");  // TODO: Product name
            } catch (auth::permission_error_t const &permission_error) {
                rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
            }
            if (!success) {
                REQL_RETHROW(error);
            }

            return new_val(result);
        } else {
            /* We're doing an emergency repair */

            /* Parse `emergency_repair` to figure out which kind we're doing. */
            datum_string_t emergency_repair_str = emergency_repair->as_str(env);
            emergency_repair_mode_t mode;
            if (emergency_repair_str == "_debug_recommit") {
                mode = emergency_repair_mode_t::DEBUG_RECOMMIT;
            } else if (emergency_repair_str == "unsafe_rollback") {
                mode = emergency_repair_mode_t::UNSAFE_ROLLBACK;
            } else if (emergency_repair_str == "unsafe_rollback_or_erase") {
                mode = emergency_repair_mode_t::UNSAFE_ROLLBACK_OR_ERASE;
            } else {
                rfail_target(emergency_repair.get(), base_exc_t::LOGIC,
                    "`emergency_repair` should be \"unsafe_rollback\" or "
                    "\"unsafe_rollback_or_erase\"");
            }

            /* Make sure none of the optargs that are used with regular reconfigurations
            are present, to avoid user confusion. */
            if (args->optarg(env, "nonvoting_replica_tags").has() ||
                    args->optarg(env, "primary_replica_tag").has() ||
                    args->optarg(env, "replicas").has() ||
                    args->optarg(env, "shards").has()) {
                rfail(base_exc_t::LOGIC, "In emergency repair mode, you can't "
                    "specify shards, replicas, etc.");
            }

            if (!args_pair.second.has_value()) {
                rfail(base_exc_t::LOGIC, "Can't emergency repair an entire database "
                    "at once; instead you should run `reconfigure()` on each table "
                    "individually.");
            }

            rfail(base_exc_t::OP_FAILED, "Per-table reconfiguration is not supported on Reql-on-FDB");  // TODO: Product name
        }
    }
    const char *name() const final { return "reconfigure"; }
};

class rebalance_term_t : public table_or_db_meta_term_t {
public:
    rebalance_term_t(compile_env_t *env, const raw_term_t &term)
        : table_or_db_meta_term_t(env, term, optargspec_t({})) { }
private:
    scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const override {
        auto args_pair = parse_args(env, args);

        rfail(base_exc_t::OP_FAILED, "Rebalancing is not supported (and unnecessary) on Reql-on-FDB");  // TODO: Product name
    }
    const char *name() const final { return "rebalance"; }
};

// QQQ: User configs include system table configs -- which means some db code using the users table needs to keep that in mind (when maintaining users table sindexes and such).

class sync_term_t : public meta_op_term_t {
public:
    sync_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }

private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const {
        // We now succeed on system tables, unlike pre-fdb sync_term_t.  This is
        // desirable.

        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);

        try {
            fdb_error_t loop_err = txn_retry_loop_table_id(
                env->env->get_rdb_ctx()->fdb,
                    env->env->get_rdb_ctx()->config_caches.get(),
                    env->env->interruptor,
                    table,
                    [&](FDBTransaction *txn, const database_id_t &db_id, const namespace_id_t &table_id, cv_check_fut&& cvc) {
                auth::fdb_user_fut<auth::write_permission> auth_fut
                    = env->env->get_user_context().transaction_require_write_permission(
                        txn, db_id, table_id);
                cvc.block_and_check(env->env->interruptor);
                auth_fut.block_and_check(env->env->interruptor);
            });
            guarantee_fdb_TODO(loop_err, "sync_term_t retry loop");
        } catch (auth::permission_error_t const &permission_error) {
            // Taken from artificial_table.cc
            rfail_datum(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        ql::datum_object_builder_t result;
        result.overwrite("synced", ql::datum_t(1.0));
        return new_val(std::move(result).to_datum());
    }
    virtual const char *name() const { return "sync"; }
};

class grant_term_t : public meta_op_term_t {
public:
    grant_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(2, 3), optargspec_t({})) { }

private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const {
        auth::username_t username(
            args->arg(env, args->num_args() - 2)->as_str(env).to_std());
        ql::datum_t permissions = args->arg(env, args->num_args() - 1)->as_datum(env);

        try {
            std::function<auth::permissions_t *(auth::user_t *, FDBTransaction *, const signal_t *)> permissions_selector;

            if (args->num_args() == 2) {
                permissions_selector = [](auth::user_t *user, FDBTransaction *, const signal_t *) -> auth::permissions_t * {
                    return &user->get_global_permissions();
                };
            } else {
                scoped_ptr_t<val_t> scope = args->arg(env, 0);
                if (scope->get_type().is_convertible(val_t::type_t::DB)) {
                    // TODO: Config version consistency logic with prior db_t name lookup.
                    const provisional_db_id prov_db = std::move(*scope).as_prov_db(env->env);
                    if (prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
                        permissions_selector = [](auth::user_t *user, FDBTransaction *, const signal_t *) -> auth::permissions_t * {
                            return &user->get_database_permissions(
                                artificial_reql_cluster_interface_t::database_id);
                        };
                    } else {
                        permissions_selector = [prov_db](auth::user_t *user, FDBTransaction *txn, const signal_t *interruptor) -> auth::permissions_t * {
                            database_id_t db_id = expect_retrieve_db(txn, prov_db, interruptor);
                            return &user->get_database_permissions(db_id);
                        };
                    }
                } else {
                    // TODO: Config version consistency logic with the table name lookup
                    // that made the namespace_id_t.
                    const provisional_table_id table = std::move(*scope).as_prov_table(env->env);
                    if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
                        optional<namespace_id_t> table_id = artificial_reql_cluster_interface_t::get_table_id(table.table_name);
                        if (!table_id.has_value()) {
                            rfail_prov_table_dne(table);
                        }
                        permissions_selector = [table_id](auth::user_t *user, FDBTransaction *, const signal_t *) -> auth::permissions_t * {
                            return &user->get_table_permissions(*table_id);
                        };
                    } else {
                        permissions_selector = [table](auth::user_t *user, FDBTransaction *txn, const signal_t *interruptor) -> auth::permissions_t * {
                            // TODO: Just retrieve table id, not table config or cv.
                            std::pair<namespace_id_t, table_config_t> info =
                                expect_retrieve_table(txn, table, interruptor).ci_value;
                            return &user->get_table_permissions(info.first);
                        };
                    }
                }
            }

            bool fdb_success;
            ql::datum_t fdb_result;
            admin_err_t fdb_err;

            fdb_error_t err = txn_retry_loop_coro(env->fdb(), env->env->interruptor,
                    [&](FDBTransaction *txn) {
                ql::datum_t result;
                admin_err_t err;
                bool ret = auth::grant(
                    txn,
                    env->env->get_user_context(),
                    std::move(username),
                    std::move(permissions),
                    env->env->interruptor,
                    permissions_selector,
                    &result,
                    &err);
                if (ret) {
                    commit(txn, env->env->interruptor);
                }
                fdb_success = ret;
                fdb_result = result;
                fdb_err = err;
            });
            guarantee_fdb_TODO(err, "retry loop in grant_term_t for grant_global");

            if (!fdb_success) {
                REQL_RETHROW(fdb_err);
            }
            return new_val(std::move(fdb_result));
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
    }

    virtual const char *name() const {
        return "grant";
    }
};

// NNN: Remove
read_mode_t dummy_read_mode() { return read_mode_t::SINGLE; }

// For when prov_table.prov_db.db_name has the artificial db.
scoped<table_t> make_artificial_table(
        artificial_reql_cluster_interface_t *art,
        const provisional_table_id &prov_table) {
    counted_t<const base_table_t> table;
    if (!art->table_find(prov_table.table_name,
            prov_table.identifier_format.value_or(admin_identifier_format_t::name),
            &table)) {
        rfail_prov_table_dne(prov_table);
    }
    return make_scoped<table_t>(
        std::move(table), make_artificial_db(), prov_table.table_name, dummy_read_mode(),
        prov_table.bt);
}

scoped<table_t> provisional_to_table(
        FDBDatabase *fdb,
        const signal_t *interruptor,
        reqlfdb_config_cache *cc,
        artificial_reql_cluster_interface_t *art_or_null,
        const provisional_table_id &prov_table) {
    r_sanity_check(art_or_null != nullptr);

    if (prov_table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
        return make_artificial_table(art_or_null, prov_table);
    }

    counted_t<const real_table_t> table;
    counted_t<const db_t> out_db;


    // OOO: Use the config cache to optimistically perform the table lookup.

#if 0
    // TODO: ASSERT_NO_CORO_WAITING on these two cache lookups, as mutex assertion.
    optional<config_info<database_id_t>> cached_db_id
        = cc->try_lookup_cached_db(prov_table.prov_db.db_name);
    if (cached_db_id.has_value()) {
        std::pair<database_id_t, name_string_t> db_table_name{
            cached_db_id->ci_value, prov_table.table_name};
        optional<config_info<std::pair<namespace_id_t, counted<const rc_wrapper<table_config_t>>>>> cached_table
            = cc->try_lookup_cached_table(db_table_name);
        if (cached_table.has_value()) {
            table.reset(new real_table_t(
                cached_table->ci_value.first,
                cached_table->ci_cv,
                cached_table->ci_value.second));
            out_db.reset(new db_t(
                cached_db_id->ci_value, prov_table.prov_db.db_name,
                config_version_checker{cached_db_id->ci_cv.value}));
        }
    }
#endif  // 0

    if (!table.has()) {
        // Perform a txn with both the db and table lookup.

        // TODO: If we have the cached db, try to optimistically lookup the table config (passing in cached_db_id).

        config_info<
            optional<std::pair<database_id_t, optional<std::pair<namespace_id_t, table_config_t>>>>> result;
        fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
                [&](FDBTransaction *txn) {
            result = config_cache_retrieve_db_and_table_by_name(
                txn, prov_table.prov_db.db_name, prov_table.table_name, interruptor);
        });
        guarantee_fdb_TODO(loop_err, "config_cache_retrieve_db_and_table_by_name loop");
        cc->note_version(result.ci_cv);

        if (!result.ci_value.has_value()) {
            // Database was not found.
            rfail_db_not_found(prov_table.prov_db.bt, prov_table.prov_db.db_name);
        }
        cc->add_db(result.ci_value->first, prov_table.prov_db.db_name);

        if (!result.ci_value->second.has_value()) {
            // Table was not found.
            rfail_table_dne_src(prov_table.bt, prov_table.prov_db.db_name,
                prov_table.table_name);
        }
        auto config = make_counted<const rc_wrapper<table_config_t>>(std::move(result.ci_value->second->second));
        cc->add_table(result.ci_value->second->first, config);

        // Both were found!
        table.reset(new real_table_t(result.ci_value->second->first,
            result.ci_cv,
            std::move(config)));
        out_db.reset(new db_t(result.ci_value->first, prov_table.prov_db.db_name,
            config_version_checker{result.ci_cv.value}));
    }

    return make_scoped<table_t>(
        std::move(table), std::move(out_db), prov_table.table_name,
        dummy_read_mode(), prov_table.bt);
}

class table_term_t : public op_term_t {
public:
    table_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, 2),
                    optargspec_t({"read_mode", "use_outdated", "identifier_format"})) { }
private:
    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const override {
        read_mode_t read_mode = read_mode_t::SINGLE;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "use_outdated")) {
            rfail(base_exc_t::LOGIC, "%s",
                  "The `use_outdated` optarg is no longer supported.  "
                  "Use the `read_mode` optarg instead.");
        }
        if (scoped_ptr_t<val_t> v = args->optarg(env, "read_mode")) {
            const datum_string_t &str = v->as_str(env);
            if (str == "majority") {
                read_mode = read_mode_t::MAJORITY;
            } else if (str == "single") {
                read_mode = read_mode_t::SINGLE;
            } else if (str == "outdated") {
                read_mode = read_mode_t::OUTDATED;
            } else if (str == "_debug_direct") {
                read_mode = read_mode_t::DEBUG_DIRECT;
            } else {
                rfail(base_exc_t::LOGIC, "Read mode `%s` unrecognized (options "
                      "are \"majority\", \"single\", and \"outdated\").",
                      str.to_std().c_str());
            }
        }

        optional<admin_identifier_format_t> identifier_format;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "identifier_format")) {
            const datum_string_t &str = v->as_str(env);
            if (str == "name") {
                identifier_format.set(admin_identifier_format_t::name);
            } else if (str == "uuid") {
                identifier_format.set(admin_identifier_format_t::uuid);
            } else {
                rfail(base_exc_t::LOGIC, "Identifier format `%s` unrecognized "
                    "(options are \"name\" and \"uuid\").", str.to_std().c_str());
            }
        }

        std::pair<database_id_t, name_string_t> db_table_name;
        provisional_db_id db;
        name_string_t table_name;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv.has());
            db = std::move(*dbv).as_prov_db(env->env);
            table_name = get_name(env->env, args->arg(env, 0), "Table");
        } else {
            r_sanity_check(args->num_args() == 2);
            db = std::move(*args->arg(env, 0)).as_prov_db(env->env);
            table_name = get_name(env->env, args->arg(env, 1), "Table");
        }

        // We don't use `read_mode` because that only got used pre-fdb.
        return new_val(provisional_table_id{
            db, table_name, identifier_format, backtrace()});
    }
    deterministic_t is_deterministic() const override { return deterministic_t::no(); }
    const char *name() const override { return "table"; }
};

// OOO: Fdb-ize the terms below.

class get_term_t : public op_term_t {
public:
    get_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }
private:
    virtual scoped_ptr_t<val_t>
    eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        return new_val(single_selection_t::provisionally_from_key(
                           std::move(*args->arg(env, 0)).as_prov_table(env->env),
                           args->arg(env, 1)->as_datum(env)));
    }
    virtual const char *name() const { return "get"; }
};

class get_all_term_t : public op_term_t {
public:
    get_all_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, -1), optargspec_t({ "index" })) { }
private:
    datum_t get_key_arg(env_t *env, const scoped_ptr_t<val_t> &arg) const {
        datum_t datum_arg = arg->as_datum(env);

        rcheck_target(arg,
                      !datum_arg.is_ptype(pseudo::geometry_string),
                      base_exc_t::LOGIC,
                      "Cannot use a geospatial index with `get_all`. "
                      "Use `get_intersecting` instead.");
        rcheck_target(arg, datum_arg.get_type() != datum_t::R_NULL,
                      base_exc_t::NON_EXISTENCE,
                      "Keys cannot be NULL.");
        return datum_arg;
    }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = std::move(*args->arg(env, 0)).as_table(env->env);
        scoped_ptr_t<val_t> index = args->optarg(env, "index");
        std::string index_str = index ? index->as_str(env).to_std() : table->get_pkey();

        std::map<datum_t, uint64_t> keys;
        for (size_t i = 1; i < args->num_args(); ++i) {
            auto key = get_key_arg(env->env, args->arg(env, i));
            keys.insert(std::make_pair(std::move(key), 0)).first->second += 1;
        }

        return new_val(
            make_scoped<selection_t>(
                table,
                table->get_all(env->env,
                               datumspec_t(std::move(keys)),
                               index_str,
                               backtrace())));
    }
    virtual const char *name() const { return "get_all"; }
};

counted_t<term_t> make_db_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<db_term_t>(env, term);
}

counted_t<term_t> make_table_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<table_term_t>(env, term);
}

counted_t<term_t> make_get_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<get_term_t>(env, term);
}

counted_t<term_t> make_get_all_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<get_all_term_t>(env, term);
}

counted_t<term_t> make_db_create_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<db_create_term_t>(env, term);
}

counted_t<term_t> make_db_drop_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<db_drop_term_t>(env, term);
}

counted_t<term_t> make_db_list_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<db_list_term_t>(env, term);
}

counted_t<term_t> make_table_create_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<table_create_term_t>(env, term);
}

counted_t<term_t> make_table_drop_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<table_drop_term_t>(env, term);
}

counted_t<term_t> make_table_list_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<table_list_term_t>(env, term);
}

counted_t<term_t> make_config_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<config_term_t>(env, term);
}

counted_t<term_t> make_status_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<status_term_t>(env, term);
}

counted_t<term_t> make_wait_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<wait_term_t>(env, term);
}

counted_t<term_t> make_reconfigure_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<reconfigure_term_t>(env, term);
}

counted_t<term_t> make_rebalance_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<rebalance_term_t>(env, term);
}

counted_t<term_t> make_sync_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sync_term_t>(env, term);
}

counted_t<term_t> make_grant_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<grant_term_t>(env, term);
}

} // namespace ql
