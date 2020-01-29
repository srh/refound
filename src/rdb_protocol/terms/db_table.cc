// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <map>
#include <string>

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/auth/permissions.hpp"
#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/namespace_interface_repository.hpp"
#include "clustering/administration/real_reql_cluster_interface.hpp"
// For utility function; maybe move convert_db_or_table_config_and_name_to_datum out.
#include "clustering/administration/tables/db_config.hpp"
// TODO: Move db_not_found_error to a different location
#include "clustering/administration/metadata.hpp"
#include "containers/name_string.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/datum_stream.hpp"
#include "rdb_protocol/datum_string.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/pseudo_geometry.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/terms/writes.hpp"

namespace ql {

name_string_t get_name(bt_rcheckable_t *target, const datum_string_t &raw_name,
                       const char *type_str) {
    name_string_t name;
    bool assignment_successful = name.assign_value(raw_name);
    rcheck_target(target,
                  assignment_successful,
                  base_exc_t::LOGIC,
                  strprintf("%s name `%s` invalid (%s).",
                            type_str,
                            raw_name.to_std().c_str(),
                            name_string_t::valid_char_msg));
    return name;
}

name_string_t get_name(const scoped_ptr_t<val_t> &name, const char *type_str) {
    r_sanity_check(name.has());
    return get_name(name.get(), name->as_str(), type_str);
}

void get_replicas_and_primary(const scoped_ptr_t<val_t> &replicas,
                              const scoped_ptr_t<val_t> &nonvoting_replica_tags,
                              const scoped_ptr_t<val_t> &primary_replica_tag,
                              table_generate_config_params_t *params) {
    if (replicas.has()) {
        params->num_replicas.clear();
        datum_t datum = replicas->as_datum();
        if (datum.get_type() == datum_t::R_OBJECT) {
            rcheck_target(replicas.get(), primary_replica_tag.has(), base_exc_t::LOGIC,
                "`primary_replica_tag` must be specified when `replicas` is an OBJECT.");
            for (size_t i = 0; i < datum.obj_size(); ++i) {
                std::pair<datum_string_t, datum_t> pair = datum.get_pair(i);
                name_string_t name = get_name(replicas.get(), pair.first, "Server tag");
                int64_t count = checked_convert_to_int(replicas.get(),
                                                       pair.second.as_num());
                rcheck_target(replicas.get(), count >= 0,
                    base_exc_t::LOGIC, "Can't have a negative number of replicas");
                size_t size_count = static_cast<size_t>(count);
                rcheck_target(replicas.get(), static_cast<int64_t>(size_count) == count,
                              base_exc_t::LOGIC,
                              strprintf("Integer too large: %" PRIi64, count));
                params->num_replicas.insert(std::make_pair(name, size_count));
            }
        } else if (datum.get_type() == datum_t::R_NUM) {
            rcheck_target(
                replicas.get(), !primary_replica_tag.has(), base_exc_t::LOGIC,
                "`replicas` must be an OBJECT if `primary_replica_tag` is specified.");
            rcheck_target(
                replicas.get(), !nonvoting_replica_tags.has(), base_exc_t::LOGIC,
                "`replicas` must be an OBJECT if `nonvoting_replica_tags` is "
                "specified.");
            size_t count = replicas->as_int<size_t>();
            params->num_replicas.insert(
                std::make_pair(params->primary_replica_tag, count));
        } else {
            rfail_target(replicas, base_exc_t::LOGIC,
                "Expected type OBJECT or NUMBER but found %s:\n%s",
                datum.get_type_name().c_str(), datum.print().c_str());
        }
    }

    if (nonvoting_replica_tags.has()) {
        params->nonvoting_replica_tags.clear();
        datum_t datum = nonvoting_replica_tags->as_datum();
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
                nonvoting_replica_tags.get(), tag.as_str(), "Server tag"));
        }
    }

    if (primary_replica_tag.has()) {
        params->primary_replica_tag = get_name(
            primary_replica_tag.get(), primary_replica_tag->as_str(), "Server tag");
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

// TODO: OOO comments -- must be done later (soon)
// TODO: NNN comments -- must be done later (now-ish, immediate cleanup)
// TODO: FTX comments -- relevant to fdb transactions exposed to user.

class db_term_t final : public meta_op_term_t {
public:
    db_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }
private:
    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const override {
        name_string_t db_name = get_name(args->arg(env, 0), "Database");

        if (db_name == artificial_reql_cluster_interface_t::database_name) {
            counted_t<const db_t> db = make_counted<db_t>(artificial_reql_cluster_interface_t::database_id, artificial_reql_cluster_interface_t::database_name);
            return new_val(std::move(db));
        }

        // FTX: config_version logic here.

        reqlfdb_config_cache *cc = env->env->get_rdb_ctx()->config_caches.get();

        optional<config_info<database_id_t>> cached = try_lookup_cached_db(
            cc, db_name);
        if (cached.has_value()) {
            // NNN: Put config_info into db_t (or into val's version of it).
            counted_t<db_t> db = make_counted<db_t>(cached->ci_value, db_name);
            db->cv.set(cached->ci_cv);
            return new_val(counted_t<const db_t>(std::move(db)));
        }

        config_info<optional<database_id_t>> result;
        // TODO: Read-only txn.
        fdb_error_t loop_err = txn_retry_loop_coro(env->fdb(), env->env->interruptor,
                [&](FDBTransaction *txn) {
            result = config_cache_retrieve_db_by_name(
                cc, txn, db_name, env->env->interruptor);
        });
        guarantee_fdb_TODO(loop_err, "config_cache_retrieve_db_by_name loop");

        cc->note_version(result.ci_cv);

        if (result.ci_value.has_value()) {
            database_id_t db_id = *result.ci_value;
            cc->add_db(db_id, db_name);

            // NNN: Put config_info into db_t (or into val's version of it).
            counted_t<db_t> db = make_counted<db_t>(cached->ci_value, db_name);
            db->cv.set(result.ci_cv);
            return new_val(counted_t<const db_t>(std::move(db)));
        } else {
            admin_err_t error = db_not_found_error(db_name);
            REQL_RETHROW(error);
        }
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
        name_string_t db_name = get_name(args->arg(env, 0), "Database");

        if (db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error = db_already_exists_error(db_name);
            REQL_RETHROW(error);
        }

        database_id_t db_id{generate_uuid()};
        bool fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Make sure config_cache_db_create checks auth permission.
            bool success = config_cache_db_create(
                txn,
                db_name,
                db_id,
                env->env->interruptor);
            if (success) {
                commit(txn, env->env->interruptor);
            }
            fdb_result = success;
        });
        guarantee_fdb_TODO(loop_err, "db_create txn failed");
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

        /*
        TODO: Check permissions as real_reql_cluster_interface db_create did, and throw this sort of permission error.

        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        */

        return new_val(result);
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
        table_generate_config_params_t config_params =
            table_generate_config_params_t::make_default();

        // Parse the 'shards' optarg
        if (scoped_ptr_t<val_t> shards_optarg = args->optarg(env, "shards")) {
            rcheck_target(shards_optarg, shards_optarg->as_int() == 1, base_exc_t::LOGIC,
                          "Every table must have exactly one shard.  (The configuration is obsolete.)");
        }

        // Parse the 'replicas', 'nonvoting_replica_tags', and
        // 'primary_replica_tag' optargs
        get_replicas_and_primary(args->optarg(env, "replicas"),
                                 args->optarg(env, "nonvoting_replica_tags"),
                                 args->optarg(env, "primary_replica_tag"),
                                 &config_params);

        std::string primary_key = "id";
        if (scoped_ptr_t<val_t> v = args->optarg(env, "primary_key")) {
            primary_key = v->as_str().to_std();
        }

        write_durability_t durability =
            parse_durability_optarg(args->optarg(env, "durability")) ==
                DURABILITY_REQUIREMENT_SOFT ?
                    write_durability_t::SOFT : write_durability_t::HARD;

        counted_t<const db_t> db;
        name_string_t tbl_name;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            db = dbv->as_db();
            tbl_name = get_name(args->arg(env, 0), "Table");
        } else {
            db = args->arg(env, 0)->as_db();
            tbl_name = get_name(args->arg(env, 1), "Table");
        }

        if (db->name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't create new tables "
                      "in it.", artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        // TODO: Fixup all '[&]' capture list usage.

        table_config_t config;
        config.basic.name = tbl_name;
        config.basic.database = db->id;
        config.basic.primary_key = primary_key;
        // TODO: Remove sharding UI.
        // TODO: Remove table_config_t::shards, ::write_ack_config, ::durability
        config.write_ack_config = write_ack_config_t::MAJORITY;
        config.durability = durability;
        config.user_data = default_user_data();
        namespace_id_t new_table_id{generate_uuid()};

        // TODO: Build the table config up here, remove outer_config_cache_table_create.
        bool fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Handle user auth permissions.
            bool success = config_cache_table_create(txn, new_table_id, config, env->env->interruptor);
            if (success) {
                commit(txn, env->env->interruptor);
            }
            fdb_result = success;
        });
        guarantee_fdb_TODO(loop_err, "table_create txn failed");

        if (!fdb_result) {
            admin_err_t error = table_already_exists_error(db->name, tbl_name);
            REQL_RETHROW(error);
        }
        // TODO: Wipe the config cache after the txn succeeds?

        server_name_map_t dummy_map;
        ql::datum_t new_config = convert_table_config_to_datum(new_table_id,
            convert_name_to_datum(db->name), config,
            admin_identifier_format_t::name, dummy_map);

        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("tables_created", ql::datum_t(1.0));
        result_builder.overwrite("config_changes",
            make_replacement_pair(ql::datum_t::null(), new_config));
        ql::datum_t result = std::move(result_builder).to_datum();

        return new_val(std::move(result));

        /* TODO: User auth stuff.
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
*/

        return new_val(result);
    }
    const char *name() const override { return "table_create"; }
};

// OOO: Re-fdb-ize stuff below (besides table_term_t).

class db_drop_term_t : public meta_op_term_t {
public:
    db_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const {
        name_string_t db_name = get_name(args->arg(env, 0), "Database");

        if (db_name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't delete it.",
                          artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }


        optional<database_id_t> fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            database_id_t db_id_local;
            optional<database_id_t> success
                = config_cache_db_drop(txn, db_name, env->env->interruptor);
            if (success.has_value()) {
                commit(txn, env->env->interruptor);
            }
            fdb_result = success;
        });
        guarantee_fdb_TODO(loop_err, "db_drop txn failed");

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


        /* TODO: Permissions check in db_drop.
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        */

        return new_val(result);
    }
    virtual const char *name() const { return "db_drop"; }
};

class table_drop_term_t : public meta_op_term_t {
public:
    table_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 2)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<const db_t> db;
        name_string_t tbl_name;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            db = dbv->as_db();
            tbl_name = get_name(args->arg(env, 0), "Table");
        } else {
            db = args->arg(env, 0)->as_db();
            tbl_name = get_name(args->arg(env, 1), "Table");
        }

        if (db->name == artificial_reql_cluster_interface_t::database_name) {
            admin_err_t error{
                strprintf("Database `%s` is special; you can't drop tables in it.",
                          artificial_reql_cluster_interface_t::database_name.c_str()),
                query_state_t::FAILED};
            REQL_RETHROW(error);
        }

        optional<std::pair<namespace_id_t, table_config_t>> fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            optional<std::pair<namespace_id_t, table_config_t>> success
                = config_cache_table_drop(txn, db->id, tbl_name,
                    env->env->interruptor);
            if (success.has_value()) {
                commit(txn, env->env->interruptor);
            }
            fdb_result = std::move(success);
        });
        guarantee_fdb_TODO(loop_err, "table_drop txn failed");

        if (!fdb_result.has_value()) {
            admin_err_t error = table_not_found_error(db->name, tbl_name);
            REQL_RETHROW(error);
        }

        server_name_map_t dummy_server_names;

        // TODO: Wipe the config cache after the txn succeeds?
        ql::datum_t old_config = convert_table_config_to_datum(
            fdb_result->first, convert_name_to_datum(db->name), fdb_result->second,
            admin_identifier_format_t::name, dummy_server_names);

        ql::datum_object_builder_t result_builder;
        result_builder.overwrite("tables_dropped", ql::datum_t(1.0));
        result_builder.overwrite("config_changes",
            make_replacement_pair(old_config, ql::datum_t::null()));
        ql::datum_t result = std::move(result_builder).to_datum();
        return new_val(std::move(result));

        /* TODO permissions stuff.
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        */
    }
    virtual const char *name() const { return "table_drop"; }
};

class db_list_term_t : public meta_op_term_t {
public:
    db_list_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(0)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *, eval_flags_t) const {
        std::vector<counted_t<const ql::db_t>> db_list;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Use a snapshot read for this?  Config txn appropriately?
            db_list = config_cache_db_list(txn, env->env->interruptor);
            // TODO: We don't need to commit read only txns, right?  Right??
        });

        guarantee_fdb_TODO(loop_err, "db_list txn failed");
        // TODO: Use the db_list to write to the config cache?
        // TODO: fdb-ize:  Remove non-fdb stuff, make use of db_list.

        std::set<name_string_t> dbs;
        admin_err_t error;
        if (!env->env->reql_cluster_interface()->db_list(
                env->env->interruptor, &dbs, &error)) {
            REQL_RETHROW(error);
        }

        std::vector<datum_t> arr;
        arr.reserve(dbs.size());
        for (auto it = dbs.begin(); it != dbs.end(); ++it) {
            arr.push_back(datum_t(datum_string_t(it->str())));
        }

        return new_val(datum_t(std::move(arr), env->env->limits()));
    }
    virtual const char *name() const { return "db_list"; }
};

class table_list_term_t : public meta_op_term_t {
public:
    table_list_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(0, 1)) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<const ql::db_t> db;
        if (args->num_args() == 0) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv);
            db = dbv->as_db();
        } else {
            db = args->arg(env, 0)->as_db();
        }

        std::vector<name_string_t> table_list;
        bool fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Use a snapshot read for this?  Config txn appropriately?
            fdb_result = config_cache_table_list(txn, db->id, env->env->interruptor, &table_list);
            // TODO: We don't need to commit read only txns, right?  Right??
        });
        guarantee_fdb_TODO(loop_err, "db_list txn failed");
        guarantee(fdb_result, "table_list got false result");

        // TODO: Fdb-ize, use table_list, remove old code.

        std::set<name_string_t> tables;
        admin_err_t error;
        if (!env->env->reql_cluster_interface()->table_list(db,
                env->env->interruptor, &tables, &error)) {
            REQL_RETHROW(error);
        }

        std::vector<datum_t> arr;
        arr.reserve(tables.size());
        for (auto it = tables.begin(); it != tables.end(); ++it) {
            arr.push_back(datum_t(datum_string_t(it->str())));
        }
        return new_val(datum_t(std::move(arr), env->env->limits()));
    }
    virtual const char *name() const { return "table_list"; }
};

class config_term_t : public meta_op_term_t {
public:
    config_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1, 1), optargspec_t({})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        // TODO: Fdb-ize this function.
        scoped_ptr_t<val_t> target = args->arg(env, 0);
        scoped_ptr_t<val_t> selection;
        bool success;
        admin_err_t error;
        try {
            /* Note that we always require an argument; we never take a default `db`
            argument. So `r.config()` is an error rather than the configuration for the
            current database. This is why we don't subclass from `table_or_db_meta_term_t`.
            */
            if (target->get_type().is_convertible(val_t::type_t::DB)) {
                success = env->env->reql_cluster_interface()->db_config(
                        env->env->get_user_context(),
                        target->as_db(),
                        backtrace(),
                        env->env,
                        &selection,
                        &error);
            } else {
                counted_t<table_t> table = target->as_table();
                name_string_t table_name = name_string_t::guarantee_valid(table->name.c_str());
                success = env->env->reql_cluster_interface()->table_config(
                        env->env->get_user_context(),
                        table->db,
                        table_name, backtrace(),
                        env->env,
                        &selection,
                        &error);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        if (!success) {
            REQL_RETHROW(error);
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
        // TODO: Fdb-ize this function
        counted_t<table_t> table = args->arg(env, 0)->as_table();
        name_string_t table_name = name_string_t::guarantee_valid(table->name.c_str());

        scoped_ptr_t<val_t> selection;
        try {
            admin_err_t error;
            if (!env->env->reql_cluster_interface()->table_status(
                    table->db, table_name, backtrace(), env->env, &selection, &error)) {
                REQL_RETHROW(error);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        return selection;
    }
    virtual const char *name() const { return "status"; }
};

/* Common superclass for terms that can operate on either a table or a database: `wait`,
`reconfigure`, and `rebalance`. */
class table_or_db_meta_term_t : public meta_op_term_t {
public:
    table_or_db_meta_term_t(compile_env_t *env, const raw_term_t &term,
                            optargspec_t &&_optargs)
        /* None of the subclasses take positional arguments except for the table/db. */
        : meta_op_term_t(env, term, argspec_t(0, 1), std::move(_optargs)) { }
protected:
    /* If the term is called on a table, then `db` and `name_if_table` indicate the
    table's database and name. If the term is called on a database, then `db `indicates
    the database and `name_if_table` will be empty. */
    virtual scoped_ptr_t<val_t> eval_impl_on_table_or_db(
            scope_env_t *env, args_t *args, eval_flags_t flags,
            const counted_t<const ql::db_t> &db,
            const optional<name_string_t> &name_if_table) const = 0;
private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t flags) const {
        scoped_ptr_t<val_t> target;
        if (args->num_args() == 0) {
            target = args->optarg(env, "db");
            r_sanity_check(target.has());
        } else {
            target = args->arg(env, 0);
        }
        if (target->get_type().is_convertible(val_t::type_t::DB)) {
            return eval_impl_on_table_or_db(env, args, flags, target->as_db(),
                r_nullopt);
        } else {
            counted_t<table_t> table = target->as_table();
            name_string_t table_name = name_string_t::guarantee_valid(table->name.c_str());
            return eval_impl_on_table_or_db(env, args, flags, table->db,
                                            make_optional(table_name));
        }
    }
};

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

    virtual scoped_ptr_t<val_t> eval_impl_on_table_or_db(
            scope_env_t *env, args_t *args, eval_flags_t,
	    const counted_t<const ql::db_t> &db,
            const optional<name_string_t> &name_if_table) const {
        // TODO: FDB-ize this function.  I think it's just table index building that
        // we'd need to wait for -- and for db's, it's all the db's tables' index
        // building operations.

        // Don't allow a wait call without explicit database
        if (args->num_args() == 0) {
            rfail(base_exc_t::LOGIC, "`wait` can only be called on a table or database.");
        }

        // Handle 'wait_for' optarg
        table_readiness_t readiness = table_readiness_t::finished;
        if (scoped_ptr_t<val_t> wait_for = args->optarg(env, "wait_for")) {
            if (wait_for->as_str() == wait_outdated_str) {
                readiness = table_readiness_t::outdated_reads;
            } else if (wait_for->as_str() == wait_reads_str) {
                readiness = table_readiness_t::reads;
            } else if (wait_for->as_str() == wait_writes_str) {
                readiness = table_readiness_t::writes;
            } else if (wait_for->as_str() == wait_all_str) {
                readiness = table_readiness_t::finished;
            } else {
                rfail_target(wait_for, base_exc_t::LOGIC,
                             "Unknown table readiness state: '%s', must be one of "
                             "'%s', '%s', '%s', or '%s'",
                             wait_for->as_str().to_std().c_str(),
                             wait_outdated_str, wait_reads_str,
                             wait_writes_str, wait_all_str);
            }
        }

        // Handle 'timeout' optarg
        signal_timer_t timeout_timer;
        wait_any_t combined_interruptor(env->env->interruptor);
        if (scoped_ptr_t<val_t> timeout = args->optarg(env, "timeout")) {
            timeout_timer.start(timeout->as_int<uint64_t>() * 1000);
            combined_interruptor.add(&timeout_timer);
        }

        // Perform db or table wait
        ql::datum_t result;
        bool success;
        admin_err_t error;
        try {
            if (static_cast<bool>(name_if_table)) {
                success = env->env->reql_cluster_interface()->table_wait(db,
                    *name_if_table, readiness, &combined_interruptor, &result, &error);
            } else {
                success = env->env->reql_cluster_interface()->db_wait(
                    db, readiness, &combined_interruptor, &result, &error);
            }
        } catch (const interrupted_exc_t &ex) {
            if (!timeout_timer.is_pulsed()) {
                throw;
            }
            rfail(base_exc_t::OP_FAILED, "Timed out while waiting for tables.");
        }

        if (!success) {
            REQL_RETHROW(error);
        }
        return new_val(result);
    }
    virtual const char *name() const { return "wait"; }
};

char const * const wait_term_t::wait_outdated_str = "ready_for_outdated_reads";
char const * const wait_term_t::wait_reads_str = "ready_for_reads";
char const * const wait_term_t::wait_writes_str = "ready_for_writes";
char const * const wait_term_t::wait_all_str = "all_replicas_ready";

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

    virtual scoped_ptr_t<val_t> eval_impl_on_table_or_db(
            scope_env_t *env, args_t *args, eval_flags_t,
            const counted_t<const ql::db_t> &db,
            const optional<name_string_t> &name_if_table) const {
        // TODO: Fdb-ize?  Remove this?  Make it a no-op?  Gradually let the shards
        // table config params and such wither away?

        // Don't allow a reconfigure call without explicit database
        if (args->num_args() == 0) {
	  rfail(base_exc_t::LOGIC, "`reconfigure` can only be called on a table or database.");
        }

        // Parse the 'dry_run' optarg
        bool dry_run = false;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "dry_run")) {
            dry_run = v->as_bool();
        }

        /* Figure out whether we're doing a regular reconfiguration or an emergency
        repair. */
        scoped_ptr_t<val_t> emergency_repair = args->optarg(env, "emergency_repair");
        if (!emergency_repair.has() ||
                emergency_repair->as_datum() == ql::datum_t::null()) {
            /* We're doing a regular reconfiguration. */

            // Use the default primary_replica_tag, unless the optarg overwrites it
            table_generate_config_params_t config_params =
                table_generate_config_params_t::make_default();

            // Parse the 'shards' optarg
            scoped_ptr_t<val_t> shards_optarg = required_optarg(env, args, "shards");
            rcheck_target(shards_optarg, shards_optarg->as_int() == 1,
                          base_exc_t::LOGIC,
                          "Every table must have exactly one shard.  (The configuration is obsolete.)");

            // Parse the 'replicas', 'nonvoting_replica_tags', and
            // 'primary_replica_tag' optargs
            get_replicas_and_primary(required_optarg(env, args, "replicas"),
                                     args->optarg(env, "nonvoting_replica_tags"),
                                     args->optarg(env, "primary_replica_tag"),
                                     &config_params);

            bool success;
            datum_t result;
            admin_err_t error;
            try {
                /* Perform the operation */
                if (static_cast<bool>(name_if_table)) {
                    success = env->env->reql_cluster_interface()->table_reconfigure(
                        env->env->get_user_context(),
                        db,
                        *name_if_table,
                        config_params,
                        dry_run,
                        env->env->interruptor,
                        &result,
                        &error);
                } else {
                    success = env->env->reql_cluster_interface()->db_reconfigure(
                        env->env->get_user_context(),
                        db,
                        config_params,
                        dry_run,
                        env->env->interruptor,
                        &result,
                        &error);
                }
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
            datum_string_t emergency_repair_str = emergency_repair->as_str();
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

            if (!static_cast<bool>(name_if_table)) {
                rfail(base_exc_t::LOGIC, "Can't emergency repair an entire database "
                    "at once; instead you should run `reconfigure()` on each table "
                    "individually.");
            }

            bool success;
            datum_t result;
            admin_err_t error;
            try {
                success = env->env->reql_cluster_interface()->table_emergency_repair(
                    env->env->get_user_context(),
                    db,
                    *name_if_table,
                    mode,
                    dry_run,
                    env->env->interruptor,
                    &result,
                    &error);
            } catch (auth::permission_error_t const &permission_error) {
                rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
            }
            if (!success) {
                REQL_RETHROW(error);
            }
            return new_val(result);
        }
    }
    virtual const char *name() const { return "reconfigure"; }
};

class rebalance_term_t : public table_or_db_meta_term_t {
public:
    rebalance_term_t(compile_env_t *env, const raw_term_t &term)
        : table_or_db_meta_term_t(env, term, optargspec_t({})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl_on_table_or_db(
            scope_env_t *env, args_t *args, eval_flags_t,
            const counted_t<const ql::db_t> &db,
            const optional<name_string_t> &name_if_table) const {
        // TODO: Fdb-ize?  Remove this?  Or make it a no-op.

        // Don't allow a rebalance call without explicit database
        if (args->num_args() == 0) {
	  rfail(base_exc_t::LOGIC, "`rebalance` can only be called on a table or database.");
        }

        ql::datum_t result;
        bool success;
        admin_err_t error;
        try {
            if (static_cast<bool>(name_if_table)) {
                success = env->env->reql_cluster_interface()->table_rebalance(
                    env->env->get_user_context(),
                    db,
                    *name_if_table,
                    env->env->interruptor,
                    &result,
                    &error);
            } else {
                success = env->env->reql_cluster_interface()->db_rebalance(
                    env->env->get_user_context(),
                    db,
                    env->env->interruptor,
                    &result,
                    &error);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }
        if (!success) {
            REQL_RETHROW(error);
        }
        return new_val(result);
    }
    virtual const char *name() const { return "rebalance"; }
};

class sync_term_t : public meta_op_term_t {
public:
    sync_term_t(compile_env_t *env, const raw_term_t &term)
        : meta_op_term_t(env, term, argspec_t(1)) { }

private:
    virtual scoped_ptr_t<val_t> eval_impl(
            scope_env_t *env, args_t *args, eval_flags_t) const {
        // TODO: Fdb-ize?  Fdb already syncs.  Maybe there's a mode which controls this.
        counted_t<table_t> t = args->arg(env, 0)->as_table();
        bool success = t->sync(env->env);
        r_sanity_check(success);
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
        // TODO: Fdb-ize.  (Add user-config to fdb.)
        auth::username_t username(
            args->arg(env, args->num_args() - 2)->as_str().to_std());
        ql::datum_t permissions = args->arg(env, args->num_args() - 1)->as_datum();

        bool success = false;
        ql::datum_t result;
        admin_err_t error;
        try {
            if (args->num_args() == 2) {
                success = env->env->reql_cluster_interface()->grant_global(
                    env->env->get_user_context(),
                    std::move(username),
                    std::move(permissions),
                    env->env->interruptor,
                    &result,
                    &error);
            } else {
                scoped_ptr_t<val_t> scope = args->arg(env, 0);
                if (scope->get_type().is_convertible(val_t::type_t::DB)) {
                    success = env->env->reql_cluster_interface()->grant_database(
                        env->env->get_user_context(),
                        scope->as_db()->id,
                        std::move(username),
                        std::move(permissions),
                        env->env->interruptor,
                        &result,
                        &error);
                } else {
                    counted_t<table_t> table = scope->as_table();
                    success = env->env->reql_cluster_interface()->grant_table(
                        env->env->get_user_context(),
                        table->db->id,
                        table->get_id(),
                        std::move(username),
                        std::move(permissions),
                        env->env->interruptor,
                        &result,
                        &error);
                }
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!success) {
            REQL_RETHROW(error);
        }

        return new_val(std::move(result));
    }

    virtual const char *name() const {
        return "grant";
    }
};

class table_term_t : public op_term_t {
public:
    table_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, 2),
                    optargspec_t({"read_mode", "use_outdated", "identifier_format"})) { }
private:
    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        // TODO: Fdb-ize this.
        read_mode_t read_mode = read_mode_t::SINGLE;
        if (scoped_ptr_t<val_t> v = args->optarg(env, "use_outdated")) {
            rfail(base_exc_t::LOGIC, "%s",
                  "The `use_outdated` optarg is no longer supported.  "
                  "Use the `read_mode` optarg instead.");
        }
        if (scoped_ptr_t<val_t> v = args->optarg(env, "read_mode")) {
            const datum_string_t &str = v->as_str();
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
            const datum_string_t &str = v->as_str();
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
        counted_t<const db_t> db;
        if (args->num_args() == 1) {
            scoped_ptr_t<val_t> dbv = args->optarg(env, "db");
            r_sanity_check(dbv.has());
            db = dbv->as_db();
            db_table_name.first = db->id;
            db_table_name.second = get_name(args->arg(env, 0), "Table");
        } else {
            r_sanity_check(args->num_args() == 2);
            db = args->arg(env, 0)->as_db();
            db_table_name.first = db->id;
            db_table_name.second = get_name(args->arg(env, 1), "Table");
        }

        reqlfdb_config_cache *cc = env->env->get_rdb_ctx()->config_caches.get();

        if (db->name == artificial_reql_cluster_interface_t::database_name) {
            // TODO: Don't need interruptor (because artificial interface won't chain to m_next)
            admin_err_t error;
            counted_t<base_table_t> table;
            if (!env->env->reql_cluster_interface()->table_find(db_table_name.second, db, identifier_format, env->env->interruptor, &table, &error)) {
                REQL_RETHROW(error);
            }
            return new_val(make_counted<table_t>(
                std::move(table), db, db_table_name.second.str(), read_mode,
                backtrace()));
        }

        // NNN: Ensure db config version and table config version match.

        optional<config_info<namespace_id_t>> cached = try_lookup_cached_table(
            cc, db_table_name);

        counted_t<real_table_t> table;
        if (cached.has_value()) {
            check_cv(db->cv.get(), cached->ci_cv);

            std::string primary_key;
            {
                ASSERT_NO_CORO_WAITING;  // cc mutex assertion
                auto it = cc->table_id_index.find(cached->ci_value);
                r_sanity_check(it != cc->table_id_index.end());
                primary_key = it->second.basic.primary_key;
            }

            reql_cluster_interface_t *rci = env->env->reql_cluster_interface();
            // TODO: remove the get_namespace_interface param from real_table_t, no interruptor.
            table.reset(new real_table_t(
                cached->ci_value,
                rci->get_namespace_repo()->get_namespace_interface(cached->ci_value, env->env->interruptor),
                primary_key,
                rci->get_changefeed_client(),
                rci->get_table_meta_client()));
            table->cv.set(cached->ci_cv);
        } else {
            config_info<optional<std::pair<namespace_id_t, table_config_t>>> result;
            // TODO: Read-only txn.
            fdb_error_t loop_err = txn_retry_loop_coro(env->fdb(), env->env->interruptor,
                    [&](FDBTransaction *txn) {
                result = config_cache_retrieve_table_by_name(
                    cc, txn, db_table_name, env->env->interruptor);
            });
            guarantee_fdb_TODO(loop_err, "config_cache_retrieve_table_by_name loop");
            check_cv(db->cv.get(), result.ci_cv);


            cc->note_version(result.ci_cv);

            if (result.ci_value.has_value()) {
                const namespace_id_t &table_id = result.ci_value->first;
                cc->add_table(table_id, result.ci_value->second);
                reql_cluster_interface_t *rci = env->env->reql_cluster_interface();
                table.reset(new real_table_t(
                    table_id,
                    rci->get_namespace_repo()->get_namespace_interface(table_id, env->env->interruptor),
                    result.ci_value->second.basic.primary_key,
                    rci->get_changefeed_client(),
                    rci->get_table_meta_client()));
                table->cv.set(result.ci_cv);
            } else {
                admin_err_t error = table_not_found_error(db->name, db_table_name.second);
                REQL_RETHROW(error);
            }
        }

        return new_val(make_counted<table_t>(
            std::move(table), db, db_table_name.second.str(), read_mode, backtrace()));
    }
    virtual deterministic_t is_deterministic() const { return deterministic_t::no(); }
    virtual const char *name() const { return "table"; }
};

class get_term_t : public op_term_t {
public:
    get_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }
private:
    virtual scoped_ptr_t<val_t>
    eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        return new_val(single_selection_t::from_key(
                           env->env,
                           backtrace(),
                           args->arg(env, 0)->as_table(),
                           args->arg(env, 1)->as_datum()));
    }
    virtual const char *name() const { return "get"; }
};

class get_all_term_t : public op_term_t {
public:
    get_all_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, -1), optargspec_t({ "index" })) { }
private:
    datum_t get_key_arg(const scoped_ptr_t<val_t> &arg) const {
        datum_t datum_arg = arg->as_datum();

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
        counted_t<table_t> table = args->arg(env, 0)->as_table();
        scoped_ptr_t<val_t> index = args->optarg(env, "index");
        std::string index_str = index ? index->as_str().to_std() : table->get_pkey();

        std::map<datum_t, uint64_t> keys;
        for (size_t i = 1; i < args->num_args(); ++i) {
            auto key = get_key_arg(args->arg(env, i));
            keys.insert(std::make_pair(std::move(key), 0)).first->second += 1;
        }

        return new_val(
            make_counted<selection_t>(
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
