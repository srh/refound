// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <string>

#include "clustering/admin_op_exc.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "containers/archive/buffer_stream.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/jobs.hpp"
#include "fdb/node_holder.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"
#include "rdb_protocol/term_walker.hpp"

namespace ql {

struct reql_version_result_t {
    cluster_version_result_t code;
    // If code is OBSOLETE_VERSION, this field gets used.
    obsolete_reql_version_t obsolete_version;
};

reql_version_result_t deserialize_importable_reql_version(
        read_stream_t *s, importable_reql_version_t *out) {
    // Initialize `out` to *something* because GCC 4.6.3 thinks that `thing`
    // could be used uninitialized, even when the return value of this function
    // is checked through `guarantee_deserialization()`.
    // See https://github.com/rethinkdb/rethinkdb/issues/2640
    *out = importable_reql_version_t::LATEST;
    int8_t raw;
    archive_result_t res = deserialize_universal(s, &raw);
    obsolete_reql_version_t FAKE = static_cast<obsolete_reql_version_t>(-1);
    if (bad(res)) {
        return reql_version_result_t{static_cast<cluster_version_result_t>(res), FAKE};
    }
    if (raw < static_cast<int8_t>(reql_version_t::EARLIEST)) {
        if (raw >= static_cast<int8_t>(obsolete_reql_version_t::EARLIEST)
            && raw <= static_cast<int8_t>(obsolete_reql_version_t::LATEST)) {
            return reql_version_result_t{
                cluster_version_result_t::OBSOLETE_VERSION,
                static_cast<obsolete_reql_version_t>(raw)
            };
        } else {
            return reql_version_result_t{cluster_version_result_t::UNRECOGNIZED_VERSION, FAKE};
        }
    } else {
        // This is the same rassert in `ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE`.
        if (raw >= static_cast<int8_t>(importable_reql_version_t::EARLIEST)
                && raw <= static_cast<int8_t>(importable_reql_version_t::LATEST)) {
            *out = static_cast<importable_reql_version_t>(raw);
            return reql_version_result_t{cluster_version_result_t::SUCCESS, FAKE};
        } else {
            return reql_version_result_t{cluster_version_result_t::UNRECOGNIZED_VERSION, FAKE};
        }
    }
}

std::string bad_deserialization_message(archive_result_t res, const char *thing) {
    std::string ret = "Deserialization of ";
    ret += thing;
    ret += " failed with error ";
    ret += archive_result_as_str(res);
    return ret;
}

void throw_if_bad_deserialization(rcheckable_t *target, archive_result_t res, const char *thing) {
    if (bad(res)) {
        rfail_target(
            (target),
            base_exc_t::LOGIC,
            "Binary blob passed to index create could not be interpreted as a "
            "reql_index_function (%s).",
            bad_deserialization_message(res, thing).c_str());
    }
}

/* `sindex_config_to_string` produces the string that goes in the `function` field of
`sindex_status()`. `sindex_config_from_string()` parses that string when it's passed to
`sindex_create()`. */

const char sindex_blob_prefix[] = "$reql_index_function$";

datum_string_t sindex_config_to_string(const sindex_config_t &config) {
    string_stream_t stream;
    UNUSED int64_t res = stream.write(sindex_blob_prefix, strlen(sindex_blob_prefix));

    write_message_t wm;
    serialize_cluster_version(&wm, cluster_version_t::LATEST_DISK);
    // Yes, we serialize it _twice_.  Really, _three times_.  We are maintaining
    // compatibility with prior behavior, which serialized sinedx_disk_info_t,
    // so that sindex configs can be imported (after being dumped from an older
    // version database).
    serialize<cluster_version_t::LATEST_DISK>(&wm, config.func_version);
    serialize<cluster_version_t::LATEST_DISK>(&wm, config.func_version);
    serialize<cluster_version_t::LATEST_DISK>(&wm, reql_version_t::LATEST);

    serialize<cluster_version_t::LATEST_DISK>(&wm, config.func);
    serialize<cluster_version_t::LATEST_DISK>(&wm, config.multi);
    serialize<cluster_version_t::LATEST_DISK>(&wm, config.geo);

    DEBUG_VAR int write_res = send_write_message(&stream, &wm);
    rassert(write_res == 0);
    return datum_string_t(std::move(stream.str()));
}

void rfail_reql_v1_13(rcheckable_t *target) {
    rfail_target(target, base_exc_t::LOGIC,
        "Attempted to import a RethinkDB 1.13 secondary index, "
        "which is no longer supported.  This secondary index "
        "may be updated by importing into RethinkDB 2.0.");
}

sindex_config_t sindex_config_from_string(
        const datum_string_t &string, rcheckable_t *target) {
    const char *const data = string.data();
    const size_t sz = string.size();
    const size_t prefix_sz = strlen(sindex_blob_prefix);
    bool bad_prefix = (sz < prefix_sz);
    for (size_t i = 0; !bad_prefix && i < prefix_sz; ++i) {
        bad_prefix |= (data[i] != sindex_blob_prefix[i]);
    }
    rcheck_target(
        target,
        !bad_prefix,
        base_exc_t::LOGIC,
        "Cannot create an sindex except from a reql_index_function returned from "
        "`index_status` in the field `function`.");

    sindex_config_t sindex_config;
    {
        buffer_read_stream_t read_stream(data + prefix_sz, sz - prefix_sz);
        cluster_version_t cluster_version;
        cluster_version_result_t cv_res = deserialize_cluster_version(&read_stream, &cluster_version);
        switch (cv_res) {
        case cluster_version_result_t::OBSOLETE_VERSION:
            rfail_reql_v1_13(target);
            break;
        case cluster_version_result_t::UNRECOGNIZED_VERSION:
            rfail_toplevel(ql::base_exc_t::INTERNAL,
                "Unrecognized secondary index version,"
                " secondary index not created.");
            break;
        case cluster_version_result_t::SUCCESS:
            // Do nothing, break.
            break;
        case cluster_version_result_t::SOCK_ERROR:
        case cluster_version_result_t::SOCK_EOF:
        case cluster_version_result_t::INT8_RANGE_ERROR:
        default:
            rfail_target(target,
                base_exc_t::LOGIC,
                "Binary blob passed to index create could not be interpreted as a "
                "reql_index_function (%s).",
                bad_deserialization_message(static_cast<archive_result_t>(cv_res), "sindex description").c_str());
        }

        // All we're really doing here is checking that it's importable and creating
        // a friendlier message (than a generic deserialization result) if it's not.
        importable_reql_version_t original_reql_version;
        reql_version_result_t res = deserialize_importable_reql_version(
                &read_stream,
                &original_reql_version);
        switch (res.code) {
        case cluster_version_result_t::OBSOLETE_VERSION:
            switch (res.obsolete_version) {
            case obsolete_reql_version_t::v1_13:
                rfail_reql_v1_13(target);
                break;
            // v1_15 is equal to v1_14
            case obsolete_reql_version_t::v1_15_is_latest:
                rfail_target(target, base_exc_t::LOGIC,
                    "Attempted to import a secondary index from before "
                    "RethinkDB 1.16, which is no longer supported.  This "
                    "secondary index may be updated by importing into "
                    "RethinkDB 2.1.");
                break;
            default:
                unreachable();
            }
            break;
        case cluster_version_result_t::UNRECOGNIZED_VERSION:
            rfail_toplevel(ql::base_exc_t::INTERNAL,
                "Unrecognized secondary index version,"
                " secondary index not created.");
        case cluster_version_result_t::SUCCESS:
        case cluster_version_result_t::SOCK_ERROR:
        case cluster_version_result_t::SOCK_EOF:
        case cluster_version_result_t::INT8_RANGE_ERROR:
        default:
            throw_if_bad_deserialization(target, static_cast<archive_result_t>(res.code), "sindex description");
        }
        importable_reql_version_t unused_version;
        archive_result_t success = deserialize_for_version(
                cluster_version,
                &read_stream,
                &unused_version);
        throw_if_bad_deserialization(target, success, "latest_compatible_reql_version");
        success = deserialize_for_version(
                cluster_version,
                &read_stream,
                &unused_version);
        throw_if_bad_deserialization(target, success, "latest_checked_reql_version");

        // But no matter how it deserializes, we set the reql version to latest.
        // In the future, we might have to do some conversions for compatibility.
        sindex_config.func_version = reql_version_t::LATEST;

        success = deserialize_for_version(cluster_version, &read_stream, &sindex_config.func);
        if (bad(success)) {
            throw_if_bad_deserialization(target, success, "sindex description");
        }
        success = deserialize_for_version(cluster_version, &read_stream, &sindex_config.multi);
        throw_if_bad_deserialization(target, success, "sindex description");
        switch (cluster_version) {
        case cluster_version_t::v1_14:
            sindex_config.geo = sindex_geo_bool_t::REGULAR;
            break;
        case cluster_version_t::v1_15: // fallthru
        case cluster_version_t::v1_16: // fallthru
        case cluster_version_t::v2_0: // fallthru
        case cluster_version_t::v2_1: // fallthru
        case cluster_version_t::v2_2: // fallthru
        case cluster_version_t::v2_3: // fallthru
        case cluster_version_t::v2_4: // fallthru
        case cluster_version_t::v2_5_is_latest:
            success = deserialize_for_version(cluster_version, &read_stream, &sindex_config.geo);
            throw_if_bad_deserialization(target, success, "sindex description");
            break;
        default: unreachable();
        }
        if (static_cast<size_t>(read_stream.tell()) != read_stream.size()) {
            rfail_target(
                target,
                base_exc_t::LOGIC,
                "Binary blob passed to index create could not be interpreted as a reql_index function (%s).",
                "The sindex description was incompletely deserialized.");
        }
    }
    return sindex_config;
}

// Helper for `sindex_status_to_datum()`
std::string format_index_create_query(
        const std::string &name,
        const sindex_config_t &config) {
    // TODO: Theoretically we need to escape quotes and UTF-8 characters inside the name.
    // Maybe use RapidJSON? Does our pretty-printer even do that for strings?
    std::string ret = "indexCreate('" + name + "', ";
    ret += config.func.det_func.compile_wire_func()->print_js_function();
    bool first_optarg = true;
    if (config.multi == sindex_multi_bool_t::MULTI) {
        if (first_optarg) {
            ret += ", {";
            first_optarg = false;
        } else {
            ret += ", ";
        }
        ret += "multi: true";
    }
    if (config.geo == sindex_geo_bool_t::GEO) {
        if (first_optarg) {
            ret += ", {";
            first_optarg = false;
        } else {
            ret += ", ";
        }
        ret += "geo: true";
    }
    if (!first_optarg) {
        ret += "}";
    }
    ret += ")";
    return ret;
}

/* `sindex_status_to_datum()` produces the documents that are returned from
`sindex_status()` and `sindex_wait()`. */

ql::datum_t sindex_status_to_datum(
        const std::string &name,
        const sindex_metaconfig_t &metaconfig,
        const sindex_status_t &status) {
    const sindex_config_t &config = metaconfig.config;
    ql::datum_object_builder_t stat;
    stat.overwrite("index", ql::datum_t(datum_string_t(name)));

    // TODO: Some of this information is bogus, and some metaconfig information is missing.  Such as creation task id.

    if (!status.ready) {
        stat.overwrite("progress",
            ql::datum_t(status.progress_numerator /
                        std::max<double>(status.progress_denominator, 1.0)));
    }
    stat.overwrite("ready", ql::datum_t::boolean(status.ready));
    stat.overwrite("outdated", ql::datum_t::boolean(status.outdated));
    stat.overwrite("multi",
        ql::datum_t::boolean(config.multi == sindex_multi_bool_t::MULTI));
    stat.overwrite("geo",
        ql::datum_t::boolean(config.geo == sindex_geo_bool_t::GEO));
    stat.overwrite("function",
        ql::datum_t::binary(sindex_config_to_string(config)));
    stat.overwrite("query",
        ql::datum_t(datum_string_t(format_index_create_query(name, config))));
    return std::move(stat).to_datum();
}

class sindex_create_term_t : public op_term_t {
public:
    sindex_create_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2, 3), optargspec_t({"multi", "geo"})) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);
        datum_t name_datum = args->arg(env, 1)->as_datum(env);
        std::string index_name = name_datum.as_str().to_std();

        /* Parse the sindex configuration */
        sindex_config_t config;
        config.multi = sindex_multi_bool_t::SINGLE;
        config.geo = sindex_geo_bool_t::REGULAR;
        if (args->num_args() == 3) {
            scoped_ptr_t<val_t> v = args->arg(env, 2);
            bool got_func = false;
            if (v->get_type().is_convertible(val_t::type_t::DATUM)) {
                datum_t d = v->as_datum(env);
                if (d.get_type() == datum_t::R_BINARY) {
                    config = sindex_config_from_string(d.as_binary(), v.get());
                    got_func = true;
                }
            }
            // We do it this way so that if someone passes a string, we produce
            // a type error asking for a function rather than BINARY.
            if (!got_func) {
                config.func = ql::deterministic_func{ql::wire_func_t(std::move(*v).as_func(env->env))};
                config.func_version = reql_version_t::LATEST;
            }
        } else {
            minidriver_t r(backtrace());
            auto x = minidriver_t::dummy_var_t::SINDEXCREATE_X;

            compile_env_t empty_compile_env((var_visibility_t()));
            counted_t<func_term_t> func_term_term =
                make_counted<func_term_t>(&empty_compile_env,
                                          r.fun(x, r.var(x)[name_datum]).root_term());

            config.func = ql::deterministic_func{ql::wire_func_t(counted<const func_t>(func_term_term->eval_to_func(env->scope)))};
            config.func_version = reql_version_t::LATEST;
        }

        config.func.det_func.compile_wire_func()->assert_deterministic(
                constant_now_t::no,
                "Index functions must be deterministic.");

        /* Check if we're doing a multi index or a normal index. */
        if (scoped_ptr_t<val_t> multi_val = args->optarg(env, "multi")) {
            config.multi = multi_val->as_bool(env)
                ? sindex_multi_bool_t::MULTI
                : sindex_multi_bool_t::SINGLE;
        }
        /* Do we want to create a geo index? */
        if (scoped_ptr_t<val_t> geo_val = args->optarg(env, "geo")) {
            config.geo = geo_val->as_bool(env)
                ? sindex_geo_bool_t::GEO
                : sindex_geo_bool_t::REGULAR;
        }

        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }
            rfail(ql::base_exc_t::OP_FAILED,
                "Database `%s` is special; you can't create secondary "
                "indexes on the tables in it.",
                artificial_reql_cluster_interface_t::database_name.c_str());
        }

        sindex_id_t new_sindex_id{generate_uuid()};
        fdb_shared_task_id new_task_id{generate_uuid()};

        optional<std::pair<reqlfdb_config_version, optional<fdb_job_info>>> fdb_result;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb,
                    env->env->interruptor, [&](FDBTransaction *txn) {
                optional<std::pair<reqlfdb_config_version, optional<fdb_job_info>>> success
                    = config_cache_sindex_create(
                    txn,
                    env->env->get_user_context(),
                    table,
                    index_name,
                    new_sindex_id,
                    new_task_id,
                    config,
                    env->env->interruptor,
                    backtrace());
                if (success.has_value()) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = success;
            });
            rcheck_fdb(loop_err, "initiating secondary index creation");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!fdb_result.has_value()) {
            rfail(ql::base_exc_t::OP_FAILED,
                "Index `%s` already exists on table `%s`.",
                index_name.c_str(), table.display_name().c_str());
        }
        env->env->get_rdb_ctx()->config_caches.get()->note_version(fdb_result->first);
        if (fdb_result->second.has_value()) {
            fdb_node_holder *node_holder = env->env->get_rdb_ctx()->node_holder;
            on_thread_t th(node_holder->home_thread());
            node_holder->supply_job(std::move(*fdb_result->second));
        }

        ql::datum_object_builder_t res;
        res.overwrite("created", datum_t(1.0));
        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "sindex_create"; }
};

class sindex_drop_term_t : public op_term_t {
public:
    sindex_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }

    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);
        std::string index_name = args->arg(env, 1)->as_datum(env).as_str().to_std();

        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }
            // TODO: Dedup index dne errors.
            rfail(ql::base_exc_t::OP_FAILED,
                "Index `%s` does not exist on table `%s`.",
                index_name.c_str(), table.display_name().c_str());
        }

        optional<reqlfdb_config_version> fdb_result;
        try {
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb,
                    env->env->interruptor, [&](FDBTransaction *txn) {
                optional<reqlfdb_config_version> success = config_cache_sindex_drop(
                    txn,
                    env->env->get_user_context(),
                    table,
                    index_name,
                    env->env->interruptor);
                if (success.has_value()) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = success;
            });
            rcheck_fdb(loop_err, "dropping secondary index");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        if (!fdb_result.has_value()) {
            rfail(ql::base_exc_t::OP_FAILED,
                "Index `%s` does not exist on table `%s`.",
                index_name.c_str(), table.display_name().c_str());
        }
        env->env->get_rdb_ctx()->config_caches.get()->note_version(*fdb_result);

        ql::datum_object_builder_t res;
        res.overwrite("dropped", datum_t(1.0));
        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "sindex_drop"; }
};

class sindex_list_term_t : public op_term_t {
public:
    sindex_list_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);

        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }
            return new_val(datum_t::empty_array());
        }

        // TODO: Is there really no user access control for this?
        config_info<std::pair<namespace_id_t, table_config_t>> fdb_result;
        fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
            // TODO: Read-only txn.
            fdb_result = expect_retrieve_table(txn, table, env->env->interruptor);
        });
        rcheck_fdb(loop_err, "listing secondary indexes");

        env->env->get_rdb_ctx()->config_caches.get()->note_version(fdb_result.ci_cv);

        /* Convert into an array and return it */
        ql::datum_array_builder_t res(ql::configured_limits_t::unlimited);
        res.reserve(fdb_result.ci_value.second.sindexes.size());
        for (const auto &pair : fdb_result.ci_value.second.sindexes) {
            res.add(ql::datum_t(datum_string_t(pair.first)));
        }
        res.sort();
        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "sindex_list"; }
};

bool sindex_is_ready(const sindex_metaconfig_t &x) {
    return x.creation_task_or_nil.value.is_nil();
}

sindex_status_t build_status(const sindex_metaconfig_t &x) {
    // QQQ: Initialize all of the status fields, or change the status output
    // format.
    sindex_status_t ret;
    ret.ready = sindex_is_ready(x);
    return ret;
}

class sindex_status_term_t : public op_term_t {
public:
    sindex_status_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, -1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        /* Parse the arguments */
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);
        std::set<std::string> sindexes;
        for (size_t i = 1; i < args->num_args(); ++i) {
            sindexes.insert(args->arg(env, i)->as_str(env).to_std());
        }

        ql::datum_array_builder_t res(ql::configured_limits_t::unlimited);
        std::set<std::string> remaining_sindexes = sindexes;
        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }
        } else {
            // TODO: Is there really no user access control for this?
            config_info<std::pair<namespace_id_t, table_config_t>> fdb_result;
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Read-only txn.
                fdb_result = expect_retrieve_table(txn, table, env->env->interruptor);
            });
            rcheck_fdb(loop_err, "reading table configuration");

            env->env->get_rdb_ctx()->config_caches.get()->note_version(fdb_result.ci_cv);

            const table_config_t &table_config = fdb_result.ci_value.second;

            // We iterate the sindex configs in name order (preserving existing behavior).
            std::vector<std::string> names;
            names.reserve(table_config.sindexes.size());
            for (const auto &pair : table_config.sindexes) {
                names.push_back(pair.first);
            }
            std::sort(names.begin(), names.end());
            for (const std::string &index_name : names) {
                if (!sindexes.empty()) {
                    if (sindexes.count(index_name) == 0) {
                        continue;
                    } else {
                        remaining_sindexes.erase(index_name);
                    }
                }

                auto it = table_config.sindexes.find(index_name);

                sindex_status_t status = build_status(it->second);
                res.add(sindex_status_to_datum(
                    index_name, it->second, status));
            }
        }

        /* Make sure we found all the requested sindexes. */
        // TODO: Dedup index not found errors.
        rcheck(remaining_sindexes.empty(), base_exc_t::OP_FAILED,
            strprintf("Index `%s` was not found on table `%s`.",
                      remaining_sindexes.begin()->c_str(),
                      table.display_name().c_str()));

        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "sindex_status"; }
};

/* We wait for no more than 10 seconds between polls to the indexes. */
namespace {
constexpr int64_t initial_poll_ms = 50;
constexpr int64_t max_poll_ms = 10000;
}

class sindex_wait_term_t : public op_term_t {
public:
    sindex_wait_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1, -1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id prov_table = std::move(*args->arg(env, 0)).as_prov_table(env->env);
        std::set<std::string> sindexes;
        for (size_t i = 1; i < args->num_args(); ++i) {
            sindexes.insert(args->arg(env, i)->as_str(env).to_std());
        }
        if (prov_table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(prov_table.table_name).has_value()) {
                rfail_prov_table_dne(prov_table);
            }
            // TODO: Do these rcheck statements or rfail instead of the admin_op_exc_t/rethrow rigamarole.
            // TODO: Dedup index not found message.
            rcheck(sindexes.empty(),
                base_exc_t::OP_FAILED,
                strprintf("Index `%s` was not found on table `%s`.",
                          sindexes.begin()->c_str(),
                          prov_table.display_name().c_str()));
            return new_val(datum_t::empty_array());
        }

        // TODO: Is there really no user access control for this?
        namespace_id_t table_id;
        optional<table_config_t> table_config;
        {
            config_info<std::pair<namespace_id_t, table_config_t>> fdb_result;
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                fdb_result = expect_retrieve_table(txn, prov_table, env->env->interruptor);
            });
            rcheck_fdb(loop_err, "retrieving table configuration");

            env->env->get_rdb_ctx()->config_caches.get()->note_version(fdb_result.ci_cv);
            // We fix the table id -- subsequent lookups use the table id.  (As if the
            // r.db().table() term evaluated the table id lookup exactly once.)
            table_id = fdb_result.ci_value.first;
            table_config.set(std::move(fdb_result.ci_value.second));
        }

        // Start with initial_poll_ms, then double the waiting period after each
        // attempt up to a maximum of max_poll_ms.
        int64_t current_poll_ms = initial_poll_ms;
        for (;;) {

            // Verify all requested sindexes exist.
            for (const auto &sindex : sindexes) {
                // TODO: Dedup error message creation.
                rcheck(table_config->sindexes.count(sindex) == 1, base_exc_t::OP_FAILED,
                    strprintf("Index `%s` was not found on table `%s`.",
                              sindex.c_str(),
                              prov_table.display_name().c_str()));
            }

            ql::datum_array_builder_t statuses(ql::configured_limits_t::unlimited);
            bool all_ready = true;
            for (const auto &pair : table_config->sindexes) {
                if (!sindexes.empty() && sindexes.count(pair.first) == 0) {
                    continue;
                }
                if (sindex_is_ready(pair.second)) {
                    sindex_status_t status = build_status(pair.second);
                    statuses.add(sindex_status_to_datum(
                        pair.first, pair.second, status));
                } else {
                    all_ready = false;
                }
            }
            if (all_ready) {
                return new_val(std::move(statuses).to_datum());
            } else {
                nap(current_poll_ms, env->env->interruptor);
                current_poll_ms = std::min(max_poll_ms, current_poll_ms * 2);
            }

            // TODO: Is there really no user access control for this? (same as above)
            fdb_error_t loop_err2 = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb, env->env->interruptor, [&](FDBTransaction *txn) {
                // TODO: Read-only txn.
                table_config = config_cache_get_table_config_without_cv_check(txn,
                    table_id,
                    env->env->interruptor);
            });
            rcheck_fdb(loop_err2, "verifying table configuration");

            if (!table_config.has_value()) {
                // QQQ: Maybe we should fail with a different message, mentioning that the table previously existed -- and mentioning the table id.
                rfail_table_dne(prov_table.prov_db.db_name, prov_table.table_name);
            }
        }
    }

    virtual const char *name() const { return "sindex_wait"; }
};

class sindex_rename_term_t : public op_term_t {
public:
    sindex_rename_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(3, 3), optargspec_t({"overwrite"})) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        provisional_table_id table = std::move(*args->arg(env, 0)).as_prov_table(env->env);
        scoped_ptr_t<val_t> old_name_val = args->arg(env, 1);
        scoped_ptr_t<val_t> new_name_val = args->arg(env, 2);
        std::string old_name = old_name_val->as_str(env).to_std();
        std::string new_name = new_name_val->as_str(env).to_std();

        scoped_ptr_t<val_t> overwrite_val = args->optarg(env, "overwrite");
        const bool overwrite = overwrite_val ? overwrite_val->as_bool(env) : false;

        if (table.prov_db.db_name == artificial_reql_cluster_interface_t::database_name) {
            if (!artificial_reql_cluster_interface_t::get_table_id(table.table_name).has_value()) {
                rfail_prov_table_dne(table);
            }

            // TODO: Dedup with rchecks in config_cache_sindex_rename.
            rcheck_src(backtrace(), old_name != artificial_table_fdb_backend_t::get_primary_key_name(),
                   ql::base_exc_t::LOGIC,
                   strprintf("Index name conflict: `%s` is the name of the primary key.",
                             old_name.c_str()));
            rcheck_src(backtrace(), new_name != artificial_table_fdb_backend_t::get_primary_key_name(),
                   ql::base_exc_t::LOGIC,
                   strprintf("Index name conflict: `%s` is the name of the primary key.",
                             new_name.c_str()));

            // TODO: Dedup index not found message.
            rfail(base_exc_t::OP_FAILED,
                "Index `%s` was not found on table `%s`.",
                          old_name.c_str(),
                          table.display_name().c_str());
        }


        config_info<rename_result> fdb_result;
        try {
            // Even if old_name == new_name, we're going to check that the table and
            // index exists by that name.
            fdb_error_t loop_err = txn_retry_loop_coro(env->env->get_rdb_ctx()->fdb,
                    env->env->interruptor, [&](FDBTransaction *txn) {
                config_info<rename_result> result = config_cache_sindex_rename(
                    txn,
                    env->env->get_user_context(),
                    table,
                    old_name,
                    new_name,
                    overwrite,
                    env->env->interruptor,
                    backtrace());
                if (result.ci_value == rename_result::success) {
                    commit(txn, env->env->interruptor);
                }
                fdb_result = result;
            });
            rcheck_fdb(loop_err, "renaming secondary index");
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        env->env->get_rdb_ctx()->config_caches.get()->note_version(fdb_result.ci_cv);

        switch (fdb_result.ci_value) {
        case rename_result::success: break;
        case rename_result::old_not_found: {
            rfail(ql::base_exc_t::OP_FAILED,
                  "Index `%s` does not exist on table `%s`.",
                  old_name.c_str(), table.display_name().c_str());
        } break;
        case rename_result::new_already_exists: {
            rfail(ql::base_exc_t::OP_FAILED,
                  "Index `%s` already exists on table `%s`.",
                  new_name.c_str(), table.display_name().c_str());
        } break;
        default: unreachable();
        }

        datum_object_builder_t retval;
        UNUSED bool b = retval.add("renamed",
                                   datum_t(old_name == new_name ?
                                                         0.0 : 1.0));
        return new_val(std::move(retval).to_datum());
    }

    virtual const char *name() const { return "sindex_rename"; }
};

counted_t<term_t> make_sindex_create_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_create_term_t>(env, term);
}
counted_t<term_t> make_sindex_drop_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_drop_term_t>(env, term);
}
counted_t<term_t> make_sindex_list_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_list_term_t>(env, term);
}
counted_t<term_t> make_sindex_status_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_status_term_t>(env, term);
}
counted_t<term_t> make_sindex_wait_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_wait_term_t>(env, term);
}
counted_t<term_t> make_sindex_rename_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<sindex_rename_term_t>(env, term);
}


} // namespace ql

